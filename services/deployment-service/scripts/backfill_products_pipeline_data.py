# -*- coding: utf-8 -*-
"""
Single-brand backfill for the product sessions pipeline behavior from
assets/products-pipeline.js.
"""

import os
import sys
import time
import traceback
from typing import Any, Dict, List, Optional

import requests
import mysql.connector
from dotenv import load_dotenv

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared.utils.encryption import decrypt
from shared.utils.logger import get_logger
from scripts.backfill_common import build_inclusive_date_range, get_onboarding_backfill_window
from scripts.backfill_hourly_products_data import fetch_brand_config, sync_products

load_dotenv(os.path.join(root_dir, ".env"))

logger = get_logger("deployment-service.backfill_products_pipeline")

SHOPIFYQL_TIMEZONE = os.getenv("SHOPIFYQL_TIMEZONE", "Asia/Kolkata")
API_VERSION = os.getenv("API_VERSION", "2024-04")


def format_shopifyql_table(table_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    columns = table_data.get("columns", []) or []
    rows = table_data.get("rows", []) or []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(dict(row))
            continue
        if isinstance(row, list):
            obj = {}
            for idx, val in enumerate(row):
                if idx < len(columns) and columns[idx].get("name"):
                    obj[columns[idx]["name"]] = val
            out.append(obj)
    return out


def build_day_clause(target_ymd: str) -> str:
    return f"SINCE {target_ymd}T00:00:00 UNTIL {target_ymd}T23:59:59"


def run_shopifyql_query(brand_info: Dict[str, Any], shopifyql: str, label: str) -> List[Dict[str, Any]]:
    shop_name = brand_info["shop_name"]
    token = decrypt(brand_info["access_token"])
    api_version = brand_info.get("api_version", API_VERSION)
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    escaped_query = shopifyql.replace('"', '\\"')
    graphql = {
        "query": f'query {{ shopifyqlQuery(query: "{escaped_query}") {{ tableData {{ rows columns {{ name }} }} parseErrors }} }}'
    }

    while True:
        resp = requests.post(
            url,
            json=graphql,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": token,
            },
            timeout=60,
        )

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", 3))
            logger.info(f"[{brand_info['brand_tag']}] ShopifyQL {label} rate-limited, sleeping {retry_after}s")
            time.sleep(retry_after)
            continue

        payload = resp.json()
        if resp.status_code != 200 or payload.get("errors"):
            logger.error(f"[{brand_info['brand_tag']}] ShopifyQL {label} failed: HTTP {resp.status_code}")
            return []

        res = payload.get("data", {}).get("shopifyqlQuery")
        if not res or res.get("parseErrors"):
            logger.error(f"[{brand_info['brand_tag']}] ShopifyQL {label} parse errors: {res.get('parseErrors') if res else 'missing payload'}")
            return []

        rows = format_shopifyql_table(res.get("tableData", {}))
        logger.info(f"[{brand_info['brand_tag']}] ShopifyQL {label} fetched {len(rows)} rows.")
        return rows


def fetch_shopifyql_sessions(brand_info: Dict[str, Any], target_ymd: str) -> List[Dict[str, Any]]:
    query = f"""
        FROM sessions
          SHOW
            landing_page_type,
            landing_page_path,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            referrer_name,
            sessions,
            sessions_with_cart_additions
          WHERE landing_page_path IS NOT NULL
            AND human_or_bot_session IN ('human', 'bot')
          GROUP BY
            landing_page_type,
            landing_page_path,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            referrer_name
          WITH TIMEZONE '{SHOPIFYQL_TIMEZONE}'
          {build_day_clause(target_ymd)}
          ORDER BY sessions DESC
          LIMIT 1000
        VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """.replace("\n", " ").strip()
    return run_shopifyql_query(brand_info, query, f"sessions {target_ymd}")


def fetch_shopifyql_hourly_sessions(brand_info: Dict[str, Any], target_ymd: str) -> List[Dict[str, Any]]:
    query = f"""
        FROM sessions
          SHOW hour_of_day, sessions, sessions_with_cart_additions
          WHERE human_or_bot_session IN ('human', 'bot')
          GROUP BY hour_of_day
          WITH TIMEZONE '{SHOPIFYQL_TIMEZONE}'
          {build_day_clause(target_ymd)}
          ORDER BY hour_of_day ASC
          LIMIT 1000
        VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
    """.replace("\n", " ").strip()
    return run_shopifyql_query(brand_info, query, f"hourly sessions {target_ymd}")


def connect_db(config: Dict[str, Any], brand_tag: str):
    return mysql.connector.connect(
        host=config["db_host"],
        user=config["db_user"],
        password=decrypt(config["db_password"]),
        port=int(config.get("port", 3306)),
        database=config.get("db_database", brand_tag),
        ssl_disabled=str(config["db_host"]).lower() in ["localhost", "127.0.0.1"],
    )


def ensure_tables(conn) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_sessions_snapshot (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                date DATE NOT NULL,
                landing_page_type VARCHAR(100) NOT NULL,
                landing_page_path VARCHAR(500) NOT NULL,
                product_id VARCHAR(50) DEFAULT NULL,
                product_title VARCHAR(255) DEFAULT NULL,
                utm_source VARCHAR(255) NULL,
                utm_medium VARCHAR(255) NULL,
                utm_campaign VARCHAR(255) NULL,
                utm_content VARCHAR(255) NULL,
                utm_term VARCHAR(255) NULL,
                referrer_name VARCHAR(255) NULL,
                sessions INT DEFAULT 0,
                sessions_with_cart_additions INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uniq_snapshot_dim (
                    date,
                    landing_page_type,
                    landing_page_path(200),
                    utm_source(100),
                    utm_medium(100),
                    utm_campaign(100),
                    utm_content(100),
                    utm_term(100),
                    referrer_name(100)
                ),
                KEY idx_snapshot_date (date),
                KEY idx_snapshot_product_id (product_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mv_product_sessions_by_type_daily (
                date DATE NOT NULL,
                landing_page_type VARCHAR(100) NOT NULL,
                product_type VARCHAR(255) NOT NULL DEFAULT 'Unknown',
                sessions INT NOT NULL DEFAULT 0,
                sessions_with_cart_additions INT NOT NULL DEFAULT 0,
                add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, landing_page_type, product_type)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mv_product_sessions_by_path_daily (
                date DATE NOT NULL,
                landing_page_path VARCHAR(500) NOT NULL,
                product_id VARCHAR(50) DEFAULT NULL,
                sessions INT NOT NULL DEFAULT 0,
                sessions_with_cart_additions INT NOT NULL DEFAULT 0,
                add_to_cart_rate DECIMAL(6,4) NOT NULL DEFAULT 0,
                add_to_cart_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
                conversion_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, landing_page_path(200)),
                KEY idx_path_product_id (product_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mv_product_sessions_by_campaign_daily (
                date DATE NOT NULL,
                landing_page_path VARCHAR(500) NOT NULL,
                utm_campaign VARCHAR(255) NOT NULL,
                product_id VARCHAR(50) DEFAULT NULL,
                referrer_name VARCHAR(255) DEFAULT NULL,
                sessions INT NOT NULL DEFAULT 0,
                sessions_with_cart_additions INT NOT NULL DEFAULT 0,
                add_to_cart_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
                conversion_rate_pct DECIMAL(7,4) NOT NULL DEFAULT 0,
                PRIMARY KEY (date, utm_campaign, landing_page_path(200), product_id, referrer_name(100))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_sessions_summary_shopify (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                number_of_sessions INT DEFAULT 0,
                number_of_atc_sessions INT DEFAULT 0,
                adjusted_number_of_sessions INT NULL,
                PRIMARY KEY (date, hour)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                key_name VARCHAR(100) PRIMARY KEY,
                key_value VARCHAR(255) NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        conn.commit()
    finally:
        cursor.close()


def upsert_product_sessions_snapshot(conn, rows: List[Dict[str, Any]], target_ymd: str, brand_tag: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM product_sessions_snapshot WHERE date = %s", (target_ymd,))
        if not rows:
            logger.info(f"[{brand_tag}] No snapshot rows for {target_ymd}.")
            conn.commit()
            return

        values = []
        for row in rows:
            values.append((
                target_ymd,
                row.get("landing_page_type") or "Unknown",
                row.get("landing_page_path"),
                None,
                None,
                row.get("utm_source"),
                row.get("utm_medium"),
                row.get("utm_campaign"),
                row.get("utm_content"),
                row.get("utm_term"),
                row.get("referrer_name"),
                int(row.get("sessions") or 0),
                int(row.get("sessions_with_cart_additions") or 0),
            ))

        cursor.executemany("""
            INSERT INTO product_sessions_snapshot (
                date, landing_page_type, landing_page_path, product_id, product_title,
                utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer_name,
                sessions, sessions_with_cart_additions
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, values)

        cursor.execute("""
            UPDATE product_sessions_snapshot s
            JOIN product_landing_mapping m
              ON (CASE WHEN s.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM s.landing_page_path) END)
               = (CASE WHEN m.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM m.landing_page_path) END)
            SET s.product_id = m.product_id,
                s.product_title = m.title
            WHERE s.date = %s
              AND (s.product_id IS NULL OR s.product_title IS NULL)
        """, (target_ymd,))
        conn.commit()
        logger.info(f"[{brand_tag}] Upserted {len(values)} snapshot rows for {target_ymd}.")
    finally:
        cursor.close()


def refresh_materialized_views(conn, target_ymd: str, brand_tag: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM mv_product_sessions_by_type_daily WHERE date = %s", (target_ymd,))
        cursor.execute("""
            INSERT INTO mv_product_sessions_by_type_daily
            (date, landing_page_type, product_type, sessions, sessions_with_cart_additions, add_to_cart_rate)
            SELECT
                s.date,
                s.landing_page_type,
                COALESCE(m.product_type, 'Unknown') AS product_type,
                SUM(s.sessions),
                SUM(s.sessions_with_cart_additions),
                CASE WHEN SUM(s.sessions) > 0
                     THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions), 4)
                     ELSE 0 END
            FROM product_sessions_snapshot s
            LEFT JOIN product_landing_mapping m
              ON (CASE WHEN s.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM s.landing_page_path) END)
               = (CASE WHEN m.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM m.landing_page_path) END)
            WHERE s.date = %s
            GROUP BY s.date, s.landing_page_type, product_type
        """, (target_ymd,))

        cursor.execute("DELETE FROM mv_product_sessions_by_path_daily WHERE date = %s", (target_ymd,))
        cursor.execute("""
            INSERT INTO mv_product_sessions_by_path_daily
            (date, landing_page_path, product_id, sessions, sessions_with_cart_additions, add_to_cart_rate, add_to_cart_rate_pct, conversion_rate_pct)
            SELECT
                s.date,
                s.landing_page_path,
                m.product_id,
                SUM(s.sessions),
                SUM(s.sessions_with_cart_additions),
                CASE WHEN SUM(s.sessions) > 0 THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions), 4) ELSE 0 END,
                CASE WHEN SUM(s.sessions) > 0 THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4) ELSE 0 END,
                CASE WHEN SUM(s.sessions) > 0 THEN ROUND(COALESCE(MAX(o.orders), 0) / SUM(s.sessions) * 100, 4) ELSE 0 END
            FROM product_sessions_snapshot s
            LEFT JOIN product_landing_mapping m
              ON (CASE WHEN s.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM s.landing_page_path) END)
               = (CASE WHEN m.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM m.landing_page_path) END)
            LEFT JOIN (
                SELECT product_id, created_dt AS created_date, COUNT(DISTINCT order_id) AS orders
                FROM shopify_orders
                WHERE created_dt = %s AND product_id IS NOT NULL
                GROUP BY product_id, created_dt
            ) o
              ON o.product_id = m.product_id AND o.created_date = s.date
            WHERE s.date = %s
            GROUP BY s.date, s.landing_page_path, m.product_id
        """, (target_ymd, target_ymd))

        cursor.execute("DELETE FROM mv_product_sessions_by_campaign_daily WHERE date = %s", (target_ymd,))
        cursor.execute("""
            INSERT INTO mv_product_sessions_by_campaign_daily
            (date, landing_page_path, utm_campaign, product_id, referrer_name, sessions, sessions_with_cart_additions, add_to_cart_rate_pct, conversion_rate_pct)
            SELECT
                s.date,
                s.landing_page_path,
                COALESCE(s.utm_campaign, '(none)') AS utm_campaign,
                m.product_id,
                s.referrer_name,
                SUM(s.sessions),
                SUM(s.sessions_with_cart_additions),
                CASE WHEN SUM(s.sessions) > 0 THEN ROUND(SUM(s.sessions_with_cart_additions) / SUM(s.sessions) * 100, 4) ELSE 0 END,
                CASE WHEN SUM(s.sessions) > 0 THEN ROUND(COALESCE(MAX(o.orders), 0) / SUM(s.sessions) * 100, 4) ELSE 0 END
            FROM product_sessions_snapshot s
            LEFT JOIN product_landing_mapping m
              ON (CASE WHEN s.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM s.landing_page_path) END)
               = (CASE WHEN m.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM m.landing_page_path) END)
            LEFT JOIN (
                SELECT product_id, created_dt AS created_date, COUNT(DISTINCT order_id) AS orders
                FROM shopify_orders
                WHERE created_dt = %s AND product_id IS NOT NULL
                GROUP BY product_id, created_dt
            ) o
              ON o.product_id = m.product_id AND o.created_date = s.date
            WHERE s.date = %s AND s.utm_campaign IS NOT NULL
            GROUP BY s.date, s.landing_page_path, utm_campaign, m.product_id, s.referrer_name
        """, (target_ymd, target_ymd))
        conn.commit()
        logger.info(f"[{brand_tag}] Refreshed product session MVs for {target_ymd}.")
    finally:
        cursor.close()


def upsert_hourly_sessions_summary(conn, rows: List[Dict[str, Any]], target_ymd: str, brand_tag: str) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM hourly_sessions_summary_shopify WHERE date = %s", (target_ymd,))
        values = []
        by_hour = {}
        for row in rows:
            hour = int(row.get("hour_of_day") or 0)
            by_hour[hour] = (
                int(row.get("sessions") or 0),
                int(row.get("sessions_with_cart_additions") or 0),
            )
        for hour in range(24):
            sessions, atc = by_hour.get(hour, (0, 0))
            values.append((target_ymd, hour, sessions, atc))
        cursor.executemany("""
            INSERT INTO hourly_sessions_summary_shopify
            (date, hour, number_of_sessions, number_of_atc_sessions)
            VALUES (%s, %s, %s, %s)
        """, values)
        conn.commit()
        logger.info(f"[{brand_tag}] Hourly sessions summary upserted for {target_ymd}.")
    finally:
        cursor.close()


def process_brand(brand_id: str, brand_tag: str, start_ymd: str, end_ymd: str) -> bool:
    logger.info(f"📦 [{brand_tag}] Starting products-pipeline backfill for onboarding window {start_ymd} -> {end_ymd}")
    config = fetch_brand_config(brand_id)
    if not config:
        logger.error(f"❌ [{brand_tag}] Missing brand config for products pipeline backfill.")
        return False

    conn = None
    try:
        conn = connect_db(config, brand_tag)
        ensure_tables(conn)
        logger.info(f"[{brand_tag}] Syncing product handles...")
        sync_products(config, conn)

        dates = build_inclusive_date_range(start_ymd, end_ymd)
        for target_ymd in dates:
            logger.info(f"[{brand_tag}] Products pipeline backfill date: {target_ymd}")
            rows = fetch_shopifyql_sessions(config, target_ymd)
            upsert_product_sessions_snapshot(conn, rows, target_ymd, brand_tag)
            refresh_materialized_views(conn, target_ymd, brand_tag)

            hourly_rows = fetch_shopifyql_hourly_sessions(config, target_ymd)
            upsert_hourly_sessions_summary(conn, hourly_rows, target_ymd, brand_tag)
            logger.info(f"[{brand_tag}] Products pipeline complete for {target_ymd}.")
        logger.info(f"✅ [{brand_tag}] Products-pipeline backfill completed.")
        return True
    except Exception as exc:
        logger.error(f"❌ [{brand_tag}] Products-pipeline backfill failed: {exc}")
        traceback.print_exc()
        return False
    finally:
        if conn:
            conn.close()


def process_event(data: dict) -> bool:
    brand_id = str(data.get("brand_id") or "").strip()
    brand_tag = str(data.get("brand_tag") or data.get("brand_name") or "").strip().lower()
    if not brand_id:
        logger.error("❌ Missing brand_id in onboarding payload for products pipeline backfill.")
        return False
    if not brand_tag:
        logger.error("❌ Missing brand_tag/brand_name in onboarding payload for products pipeline backfill.")
        return False

    start_ymd, end_ymd = get_onboarding_backfill_window()
    return process_brand(brand_id, brand_tag, start_ymd, end_ymd)
