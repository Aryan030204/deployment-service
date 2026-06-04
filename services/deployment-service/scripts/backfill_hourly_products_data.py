# -*- coding: utf-8 -*-
"""
Backfill Hourly Products Data Script (Python)
Ported from assets/backfill-hourly-products-data.js
"""

import os
import sys
import json
import time
import requests
import logging
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Sys Path Setup ---
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared.utils.encryption import decrypt
from dotenv import load_dotenv
from scripts.backfill_common import build_inclusive_date_range, get_onboarding_backfill_window

# Load .env
load_dotenv(os.path.join(root_dir, ".env"))

# ---- Logging ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

# ---- Config ----
GET_BRANDS_API = os.getenv("GET_BRANDS_API")
PIPELINE_AUTH_HEADER = os.getenv("PIPELINE_AUTH_HEADER")
API_VERSION = os.getenv("API_VERSION", "2024-04")
CONCURRENCY = int(os.getenv("BACKFILL_CONCURRENCY", "5"))
SHOPIFYQL_TIMEZONE = os.getenv("SHOPIFYQL_TIMEZONE", "Asia/Kolkata")
PAGE_SIZE = 1000

# ---- Imports for DB ----
import mysql.connector
from mysql.connector import pooling

# --------------------------
# Utilities
# --------------------------

def decrypt_value(val: str) -> str:
    if not val: return ""
    try:
        return decrypt(val)
    except Exception as e:
        logger.error(f"❌ Decryption failed: {e}")
        return val

def get_date_range(start_ymd: str, end_ymd: str) -> List[str]:
    return build_inclusive_date_range(start_ymd, end_ymd)

# --------------------------
# Brand Discovery
# --------------------------

def fetch_active_brands() -> Dict[str, str]:
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error("❌ GET_BRANDS_API or PIPELINE_AUTH_HEADER not set.")
        return {}
    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(GET_BRANDS_API, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"❌ Failed to fetch brands: {resp.status_code}")
    except Exception as e:
        logger.error(f"❌ Brand API error: {e}")
    return {}

def fetch_brand_config(brand_id: str) -> Optional[Dict]:
    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id.lower()}"
    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"❌ Failed to fetch config for {brand_id}: {resp.status_code}")
    except Exception as e:
        logger.error(f"❌ Config fetch error: {e}")
    return None

# --------------------------
# Database Logic
# --------------------------

def ensure_tables(conn):
    cursor = conn.cursor()
    try:
        # 1. product_landing_mapping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_landing_mapping (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                product_id BIGINT NOT NULL,
                landing_page_path VARCHAR(500) NOT NULL,
                status VARCHAR(50) DEFAULT NULL,
                title VARCHAR(255) DEFAULT NULL,
                product_type VARCHAR(255) DEFAULT NULL,
                last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uniq_landing_page_path (landing_page_path(200)),
                KEY idx_product_id (product_id),
                KEY idx_last_synced_at (last_synced_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # 2. pipeline_metadata
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                key_name VARCHAR(100) PRIMARY KEY,
                key_value VARCHAR(255) NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # 3. hourly_product_sessions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_product_sessions (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                landing_page_type VARCHAR(100) DEFAULT NULL,
                landing_page_path VARCHAR(500) NOT NULL,
                product_id VARCHAR(50) DEFAULT NULL,
                product_title VARCHAR(255) DEFAULT NULL,
                utm_source VARCHAR(255) DEFAULT NULL,
                utm_medium VARCHAR(255) DEFAULT NULL,
                utm_campaign VARCHAR(255) DEFAULT NULL,
                utm_content VARCHAR(255) DEFAULT NULL,
                utm_term VARCHAR(255) DEFAULT NULL,
                referrer_name VARCHAR(255) DEFAULT NULL,
                sessions INT NOT NULL DEFAULT 0,
                sessions_with_cart_additions INT NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_date_hour (date, hour),
                KEY idx_product_date (product_id, date),
                KEY idx_date_campaign (date, utm_campaign(150)),
                KEY idx_date_path (date, landing_page_path(200))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # 4. hourly_product_performance_rollup
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS hourly_product_performance_rollup (
                date DATE NOT NULL,
                hour TINYINT UNSIGNED NOT NULL,
                product_id VARCHAR(50) NOT NULL,
                product_title VARCHAR(255) DEFAULT NULL,
                sessions INT UNSIGNED NOT NULL DEFAULT 0,
                sessions_with_cart_additions INT UNSIGNED NOT NULL DEFAULT 0,
                orders INT UNSIGNED NOT NULL DEFAULT 0,
                units_sold INT UNSIGNED NOT NULL DEFAULT 0,
                total_sales DECIMAL(14,2) NOT NULL DEFAULT 0.00,
                add_to_cart_rate DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
                cvr DECIMAL(8,4) NOT NULL DEFAULT 0.0000,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date, hour, product_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        # Indexes for rollup
        try:
            cursor.execute("ALTER TABLE hourly_product_performance_rollup ADD INDEX idx_hpr_product_date_hour (product_id, date, hour)")
        except: pass
        try:
            cursor.execute("ALTER TABLE hourly_product_performance_rollup ADD INDEX idx_hpr_date_product (date, product_id)")
        except: pass
        conn.commit()
    finally:
        cursor.close()

def sync_products(brand_info, conn):
    shop_name = brand_info['shop_name']
    token = decrypt_value(brand_info['access_token'])
    api_version = brand_info.get('api_version', API_VERSION)
    
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/products.json?limit=250&fields=id,title,status,handle,product_type"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    
    cursor = conn.cursor()
    try:
        while url:
            resp = requests.get(url, headers=headers, timeout=60)
            if resp.status_code == 429:
                retry = int(resp.headers.get("Retry-After", 2))
                time.sleep(retry)
                continue
            if resp.status_code != 200:
                logger.error(f"[{shop_name}] Product sync failed: {resp.status_code}")
                break
                
            products = resp.json().get('products', [])
            if not products: break
            
            for p in products:
                path = f"/products/{p['handle']}"
                cursor.execute("""
                    INSERT INTO product_landing_mapping (product_id, landing_page_path, status, title, product_type)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        product_id = VALUES(product_id),
                        status = VALUES(status),
                        title = VALUES(title),
                        product_type = VALUES(product_type),
                        last_synced_at = CURRENT_TIMESTAMP
                """, (p['id'], path, p.get('status'), p.get('title'), p.get('product_type')))
            conn.commit()
            
            # Pagination
            link_header = resp.headers.get("Link")
            if link_header and 'rel="next"' in link_header:
                next_link = [l for l in link_header.split(",") if 'rel="next"' in l][0]
                url = next_link.split(";")[0].strip("< >")
            else:
                url = None
    finally:
        cursor.close()

# --------------------------
# ShopifyQL Logic
# --------------------------

def run_shopifyql(brand_info, query, label):
    shop_name = brand_info['shop_name']
    token = decrypt_value(brand_info['access_token'])
    api_version = brand_info.get('api_version', API_VERSION)
    
    endpoint = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    
    escaped_query = query.replace('"', '\\"')
    graphql = {
        "query": f'query {{ shopifyqlQuery(query: "{escaped_query}") {{ tableData {{ rows columns {{ name }} }} parseErrors }} }}'
    }
    
    for attempt in range(5):
        try:
            resp = requests.post(endpoint, json=graphql, headers=headers, timeout=60)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                continue
                
            data = resp.json()
            if "errors" in data:
                # Check for THROTTLED
                if any(e.get("extensions", {}).get("code") == "THROTTLED" for e in data["errors"]):
                    time.sleep(10)
                    continue
                raise Exception(f"GraphQL Errors: {data['errors']}")
            
            res = data.get("data", {}).get("shopifyqlQuery")
            if not res or res.get("parseErrors"):
                raise Exception(f"ShopifyQL Parse Errors: {res.get('parseErrors') if res else 'No Result'}")
            
            # Format Table
            table_data = res.get("tableData", {})
            cols = [c["name"] for c in table_data.get("columns", [])]
            rows = table_data.get("rows", [])
            
            formatted_rows = []
            for row in rows:
                if isinstance(row, dict):
                    formatted_rows.append(row)
                elif isinstance(row, list):
                    formatted_rows.append(dict(zip(cols, row)))
            return formatted_rows
        except Exception as e:
            logger.error(f"[{shop_name}] Attempt {attempt+1} failed for {label}: {e}")
            time.sleep(2 ** attempt)
    return []

def fetch_hourly_sessions(brand_info, date_str):
    out = []
    offset = 0
    while True:
        query = f"""
            FROM sessions SHOW hour_of_day, landing_page_type, landing_page_path, 
            utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer_name, 
            sessions, sessions_with_cart_additions 
            WHERE landing_page_path IS NOT NULL AND human_or_bot_session IN ('human', 'bot') 
            GROUP BY hour_of_day, landing_page_type, landing_page_path, utm_source, utm_medium, 
            utm_campaign, utm_content, utm_term, referrer_name 
            WITH TIMEZONE '{SHOPIFYQL_TIMEZONE}' 
            SINCE {date_str}T00:00:00 UNTIL {date_str}T23:59:59 
            ORDER BY sessions DESC LIMIT {PAGE_SIZE} {{ OFFSET {offset} }} 
            VISUALIZE sessions, sessions_with_cart_additions TYPE list_with_dimension_values
        """.replace("\n", " ").strip()
        
        rows = run_shopifyql(brand_info, query, f"hourly-sessions-{date_str}-offset-{offset}")
        if not rows: break
        out.extend(rows)
        if len(rows) < PAGE_SIZE: break
        offset += PAGE_SIZE
    return out

# --------------------------
# Orchestration
# --------------------------

def process_brand(brand_id: str, brand_tag: str, start_ymd: str, end_ymd: str) -> bool:
    logger.info(f"🚀 Processing brand: {brand_tag} ({brand_id})")
    config = fetch_brand_config(brand_id)
    if not config:
        logger.error(f"❌ [{brand_tag}] Missing brand config for brand_id={brand_id}")
        return False
    
    db_pass = decrypt_value(config['db_password'])
    db_host = config['db_host']
    db_user = config['db_user']
    db_port = int(config.get('port', 3306))
    db_name = config.get('db_database', brand_tag)
    
    conn = None
    try:
        conn = mysql.connector.connect(
            host=db_host, user=db_user, password=db_pass, port=db_port, database=db_name,
            ssl_disabled=(db_host.lower() in ["localhost", "127.0.0.1"])
        )
        ensure_tables(conn)
        
        # Sync product handles
        logger.info(f"[{brand_tag}] Syncing product handles...")
        sync_products(config, conn)
        
        dates = get_date_range(start_ymd, end_ymd)
        for date_str in dates:
            logger.info(f"[{brand_tag}] Backfilling date: {date_str}...")
            
            # 1. Fetch sessions
            raw_rows = fetch_hourly_sessions(config, date_str)
            if not raw_rows:
                logger.info(f"[{brand_tag}] No session data for {date_str}")
                continue
                
            # 2. Upsert into hourly_product_sessions
            cursor = conn.cursor()
            cursor.execute("DELETE FROM hourly_product_sessions WHERE date = %s", (date_str,))
            
            chunk_size = 500
            for i in range(0, len(raw_rows), chunk_size):
                chunk = raw_rows[i:i+chunk_size]
                values = []
                for r in chunk:
                    values.append((
                        date_str, int(r.get('hour_of_day', 0)), r.get('landing_page_type', 'Unknown'),
                        r.get('landing_page_path'), None, None, r.get('utm_source'), r.get('utm_medium'),
                        r.get('utm_campaign'), r.get('utm_content'), r.get('utm_term'), r.get('referrer_name'),
                        int(r.get('sessions', 0)), int(r.get('sessions_with_cart_additions', 0))
                    ))
                cursor.executemany("""
                    INSERT INTO hourly_product_sessions 
                    (date, hour, landing_page_type, landing_page_path, product_id, product_title, 
                    utm_source, utm_medium, utm_campaign, utm_content, utm_term, referrer_name, 
                    sessions, sessions_with_cart_additions)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, values)
            conn.commit()
            
            # 3. Update product_id/title from mapping
            cursor.execute("""
                UPDATE hourly_product_sessions h
                JOIN product_landing_mapping m ON (
                    CASE WHEN h.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM h.landing_page_path) END
                ) = (
                    CASE WHEN m.landing_page_path = '/' THEN '/' ELSE TRIM(TRAILING '/' FROM m.landing_page_path) END
                )
                SET h.product_id = m.product_id, h.product_title = m.title
                WHERE h.date = %s AND h.product_id IS NULL
            """, (date_str,))
            conn.commit()
            
            # 4. Rollup performance
            cursor.execute("DELETE FROM hourly_product_performance_rollup WHERE date = %s", (date_str,))
            cursor.execute("""
                INSERT INTO hourly_product_performance_rollup
                (date, hour, product_id, product_title, sessions, sessions_with_cart_additions, orders, units_sold, total_sales, add_to_cart_rate, cvr)
                SELECT
                    k.date, k.hour, k.product_id,
                    COALESCE(s.product_title, o.product_title, 'Unknown') AS product_title,
                    COALESCE(s.sessions, 0) AS sessions,
                    COALESCE(s.sessions_with_cart_additions, 0) AS sessions_with_cart_additions,
                    COALESCE(o.orders, 0) AS orders,
                    COALESCE(o.units_sold, 0) AS units_sold,
                    COALESCE(o.total_sales, 0.00) AS total_sales,
                    ROUND((COALESCE(s.sessions_with_cart_additions, 0) / NULLIF(COALESCE(s.sessions, 0), 0)) * 100, 4) AS add_to_cart_rate,
                    ROUND((COALESCE(o.orders, 0) / NULLIF(COALESCE(s.sessions, 0), 0)) * 100, 4) AS cvr
                FROM (
                    SELECT date, hour, product_id FROM (
                        SELECT date, hour, product_id FROM hourly_product_sessions WHERE date = %s AND product_id IS NOT NULL AND product_id <> '' GROUP BY date, hour, product_id
                        UNION
                        SELECT created_dt AS date, created_hr AS hour, product_id FROM shopify_orders WHERE created_dt = %s AND order_id IS NOT NULL AND product_id IS NOT NULL AND product_id <> '' GROUP BY date, hour, product_id
                    ) u
                ) k
                LEFT JOIN (
                    SELECT date, hour, product_id, MAX(product_title) AS product_title, SUM(sessions) AS sessions, SUM(sessions_with_cart_additions) AS sessions_with_cart_additions
                    FROM hourly_product_sessions WHERE date = %s AND product_id IS NOT NULL AND product_id <> '' GROUP BY date, hour, product_id
                ) s ON s.date = k.date AND s.hour = k.hour AND s.product_id = k.product_id
                LEFT JOIN (
                    SELECT created_dt AS date, created_hr AS hour, product_id, MAX(line_item) AS product_title, COUNT(DISTINCT order_id) AS orders, SUM(GREATEST(COALESCE(line_item_quantity, 0), 0)) AS units_sold,
                    ROUND(SUM((COALESCE(line_item_price, 0) * GREATEST(COALESCE(line_item_quantity, 0), 0)) - COALESCE(line_item_total_discount, 0)), 2) AS total_sales
                    FROM shopify_orders WHERE created_dt = %s AND order_id IS NOT NULL AND product_id IS NOT NULL AND product_id <> '' GROUP BY date, hour, product_id
                ) o ON o.date = k.date AND o.hour = k.hour AND o.product_id = k.product_id
            """, (date_str, date_str, date_str, date_str))
            conn.commit()
            cursor.close()
            logger.info(f"✅ [{brand_tag}] Completed {date_str}")
        return True
    except Exception as e:
        logger.error(f"❌ [{brand_tag}] Failed: {e}")
        traceback.print_exc()
        return False
    finally:
        if conn:
            conn.close()


def process_event(data: dict) -> bool:
    brand_id = str(data.get("brand_id") or "").strip()
    brand_tag = str(data.get("brand_tag") or data.get("brand_name") or "").strip().lower()

    if not brand_id:
        logger.error("❌ Missing brand_id in onboarding payload for hourly products backfill.")
        return False
    if not brand_tag:
        logger.error("❌ Missing brand_tag/brand_name in onboarding payload for hourly products backfill.")
        return False

    start_ymd, end_ymd = get_onboarding_backfill_window()
    logger.info(
        f"📦 [{brand_tag}] Starting hourly product backfill for onboarding window {start_ymd} -> {end_ymd}"
    )
    ok = process_brand(brand_id, brand_tag, start_ymd, end_ymd)
    if ok:
        logger.info(f"✅ [{brand_tag}] Hourly product backfill completed.")
    return ok

def main():
    logger.info("⚡ Starting Hourly Product Performance Backfill")
    brands = fetch_active_brands()
    if not brands:
        logger.warning("⚠️ No brands found.")
        return
        
    logger.info(f"📍 Found {len(brands)} brands. Starting workers (concurrency={CONCURRENCY}).")
    start_ymd, end_ymd = get_onboarding_backfill_window()
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = [
            executor.submit(process_brand, bid, tag, start_ymd, end_ymd)
            for bid, tag in brands.items()
        ]
        for future in as_completed(futures):
            future.result()
            
    logger.info("✨ Backfill Process Completed.")

if __name__ == "__main__":
    main()
