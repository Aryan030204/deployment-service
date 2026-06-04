import os
import sys
import time
from functools import wraps
from typing import Any, Dict, List

import mysql.connector
import requests
from mysql.connector import Error
from dotenv import load_dotenv

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared.utils.encryption import decrypt
from shared.utils.logger import get_logger
from scripts.backfill_common import build_inclusive_date_range, get_onboarding_backfill_window

load_dotenv(os.path.join(root_dir, ".env"))

logger = get_logger("deployment-service.backfill_utm_aggregation")

GET_BRANDS_API = os.getenv("GET_BRANDS_API", "")
PIPELINE_AUTH_HEADER = os.getenv("PIPELINE_AUTH_HEADER", "")
RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "2"))

DAILY_TABLES = [
    "utm_source_daily",
    "utm_medium_daily",
    "utm_campaign_daily",
    "utm_source_medium_daily",
    "utm_source_campaign_daily",
    "utm_medium_campaign_daily",
    "utm_source_medium_campaign_daily",
]

HOURLY_TABLES = [
    "utm_source_hourly",
    "utm_medium_hourly",
    "utm_campaign_hourly",
    "utm_source_medium_hourly",
    "utm_source_campaign_hourly",
    "utm_medium_campaign_hourly",
    "utm_source_medium_campaign_hourly",
]

METRICS_COLUMNS = """
    orders INT DEFAULT 0,
    sales DECIMAL(15, 2) DEFAULT 0.00,
    average_order_value DECIMAL(15, 2) DEFAULT 0.00,
    sessions INT DEFAULT 0,
    atc_sessions INT DEFAULT 0,
    conversion_rate DECIMAL(5, 4) DEFAULT 0.0000,
    cancellation_rate DECIMAL(5, 4) DEFAULT 0.0000,
    refund_rate DECIMAL(5, 4) DEFAULT 0.0000,
    prepaid_orders INT DEFAULT 0,
    cod_orders INT DEFAULT 0,
    ppcod_orders INT DEFAULT 0,
    cancelled_orders INT DEFAULT 0,
    refunded_orders INT DEFAULT 0
"""

METRIC_COLUMNS = [
    "orders",
    "sales",
    "average_order_value",
    "sessions",
    "atc_sessions",
    "conversion_rate",
    "cancellation_rate",
    "refund_rate",
    "prepaid_orders",
    "cod_orders",
    "ppcod_orders",
    "cancelled_orders",
    "refunded_orders",
]

PAYMENT_TYPE_CASE_SQL = """
    CASE
        WHEN LOWER(payment_gateway_names) LIKE '%gokwik ppcod%' THEN 'PPCOD'
        WHEN (payment_gateway_names IS NULL
            OR payment_gateway_names = ''
            OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
            OR payment_gateway_names LIKE '%cash_on_delivery%')
            AND (payment_gateway_names NOT LIKE '%Gokwik PPCOD%' OR payment_gateway_names IS NULL)
            THEN 'COD'
        ELSE 'PREPAID'
    END
"""


def retry_db_operation(max_attempts: int = RETRY_ATTEMPTS, delay: int = RETRY_DELAY_SECONDS):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    attempts += 1
                    logger.warning(
                        f"Attempt {attempts}/{max_attempts} failed for {func.__name__}: {exc}"
                    )
                    if attempts >= max_attempts:
                        raise
                    time.sleep(delay * attempts)

        return wrapper

    return decorator


class DBManager:
    def __init__(self, brand_name: str, host: str, user: str, password: str, database: str, port: int = 3306):
        self.brand = brand_name
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                ssl_disabled=str(self.host).lower() in ["localhost", "127.0.0.1"],
            )
            return self.connection
        except Error as exc:
            logger.error(
                f"Error while connecting to MySQL database '{self.database}' ({self.host}): {exc}"
            )
            raise

    def close(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()

    def execute_query(self, query: str, params: tuple = None, commit: bool = False):
        if not self.connection or not self.connection.is_connected():
            self.connect()

        cursor = self.connection.cursor(dictionary=True)
        try:
            cursor.execute(query, params)
            if commit:
                self.connection.commit()
                return cursor.rowcount
            return cursor.fetchall()
        except Error as exc:
            logger.error(f"Error executing query for {self.brand}: {exc}")
            if commit:
                self.connection.rollback()
            raise
        finally:
            cursor.close()


def get_dimension_columns_and_indexes(table_name: str, is_hourly: bool = False):
    parts = table_name.replace("_daily", "").replace("_hourly", "").split("_")[1:]
    columns = ["metric_date DATE"]
    indexes = ["metric_date"]

    if is_hourly:
        columns.append("metric_hour INT")
        indexes.append("metric_hour")

    extra_indexes = []
    for part in parts:
        col_name = f"utm_{part}"
        columns.append(f"{col_name} VARCHAR(255)")
        indexes.append(col_name)
        extra_indexes.append(f"INDEX idx_{table_name}_{col_name} ({col_name})")

    if "source" in parts and "medium" in parts:
        extra_indexes.append(f"INDEX idx_{table_name}_src_med (utm_source, utm_medium)")
    if "medium" in parts and "campaign" in parts:
        extra_indexes.append(f"INDEX idx_{table_name}_med_cam (utm_medium, utm_campaign)")

    cols_def = ",\n    ".join(columns)
    idx_def = ", ".join(indexes)
    extra_indexes_sql = ""
    if extra_indexes:
        extra_indexes_sql = ",\n            " + ",\n            ".join(extra_indexes)
    return cols_def, idx_def, extra_indexes_sql


def ensure_tables_exist(db_manager: DBManager):
    all_tables = [(t, False) for t in DAILY_TABLES] + [(t, True) for t in HOURLY_TABLES]

    for table_name, is_hourly in all_tables:
        dims_cols, idx_cols, extra_idx_sql = get_dimension_columns_and_indexes(table_name, is_hourly)
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {dims_cols},
            {METRICS_COLUMNS},
            INDEX idx_{table_name}_dims ({idx_cols}){extra_idx_sql}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        db_manager.execute_query(create_sql, commit=True)


def get_dimension_columns(table_name: str, is_hourly: bool = False) -> List[str]:
    parts = table_name.replace("_daily", "").replace("_hourly", "").split("_")[1:]
    columns = ["metric_date"]
    if is_hourly:
        columns.append("metric_hour")
    for part in parts:
        columns.append(f"utm_{part}")
    return columns


def get_all_columns(table_name: str, is_hourly: bool = False) -> List[str]:
    return get_dimension_columns(table_name, is_hourly) + list(METRIC_COLUMNS)


def build_delete_query(table_name: str) -> str:
    return f"DELETE FROM {table_name} WHERE metric_date = %s;"


def build_select_aggregation_query(table_name: str, is_hourly: bool = False) -> str:
    dims = get_dimension_columns(table_name, is_hourly)
    sessions_table = "hourly_product_sessions" if is_hourly else "product_sessions_snapshot"

    order_select_dims = []
    session_select_dims = []
    group_by_dims = []

    for dim in dims:
        if dim == "metric_date":
            order_select_dims.append("DATE(so.created_at) AS metric_date")
            session_select_dims.append("date AS metric_date")
            group_by_dims.append("metric_date")
        elif dim == "metric_hour":
            order_select_dims.append("HOUR(so.created_at) AS metric_hour")
            session_select_dims.append("hour AS metric_hour")
            group_by_dims.append("metric_hour")
        else:
            order_select_dims.append(f"IFNULL(so.{dim}, 'Unknown') AS {dim}")
            session_select_dims.append(f"IFNULL({dim}, 'Unknown') AS {dim}")
            group_by_dims.append(dim)

    order_select_str = ", ".join(order_select_dims)
    session_select_str = ", ".join(session_select_dims)
    group_by_str = ", ".join(group_by_dims)

    return f"""
    SELECT
        {group_by_str},
        SUM(orders) AS orders,
        SUM(sales) AS sales,
        CASE WHEN SUM(orders) > 0 THEN SUM(sales) / SUM(orders) ELSE 0 END AS average_order_value,
        SUM(sessions) AS sessions,
        SUM(atc_sessions) AS atc_sessions,
        CASE WHEN SUM(sessions) > 0 THEN SUM(orders) / SUM(sessions) ELSE 0.0000 END AS conversion_rate,
        CASE WHEN SUM(orders) > 0 THEN SUM(cancelled_orders) / SUM(orders) ELSE 0.0000 END AS cancellation_rate,
        CASE WHEN SUM(orders) > 0 THEN SUM(refunded_orders) / SUM(orders) ELSE 0.0000 END AS refund_rate,
        SUM(prepaid_orders) AS prepaid_orders,
        SUM(cod_orders) AS cod_orders,
        SUM(ppcod_orders) AS ppcod_orders,
        SUM(cancelled_orders) AS cancelled_orders,
        SUM(refunded_orders) AS refunded_orders
    FROM (
        SELECT
            {order_select_str},
            COUNT(DISTINCT so.order_id) AS orders,
            COALESCE(SUM(so.total_price), 0) AS sales,
            0 AS sessions,
            0 AS atc_sessions,
            SUM(CASE WHEN payment_type = 'PREPAID' THEN 1 ELSE 0 END) AS prepaid_orders,
            SUM(CASE WHEN payment_type = 'COD' THEN 1 ELSE 0 END) AS cod_orders,
            SUM(CASE WHEN payment_type = 'PPCOD' THEN 1 ELSE 0 END) AS ppcod_orders,
            SUM(IFNULL(r.is_cancelled, 0)) AS cancelled_orders,
            SUM(IFNULL(r.is_refunded, 0)) AS refunded_orders
        FROM (
            SELECT *, ({PAYMENT_TYPE_CASE_SQL}) AS payment_type
            FROM shopify_orders
            WHERE DATE(created_at) = %s
        ) so
        LEFT JOIN (
            SELECT order_id,
                   MAX(CASE WHEN event_type = 'CANCEL' THEN 1 ELSE 0 END) AS is_cancelled,
                   MAX(CASE WHEN event_type = 'REFUND' THEN 1 ELSE 0 END) AS is_refunded
            FROM returns_fact
            GROUP BY order_id
        ) r ON so.order_id = r.order_id
        GROUP BY {group_by_str}

        UNION ALL

        SELECT
            {session_select_str},
            0 AS orders,
            0 AS sales,
            SUM(IFNULL(sessions, 0)) AS sessions,
            SUM(IFNULL(sessions_with_cart_additions, 0)) AS atc_sessions,
            0 AS prepaid_orders,
            0 AS cod_orders,
            0 AS ppcod_orders,
            0 AS cancelled_orders,
            0 AS refunded_orders
        FROM {sessions_table}
        WHERE date = %s
        GROUP BY {group_by_str}
    ) combined
    GROUP BY {group_by_str};
    """


def build_bulk_insert_query(table_name: str, columns: List[str], num_rows: int) -> str:
    cols_csv = ", ".join(columns)
    placeholder_group = "(" + ", ".join(["%s"] * len(columns)) + ")"
    values_csv = ", ".join([placeholder_group] * num_rows)
    return f"INSERT INTO {table_name} ({cols_csv}) VALUES {values_csv};"


class AggregateRunner:
    INSERT_CHUNK_SIZE = 500

    def __init__(self, brand_info: Dict[str, Any], target_dates: List[str]):
        self.brand_info = brand_info
        self.target_dates = target_dates
        self.db_manager = DBManager(
            brand_name=brand_info["brand_name"],
            host=brand_info["host"],
            user=brand_info["user"],
            password=brand_info["password"],
            database=brand_info["database"],
            port=brand_info.get("port", 3306),
        )

    @retry_db_operation()
    def initialize(self):
        ensure_tables_exist(self.db_manager)

    def _rebuild_table(self, table_name: str, is_hourly: bool, metric_date: str):
        columns = get_all_columns(table_name, is_hourly)
        delete_query = build_delete_query(table_name)
        self.db_manager.execute_query(delete_query, (metric_date,), commit=True)

        select_query = build_select_aggregation_query(table_name, is_hourly)
        rows = self.db_manager.execute_query(select_query, (metric_date, metric_date), commit=False)
        if not rows:
            return

        for start in range(0, len(rows), self.INSERT_CHUNK_SIZE):
            chunk = rows[start:start + self.INSERT_CHUNK_SIZE]
            insert_query = build_bulk_insert_query(table_name, columns, len(chunk))
            params: List[Any] = []
            for row in chunk:
                params.extend(row[col] for col in columns)
            self.db_manager.execute_query(insert_query, tuple(params), commit=True)

    @retry_db_operation()
    def run_aggregations(self):
        all_tables = [(t, False) for t in DAILY_TABLES] + [(t, True) for t in HOURLY_TABLES]
        try:
            for metric_date in self.target_dates:
                logger.info(
                    f"[{self.brand_info['brand_name']}] Rebuilding UTM aggregates for {metric_date}"
                )
                for table_name, is_hourly in all_tables:
                    self._rebuild_table(table_name, is_hourly, metric_date)
        finally:
            self.db_manager.close()


def fetch_brand_config(brand_id: str) -> Dict[str, Any] | None:
    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error("GET_BRANDS_API or PIPELINE_AUTH_HEADER not configured for UTM backfill.")
        return None

    url = f"{GET_BRANDS_API.rstrip('/')}/{brand_id.lower()}"
    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"Failed to fetch brand config for brand_id={brand_id}: {resp.status_code}")
        return None
    except Exception as exc:
        logger.error(f"Brand config fetch failed for brand_id={brand_id}: {exc}")
        return None


def build_brand_info(config: Dict[str, Any], fallback_brand_name: str) -> Dict[str, Any]:
    encrypted_password = config.get("db_password", "")
    try:
        password = decrypt(encrypted_password) if encrypted_password else ""
    except Exception as exc:
        logger.error(f"Failed to decrypt db_password for {fallback_brand_name}: {exc}")
        raise

    return {
        "brand_name": config.get("brand_name") or fallback_brand_name,
        "host": config.get("db_host", "localhost"),
        "user": config.get("db_user", "root"),
        "password": password,
        "database": config.get("db_database") or fallback_brand_name,
        "port": int(config.get("port", 3306)),
    }


def process_event(data: dict) -> bool:
    brand_id = str(data.get("brand_id") or "").strip()
    brand_tag = str(data.get("brand_tag") or data.get("brand_name") or "").strip().lower()
    if not brand_id:
        logger.error("❌ Missing brand_id in onboarding payload for UTM backfill.")
        return False
    if not brand_tag:
        logger.error("❌ Missing brand_tag/brand_name in onboarding payload for UTM backfill.")
        return False

    start_ymd, end_ymd = get_onboarding_backfill_window()
    target_dates = build_inclusive_date_range(start_ymd, end_ymd)
    logger.info(
        f"📦 [{brand_tag}] Starting UTM aggregation backfill for onboarding window {start_ymd} -> {end_ymd}"
    )

    config = fetch_brand_config(brand_id)
    if not config:
        return False

    try:
        brand_info = build_brand_info(config, brand_tag.upper())
        runner = AggregateRunner(brand_info, target_dates)
        runner.initialize()
        runner.run_aggregations()
        logger.info(f"✅ [{brand_tag}] UTM aggregation backfill completed.")
        return True
    except Exception as exc:
        logger.error(f"❌ [{brand_tag}] UTM aggregation backfill failed: {exc}")
        return False
