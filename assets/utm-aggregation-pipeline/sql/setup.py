from app.utils.logger import get_logger

logger = get_logger(__name__)

DAILY_TABLES = [
    "utm_source_daily", "utm_medium_daily", "utm_campaign_daily",
    "utm_source_medium_daily", "utm_source_campaign_daily",
    "utm_medium_campaign_daily", "utm_source_medium_campaign_daily"
]

HOURLY_TABLES = [
    "utm_source_hourly", "utm_medium_hourly", "utm_campaign_hourly",
    "utm_source_medium_hourly", "utm_source_campaign_hourly",
    "utm_medium_campaign_hourly", "utm_source_medium_campaign_hourly"
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

def get_dimension_columns_and_indexes(table_name: str, is_hourly: bool = False):
    parts = table_name.replace('_daily', '').replace('_hourly', '').split('_')[1:]
    
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
        # Individual index for each dimension
        extra_indexes.append(f"INDEX idx_{table_name}_{col_name} ({col_name})")
        
    # Extra composite combos requested by user
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

def ensure_tables_exist(db_manager):
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
        try:
            db_manager.execute_query(create_sql, commit=True)
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise e
    logger.info("All UTM aggregate tables verified/created successfully.")
