from typing import List
from app.services.payment_classifier import PAYMENT_TYPE_CASE_SQL

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

def get_dimension_columns(table_name: str, is_hourly: bool = False) -> List[str]:
    parts = table_name.replace('_daily', '').replace('_hourly', '').split('_')[1:]
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

    # Selection of sessions table based on granularity
    sessions_table = "hourly_product_sessions" if is_hourly else "product_sessions_snapshot"

    order_select_dims = []
    session_select_dims = []
    group_by_dims = []

    for d in dims:
        if d == "metric_date":
            # standardizing on 'metric_date' but using original columns
            order_select_dims.append("DATE(so.created_at) AS metric_date")
            session_select_dims.append("date AS metric_date")
            group_by_dims.append("metric_date")
        elif d == "metric_hour":
            order_select_dims.append("HOUR(so.created_at) AS metric_hour")
            session_select_dims.append("hour AS metric_hour")
            group_by_dims.append("metric_hour")
        else:
            # utm_source, utm_medium, etc.
            order_select_dims.append(f"IFNULL(so.{d}, 'Unknown') AS {d}")
            session_select_dims.append(f"IFNULL({d}, 'Unknown') AS {d}")
            group_by_dims.append(d)

    order_select_str = ", ".join(order_select_dims)
    session_select_str = ", ".join(session_select_dims)
    group_by_str = ", ".join(group_by_dims)

    select_sql = f"""
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
        -- Orders data with returns information
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

        -- Sessions data from snapshot table (Daily or Hourly)
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

    return select_sql

def build_bulk_insert_query(table_name: str, columns: List[str], num_rows: int) -> str:
    cols_csv = ", ".join(columns)
    placeholder_group = "(" + ", ".join(["%s"] * len(columns)) + ")"
    values_csv = ", ".join([placeholder_group] * num_rows)
    return f"INSERT INTO {table_name} ({cols_csv}) VALUES {values_csv};"
