from typing import Dict, Any, List
from app.utils.logger import get_logger
from app.utils.retry import retry_db_operation
from app.services.db_manager import DBManager
from app.services.query_builder import (
    build_delete_query,
    build_select_aggregation_query,
    build_bulk_insert_query,
    get_all_columns,
)
from app.sql.setup import ensure_tables_exist, DAILY_TABLES, HOURLY_TABLES

logger = get_logger(__name__)

INSERT_CHUNK_SIZE = 500

class AggregateRunner:
    def __init__(self, brand_info: Dict[str, Any], target_dates: List[str]):
        self.brand_info = brand_info
        self.target_dates = target_dates
        self.db_manager = DBManager(
            brand_name=brand_info["brand_name"],
            host=brand_info["host"],
            user=brand_info["user"],
            password=brand_info["password"],
            database=brand_info["database"],
            port=brand_info.get("port", 3306)
        )

    @retry_db_operation()
    def initialize(self):
        """Ensures that the aggregate tables exist before we try to populate them."""
        ensure_tables_exist(self.db_manager)

    def _rebuild_table(self, table_name: str, is_hourly: bool, metric_date: str):
        columns = get_all_columns(table_name, is_hourly)

        # 1. Delete current day's aggregates
        delete_query = build_delete_query(table_name)
        self.db_manager.execute_query(delete_query, (metric_date,), commit=True)

        # 2. Compute aggregated rows via a lock-free SELECT (pure MVCC) so
        #    we don't hold next-key locks on shopify_orders while the orders
        #    pipeline is writing to it.
        select_query = build_select_aggregation_query(table_name, is_hourly)
        rows = self.db_manager.execute_query(
            select_query, (metric_date, metric_date), commit=False
        )

        if not rows:
            return

        # 3. Bulk insert the fetched rows into the aggregate table in chunks.
        for start in range(0, len(rows), INSERT_CHUNK_SIZE):
            chunk = rows[start:start + INSERT_CHUNK_SIZE]
            insert_query = build_bulk_insert_query(table_name, columns, len(chunk))
            params: List[Any] = []
            for row in chunk:
                params.extend(row[c] for c in columns)
            self.db_manager.execute_query(insert_query, tuple(params), commit=True)

    @retry_db_operation()
    def run_aggregations(self):
        brand_name = self.brand_info["brand_name"]
        all_tables = [(t, False) for t in DAILY_TABLES] + [(t, True) for t in HOURLY_TABLES]

        try:
            for metric_date in self.target_dates:
                logger.info(f"Starting aggregations for brand: {brand_name}, date: {metric_date}")

                for table_name, is_hourly in all_tables:
                    logger.debug(f"Rebuilding {table_name} for date {metric_date}")
                    self._rebuild_table(table_name, is_hourly, metric_date)

            logger.info(f"Successfully rebuilt all tables for {brand_name} across {len(self.target_dates)} day(s).")
        except Exception as e:
            logger.error(f"Failed to rebuild aggregate tables for {brand_name}: {e}")
            raise e
        finally:
            self.db_manager.close()
