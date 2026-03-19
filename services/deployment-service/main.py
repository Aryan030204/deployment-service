import threading

from services.api_service import run as run_api_service
from services.database_service import run as run_database_service
from services.shopify_service import run as run_shopify_service
from services.tenant_router_service import run as run_tenant_router_service
from shared.utils.logger import get_logger


logger = get_logger("deployment-service")


def main() -> None:
    logger.info("Starting deployment-service orchestrator services")
    workers = [
        threading.Thread(target=run_shopify_service, name="shopify-service"),
        threading.Thread(target=run_database_service, name="database-service"),
        threading.Thread(target=run_tenant_router_service, name="tenant-router-service"),
        threading.Thread(target=run_api_service, name="api-service"),
    ]

    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()


if __name__ == "__main__":
    main()
