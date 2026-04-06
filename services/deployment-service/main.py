from services.api_service import run as run_api_service
from shared.utils.logger import get_logger


logger = get_logger("deployment-service")


def main() -> None:
    logger.info("Starting deployment-service API orchestrator")
    run_api_service()


if __name__ == "__main__":
    main()
