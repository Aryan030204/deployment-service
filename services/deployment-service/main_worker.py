from services.worker_service import run_worker
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.worker-main")


def main() -> None:
    logger.info("Starting deployment-service onboarding worker")
    run_worker()


if __name__ == "__main__":
    main()
