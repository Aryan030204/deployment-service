from kafka_client.consumer import BrandEventConsumer
from shared.utils.logger import get_logger


logger = get_logger("deployment-service")


def main() -> None:
    logger.info("Starting deployment-service Kafka consumer")
    consumer = BrandEventConsumer()
    consumer.consume_forever()


if __name__ == "__main__":
    main()
