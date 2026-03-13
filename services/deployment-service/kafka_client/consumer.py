from confluent_kafka import Consumer

from config.settings import KAFKA_BROKER, KAFKA_TOPIC
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.consumer")


class BrandEventConsumer:
    def __init__(self) -> None:
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BROKER,
                "group.id": "deployment-service-group",
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([KAFKA_TOPIC])
        logger.info("Subscribed to topic: %s", KAFKA_TOPIC)

    def consume_forever(self) -> None:
        try:
            while True:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    logger.error("Kafka consumer error: %s", message.error())
                    continue

                payload = message.value().decode("utf-8") if message.value() else ""
                logger.info("Deployment Service received event from %s", KAFKA_TOPIC)
                logger.info("Message payload: %s", payload)
        finally:
            self.consumer.close()
