import json

from confluent_kafka import Producer

from config.settings import KAFKA_BROKER, KAFKA_TOPIC
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.producer")


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.error("Failed to deliver message: %s", err)
        return
    logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def publish_brand_event(payload) -> None:
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    producer.produce(
        topic=KAFKA_TOPIC,
        value=json.dumps(payload).encode("utf-8"),
        callback=_delivery_report,
    )
    producer.flush()
