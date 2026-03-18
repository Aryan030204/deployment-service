import json
import time

from confluent_kafka import Consumer

from config.settings import KAFKA_BROKER, KAFKA_TOPIC
from kafka_client.producer import publish_brand_event
from scripts.backfill_data import process_event as process_backfill_event
from scripts.clone_database import process_event as process_clone_db_event
from services.workflow import (
    STATE_ACCESS_TOKEN_GENERATED,
    STATE_DATA_BACKFILL_COMPLETED,
    STATE_DATABASE_CREATED,
    STATE_PIPELINE_CREDENTIALS_CREATED,
    STATE_TENANT_ADDED,
    ensure_workflow_payloads,
)
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.database-service")


def run():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "database-service-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    logger.info("Subscribed to topic: %s", KAFKA_TOPIC)

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                logger.error("Kafka error: %s", message.error())
                continue

            try:
                payload = message.value().decode("utf-8")
                data = ensure_workflow_payloads(json.loads(payload))
                state = data.get("state")

                if state == STATE_ACCESS_TOKEN_GENERATED:
                    if not process_clone_db_event(data):
                        logger.error("Clone database failed for %s", data.get("brand_name"))
                        continue
                    data["state"] = STATE_DATABASE_CREATED
                    publish_brand_event(data)
                    logger.info("Published state=%s for %s", STATE_DATABASE_CREATED, data.get("brand_name"))

                elif state == STATE_PIPELINE_CREDENTIALS_CREATED:
                    if not process_backfill_event(data):
                        logger.error("Backfill failed for %s", data.get("brand_name"))
                        continue
                    data["state"] = STATE_DATA_BACKFILL_COMPLETED
                    publish_brand_event(data)
                    logger.info("Published state=%s for %s", STATE_DATA_BACKFILL_COMPLETED, data.get("brand_name"))

                    time.sleep(10)
                    data["state"] = STATE_TENANT_ADDED
                    publish_brand_event(data)
                    logger.info("Published state=%s for %s", STATE_TENANT_ADDED, data.get("brand_name"))

            except Exception as exc:
                logger.error("Error processing message in database service: %s", exc)
    finally:
        consumer.close()
