import json

from confluent_kafka import Consumer

from config.settings import KAFKA_BROKER, KAFKA_TOPIC
from kafka_client.producer import publish_brand_event
from scripts.create_pipelinecreds import process_event as process_pipelinecreds_event
from scripts.create_tenant_record import process_event as process_tenant_event
from services.workflow import (
    STATE_PIPELINE_CREDENTIALS_CREATED,
    STATE_SPEED_KEY_GENERATED,
    STATE_TENANT_RECORD_CREATED,
    ensure_workflow_payloads,
)
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.tenant-router-service")


def run():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "tenant-router-service-group",
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

                if state == STATE_SPEED_KEY_GENERATED:
                    if not process_tenant_event(data):
                        logger.error("Create tenant failed for %s", data.get("brand_name"))
                        continue
                    data["state"] = STATE_TENANT_RECORD_CREATED
                    publish_brand_event(data)
                    logger.info("Published state=%s for %s", STATE_TENANT_RECORD_CREATED, data.get("brand_name"))

                elif state == STATE_TENANT_RECORD_CREATED:
                    if not process_pipelinecreds_event(data):
                        logger.error("Create pipelinecreds failed for %s", data.get("brand_name"))
                        continue
                    data["state"] = STATE_PIPELINE_CREDENTIALS_CREATED
                    publish_brand_event(data)
                    logger.info("Published state=%s for %s", STATE_PIPELINE_CREDENTIALS_CREATED, data.get("brand_name"))

            except Exception as exc:
                logger.error("Error processing message in tenant router service: %s", exc)
    finally:
        consumer.close()
