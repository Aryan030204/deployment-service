import json

from confluent_kafka import Consumer

from config.settings import KAFKA_BROKER, KAFKA_TOPIC
from kafka_client.producer import publish_brand_event
from scripts.generate_access_token import process_event as process_access_token_event
from scripts.generate_speed_token import process_event as process_speed_token_event
from services.workflow import (
    STATE_ACCESS_TOKEN_GENERATED,
    STATE_DATABASE_CREATED,
    STATE_INITIATED,
    STATE_SPEED_KEY_GENERATED,
    ensure_workflow_payloads,
    set_access_token,
    set_speed_key,
)
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.shopify-service")


def run():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "shopify-service-group",
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
                data = json.loads(payload)
                state = data.get("state", STATE_INITIATED)
                data = ensure_workflow_payloads(data)

                if state == STATE_INITIATED:
                    access_token = process_access_token_event(data)
                    if not access_token:
                        logger.error("Access token generation failed for %s", data.get("brand_name"))
                        continue
                    updated = set_access_token(data, access_token)
                    updated["state"] = STATE_ACCESS_TOKEN_GENERATED
                    publish_brand_event(updated)
                    logger.info("Published state=%s for %s", STATE_ACCESS_TOKEN_GENERATED, data.get("brand_name"))

                elif state == STATE_DATABASE_CREATED:
                    speed_key = process_speed_token_event(data)
                    if not speed_key:
                        logger.error("Speed token generation failed for %s", data.get("brand_name"))
                        continue
                    updated = set_speed_key(data, speed_key)
                    updated["state"] = STATE_SPEED_KEY_GENERATED
                    publish_brand_event(updated)
                    logger.info("Published state=%s for %s", STATE_SPEED_KEY_GENERATED, data.get("brand_name"))

            except Exception as exc:
                logger.error("Error processing message in shopify service: %s", exc)
    finally:
        consumer.close()
