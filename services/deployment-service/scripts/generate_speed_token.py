# -*- coding: utf-8 -*-
"""
Standalone Speed Token Generator
Consumes brand events from Kafka and creates speed API keys via API.
"""

import os
import sys
import json
import requests
from confluent_kafka import Consumer

# Add config path for settings
service_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

from config.settings import KAFKA_BROKER, KAFKA_TOPIC

# Load .env from root
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from dotenv import load_dotenv

load_dotenv(os.path.join(root_dir, ".env"))

GET_SPEED_TOKEN_API = os.getenv("GET_SPEED_TOKEN_API")
PIPELINE_AUTH_HEADER = os.getenv("X_PIPELINE_KEY")
PERMISSIONS = ["upload:files", "read:files", "metrics:read"]


def _missing_env_vars() -> list[str]:
    required = {
        "GET_SPEED_TOKEN_API": GET_SPEED_TOKEN_API,
        "PIPELINE_AUTH_HEADER": PIPELINE_AUTH_HEADER,
    }
    return [name for name, value in required.items() if not value]


def _build_payload(brand_name: str) -> dict:
    return {
        "brand_key": brand_name,
        "name": brand_name,
        "permissions": PERMISSIONS,
    }


def run():
    print("🚀 Starting Speed Token Generator Consumer")

    missing_env = _missing_env_vars()
    if missing_env:
        print(f"❌ Missing required env vars: {', '.join(missing_env)}")
        return

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "speed-token-service-group",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    print(f"📡 Subscribed to Kafka Topic: {KAFKA_TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            try:
                raw_payload = message.value().decode("utf-8")
                print(f"📩 Received Event: {raw_payload}")

                data = json.loads(raw_payload)
                brand_name = data.get("brand_name")

                if not brand_name:
                    print("⚠️ Missing required field: brand_name")
                    continue

                payload = _build_payload(brand_name)
                headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}

                response = requests.post(
                    GET_SPEED_TOKEN_API,
                    json=payload,
                    headers=headers,
                    timeout=30,
                )

                if 200 <= response.status_code < 300:
                    response_data = response.json()
                    plain_key = response_data.get("plain_key")
                    if plain_key:
                        print(f"✅ Speed token generated for {brand_name}: {plain_key}")
                    else:
                        print(f"❌ plain_key missing in response for {brand_name}: {response_data}")
                else:
                    print(f"❌ Speed token API failed ({response.status_code}): {response.text[:500]}")

            except json.JSONDecodeError as exc:
                print(f"❌ Invalid JSON payload: {exc}")
            except requests.RequestException as exc:
                print(f"❌ Speed token API request error: {exc}")
            except Exception as exc:
                print(f"❌ Error processing message: {exc}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
