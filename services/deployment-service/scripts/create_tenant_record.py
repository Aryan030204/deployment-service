# -*- coding: utf-8 -*-
"""
Standalone Tenant Record Creator
Consumes brand events from Kafka and creates tenant records via API.
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

# Load .env relative from root
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from dotenv import load_dotenv

load_dotenv(os.path.join(root_dir, ".env"))

CREATE_TENANT_API = os.getenv("CREATE_TENANT_API")
X_PIPELINE_KEY = os.getenv("PIPELINE_AUTH_HEADER")
READ_REPLICA = os.getenv("READ_REPLICA")
DB_HOST = os.getenv("DB_HOST")
DB_PASSWORD = os.getenv("DB_PASSWORD")
API_VERSION = os.getenv("API_VERSION")


def _missing_env_vars() -> list[str]:
    required = {
        "CREATE_TENANT_API": CREATE_TENANT_API,
        "X_PIPELINE_KEY": X_PIPELINE_KEY,
        "READ_REPLICA": READ_REPLICA,
        "DB_HOST": DB_HOST,
        "DB_PASSWORD": DB_PASSWORD,
        "API_VERSION": API_VERSION,
    }
    return [name for name, value in required.items() if not value]


def _build_payload(data: dict) -> dict:
    brand_id = data.get("brand_id")
    brand_name = data.get("brand_name")
    shop_name = data.get("shop_name")
    access_token = data.get("access_token", "")
    app_id_mapping = data.get("app_id_mapping", "")
    speed_key = data.get("speed_key", "")

    return {
        "brand_id": brand_name.upper(),
        "brand_num": brand_id,
        "shard_id": brand_name.lower(),
        "rds_proxy_endpoint": READ_REPLICA,
        "database": brand_name,
        "db_host": DB_HOST,
        "user": "admin",
        "app_id_mapping": app_id_mapping,
        "password": DB_PASSWORD,
        "shop_name": shop_name,
        "api_version": API_VERSION,
        "access_token": access_token,
        "port": 3306,
        "status": "active",
        "speed_key": speed_key,
        "session_url": "",
    }


def create_tenant_record(payload: dict) -> bool:
    missing_env = _missing_env_vars()
    if missing_env:
        print(f"❌ Missing required env vars: {', '.join(missing_env)}")
        return False

    headers = {"x-pipeline-key": X_PIPELINE_KEY}
    response = requests.post(
        CREATE_TENANT_API,
        json=payload,
        headers=headers,
        timeout=30,
    )
    if 200 <= response.status_code < 300:
        return True
    print(f"❌ Tenant API failed ({response.status_code}): {response.text[:500]}")
    return False


def process_event(data: dict) -> bool:
    tenant_payload = data.get("tenant_payload") or _build_payload(data)
    brand_name = data.get("brand_name")
    if not tenant_payload.get("brand_num") or not tenant_payload.get("brand_id") or not tenant_payload.get("shop_name"):
        print("⚠️ Missing required fields for tenant payload.")
        return False

    ok = create_tenant_record(tenant_payload)
    if ok:
        print(f"✅ Tenant record created for brand_name={brand_name}")
    return ok


def run():
    print("🚀 Starting Create Tenant Record Consumer")

    missing_env = _missing_env_vars()
    if missing_env:
        print(f"❌ Missing required env vars: {', '.join(missing_env)}")
        return

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "create-tenant-record-service-group",
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
                brand_id = data.get("brand_id")
                brand_name = data.get("brand_name")
                shop_name = data.get("shop_name")

                if not all([brand_id, brand_name, shop_name]):
                    print("⚠️ Missing required fields: brand_id, brand_name, shop_name")
                    continue

                process_event(data)

            except json.JSONDecodeError as exc:
                print(f"❌ Invalid JSON payload: {exc}")
            except requests.RequestException as exc:
                print(f"❌ Tenant API request error: {exc}")
            except Exception as exc:
                print(f"❌ Error processing message: {exc}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
