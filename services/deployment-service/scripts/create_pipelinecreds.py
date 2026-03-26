# -*- coding: utf-8 -*-
"""
Standalone PipelineCreds Creator
Consumes brand events from Kafka and creates pipelinecreds via API.
"""

import os
import sys
import json
from datetime import datetime, timezone

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

CREATE_PIPELINECREDS_API = os.getenv("CREATE_PIPELINECREDS_API")
API_VERSION = os.getenv("API_VERSION")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_PASSWORD = os.getenv("DB_PASSWORD")
PIPELINE_KEY = os.getenv("PIPELINE_AUTH_HEADER")


def _missing_env_vars() -> list[str]:
    required = {
        "CREATE_PIPELINECREDS_API": CREATE_PIPELINECREDS_API,
        "API_VERSION": API_VERSION,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        "DB_PASSWORD": DB_PASSWORD,
        "PIPELINE_AUTH_HEADER": PIPELINE_KEY,
    }
    return [name for name, value in required.items() if not value]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _build_payload(data: dict) -> dict:
    brand_id = data.get("brand_id")
    brand_name = data.get("brand_name")
    shop_name = data.get("shop_name")
    now_iso = _utc_now_iso()
    db_user = "admin"
    db_port = int(DB_PORT)
    access_token = data.get("access_token", "")
    speed_key = data.get("speed_key", "")

    DEFAULT_APP_ID_MAPPING = """{
    "238372913153": "GoKwik",
    "580111": "Online Store",
    "1354745": "Draft Order"
}"""
    # Use mapping from event data (generated in tenant step) or fallback to default
    app_id_mapping = data.get("app_id_mapping") or DEFAULT_APP_ID_MAPPING

    brand_name_upper = str(brand_name).upper() if brand_name else ""
    return {
        "brand_id": brand_id,
        "brand_name": brand_name_upper,
        "brand_tag": str(brand_name).lower() if brand_name else "",
        "shop_name": shop_name,
        "api_version": API_VERSION,
        "access_token": access_token,
        "db_host": DB_HOST,
        "port": db_port,
        "db_database": brand_name_upper,
        "db_user": db_user,
        "db_password": DB_PASSWORD,
        "my_sql_url": f"mysql+pymysql://{db_user}:{DB_PASSWORD}@{DB_HOST}:{db_port}/{brand_name_upper}?charset=utf8mb4" + ("&ssl=true" if DB_HOST.lower() not in ["localhost", "127.0.0.1"] else ""),
        "app_id_mapping": app_id_mapping,
        "speed_key": speed_key,
        "created_at": now_iso,
        "updated_at": now_iso,
        "__v": 0,
    }


def create_pipelinecreds(payload: dict) -> bool:
    missing_env = _missing_env_vars()
    if missing_env:
        print(f"❌ Missing required env vars: {', '.join(missing_env)}")
        return False
    headers = {"x-pipeline-key": PIPELINE_KEY}
    print(f"DEBUG create_pipelinecreds payload to {CREATE_PIPELINECREDS_API}:\n{json.dumps(payload, indent=2)}")
    response = requests.post(
        CREATE_PIPELINECREDS_API,
        json=payload,
        headers=headers,
        timeout=30,
    )
    if 200 <= response.status_code < 300:
        return True
    print(f"❌ PipelineCreds API failed ({response.status_code}): {response.text[:500]}")
    return False


def process_event(data: dict) -> bool:
    pipeline_payload = data.get("pipelinecreds_payload") or _build_payload(data)
    now_iso = _utc_now_iso()
    pipeline_payload.setdefault("created_at", now_iso)
    pipeline_payload["updated_at"] = now_iso
    pipeline_payload.setdefault("__v", 0)
    if not pipeline_payload.get("brand_id") or not pipeline_payload.get("brand_name") or not pipeline_payload.get("shop_name"):
        print("⚠️ Missing required fields for pipelinecreds payload.")
        return False
    ok = create_pipelinecreds(pipeline_payload)
    if ok:
        print(f"✅ PipelineCreds created for brand_name={pipeline_payload.get('brand_name')}")
    return ok


def run():
    print("🚀 Starting PipelineCreds Creator Consumer")

    missing_env = _missing_env_vars()
    if missing_env:
        print(f"❌ Missing required env vars: {', '.join(missing_env)}")
        return

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "create-pipelinecreds-service-group",
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
                print(f"❌ PipelineCreds API request error: {exc}")
            except Exception as exc:
                print(f"❌ Error processing message: {exc}")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
