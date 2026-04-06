# -*- coding: utf-8 -*-
"""
Standalone Speed Token Generator
Creates speed API keys via API.
"""

import os
import sys
import json
import requests

# Add config path for settings
service_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

# Load .env from root
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
from dotenv import load_dotenv

load_dotenv(os.path.join(root_dir, ".env"))

GET_SPEED_TOKEN_API = os.getenv("GET_SPEED_TOKEN_API")
PIPELINE_AUTH_HEADER = os.getenv("PIPELINE_AUTH_HEADER")
PERMISSIONS = ["read:data", "upload:files"]


def _missing_env_vars() -> list[str]:
    required = {
        "GET_SPEED_TOKEN_API": GET_SPEED_TOKEN_API,
        "PIPELINE_AUTH_HEADER": PIPELINE_AUTH_HEADER,
    }
    return [name for name, value in required.items() if not value]


def _build_payload(brand_name: str) -> dict:
    return {
        "brand_key": brand_name,
        "name": "Pipeline Integration User",
        "permissions": PERMISSIONS,
    }


def generate_speed_key(brand_name: str) -> str | None:
    missing_env = _missing_env_vars()
    if missing_env:
        print(f"❌ Missing required env vars: {', '.join(missing_env)}")
        return None
    if not brand_name:
        print("⚠️ Missing required field: brand_name")
        return None

    payload = _build_payload(brand_name)
    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}
    try:
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
                from shared.utils.encryption import encrypt
                return encrypt(plain_key)
            return None

        print(f"❌ Speed token API failed ({response.status_code}): {response.text[:500]}")
        return None


    except Exception as e:
        print(f"❌ Speed token API request exception: {e}")
        print(f"❌ Speed token API request exception: {e}")
        return None



def process_event(data: dict) -> str | None:
    brand_name = data.get("brand_name")
    website_url = data.get("website_url")

    if brand_name and website_url:
        speed_db_uri = os.getenv("SPEED_DB_URI")
        if speed_db_uri:
            try:
                from pymongo import MongoClient
                client = MongoClient(speed_db_uri)
                db = client["pagespeed_brands"]
                collection = db["brands"]
                
                # Upsert to prevent duplicates
                collection.update_one(
                    {"brand_name": brand_name},
                    {"$addToSet": {"urls": website_url}},
                    upsert=True
                )
                print(f"✅ Created/Updated MongoDB document for {brand_name}")
            except Exception as e:
                print(f"❌ MongoDB error for {brand_name}: {e}")
        else:
            print("⚠️ SPEED_DB_URI not set in environment, skipping MongoDB insertion")

    return generate_speed_key(brand_name)


# Kafka Consumer logic removed.
pass


if __name__ == "__main__":
    # run() removed.
    pass
