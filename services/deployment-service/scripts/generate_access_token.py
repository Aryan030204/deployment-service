# -*- coding: utf-8 -*-
"""
Standalone Shopify OAuth Access Token Generator
Listens on Kafka for brand events containing encrypted client_id and client_secret,
performs the OAuth authorization flow, and returns the access_token.
"""

import os
import sys
import json
import base64
import requests
from urllib.parse import urlparse, parse_qs

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

from confluent_kafka import Consumer

# Add config path for settings
service_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

from config.settings import KAFKA_BROKER, KAFKA_TOPIC

# Load .env from root
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)
from dotenv import load_dotenv
load_dotenv(os.path.join(root_dir, ".env"))
from shared.utils.encryption import decrypt

PASSWORD_AES_KEY = os.getenv("PASSWORD_AES_KEY")
REDIRECT_URI = "https://api.trytechit.co/health"


# ---------------------------
# AES-256-CBC Decryption
# ---------------------------
from shared.utils.logger import get_logger

logger = get_logger("deployment-service.access-token-script")

def decrypt_value(encrypted_val: str) -> str:
    """
    Decrypts a value encrypted using AES-256-CBC using shared utility.
    """
    try:
        return decrypt(encrypted_val)
    except Exception as e:
        logger.error(f"❌ Decryption failed: {e}")
        return encrypted_val


# ---------------------------
# Shopify OAuth Flow
# ---------------------------
def generate_access_token(shop_name: str, client_id: str, client_secret: str, code: str = None) -> str | None:
    """
    Performs the Shopify OAuth flow:
    1. GET authorize → extract 'code' from redirect URL (Skipped if code provided)
    2. POST access_token with code → get access_token
    """
    if code:
        logger.info(f"⏩ Code provided in payload: {code[:8]}... skipping Step 1 authorization.")
    else:
        # Step 1: Authorize - GET request (allow_redirects=False to capture redirect URL)
        authorize_url = (
            f"https://{shop_name}.myshopify.com/admin/oauth/authorize"
            f"?client_id={client_id}"
            f"&redirect_uri={REDIRECT_URI}"
        )
        logger.info(f"🔗 Step 1 - Authorizing: {authorize_url}")

        try:
            resp = requests.get(authorize_url, allow_redirects=False, timeout=30)
            redirect_url = resp.headers.get("Location", resp.url)

            if not redirect_url:
                logger.error(f"❌ No redirect URL found. Status: {resp.status_code}")
                return None

            logger.info(f"🔀 Redirect URL: {redirect_url}")

            parsed = urlparse(redirect_url)
            params = parse_qs(parsed.query)
            code = params.get("code", [None])[0]

            if not code:
                logger.error(f"❌ 'code' param not found in redirect URL")
                return None

            logger.info(f"✅ Authorization code received: {code[:8]}...")

        except Exception as e:
            logger.error(f"❌ Authorization request failed: {e}")
            return None

    # Step 2: Exchange code for access_token
    token_url = f"https://{shop_name}.myshopify.com/admin/oauth/access_token"
    logger.info(f"🔄 Step 2 - Exchanging code for token: {token_url}")

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
    }

    try:
        resp = requests.post(token_url, json=payload, timeout=30)
        
        if resp.status_code == 200:
            token_data = resp.json()
            access_token = token_data.get("access_token")
            if access_token:
                logger.info("✅ Access token generated successfully")
                return access_token
            else:
                logger.error(f"❌ 'access_token' not in response: {token_data}")
                return None
        else:
            logger.error(f"❌ Token exchange failed: {resp.status_code}")
            logger.error(f"   Response: {resp.text[:500]}")
            return None

    except Exception as e:
        logger.error(f"❌ Token exchange request failed: {e}")
        return None


def process_event(data: dict) -> str | None:
    logger.info(f"DEBUG process_event data keys: {list(data.keys())} | code is: {data.get('code')}")
    shop_name = data.get("shop_name")
    encrypted_client_id = data.get("client_id")
    encrypted_client_secret = data.get("client_secret")

    if not all([shop_name, encrypted_client_id, encrypted_client_secret]):
        logger.warning("⚠️ Missing required fields: shop_name, client_id, client_secret")
        return None

    client_id = decrypt_value(encrypted_client_id)
    client_secret = decrypt_value(encrypted_client_secret)
    code = data.get("code")
    return generate_access_token(shop_name, client_id, client_secret, code)


# ---------------------------
# Kafka Consumer
# ---------------------------
def run():
    print("🚀 Starting Access Token Generator Consumer")

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "access-token-service-group",
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
                payload = message.value().decode("utf-8")
                print(f"📩 Received Event: {payload}")

                data = json.loads(payload)
                access_token = process_event(data)
                shop_name = data.get("shop_name")

                if access_token:
                    print(f"🎉 Final Access Token for {shop_name}: {access_token}")
                else:
                    print(f"❌ Failed to generate access token for {shop_name}")

            except Exception as e:
                print(f"❌ Error processing message: {e}")

    finally:
        consumer.close()


if __name__ == "__main__":
    run()
