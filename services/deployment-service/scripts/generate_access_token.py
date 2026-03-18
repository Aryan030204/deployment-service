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
from dotenv import load_dotenv
load_dotenv(os.path.join(root_dir, ".env"))

PASSWORD_AES_KEY = os.getenv("PASSWORD_AES_KEY")
REDIRECT_URI = "https://api.trytechit.co/health"


# ---------------------------
# AES-256-CBC Decryption
# ---------------------------
def decrypt_value(encrypted_val: str) -> str:
    """
    Decrypts a value encrypted using AES-256-CBC.
    Expected format: 'iv_b64:ciphertext_b64'
    """
    if not encrypted_val or ":" not in encrypted_val:
        return encrypted_val

    if not PASSWORD_AES_KEY:
        print("⚠️ PASSWORD_AES_KEY not set. Returning raw value.")
        return encrypted_val

    try:
        iv_b64, ciphertext_b64 = encrypted_val.split(":", 1)
        iv = base64.b64decode(iv_b64)
        ciphertext = base64.b64decode(ciphertext_b64)

        key_bytes = PASSWORD_AES_KEY.encode("utf-8")
        if len(key_bytes) < 32:
            key_bytes = key_bytes.ljust(32, b'\0')
        elif len(key_bytes) > 32:
            key_bytes = key_bytes[:32]

        cipher = Cipher(algorithms.AES(key_bytes), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        padded_content = decryptor.update(ciphertext) + decryptor.finalize()

        # PKCS7 unpadding
        padding_len = padded_content[-1]
        if padding_len < 1 or padding_len > 16:
            return padded_content.decode("utf-8", errors="ignore")

        content = padded_content[:-padding_len]
        return content.decode("utf-8")
    except Exception as e:
        print(f"❌ Decryption failed: {e}")
        return encrypted_val


# ---------------------------
# Shopify OAuth Flow
# ---------------------------
def generate_access_token(shop_name: str, client_id: str, client_secret: str) -> str | None:
    """
    Performs the Shopify OAuth flow:
    1. GET authorize → extract 'code' from redirect URL
    2. POST access_token with code → get access_token
    """
    # Step 1: Authorize - GET request (allow_redirects=False to capture redirect URL)
    authorize_url = (
        f"https://{shop_name}.myshopify.com/admin/oauth/authorize"
        f"?client_id={client_id}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    print(f"🔗 Step 1 - Authorizing: {authorize_url}")

    try:
        resp = requests.get(authorize_url, allow_redirects=False, timeout=30)

        # The code comes in the redirect Location header or the final URL
        redirect_url = resp.headers.get("Location", resp.url)

        if not redirect_url:
            print(f"❌ No redirect URL found. Status: {resp.status_code}")
            print(f"   Response: {resp.text[:500]}")
            return None

        print(f"🔀 Redirect URL: {redirect_url}")

        # Parse 'code' from the redirect URL query params
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)
        code = params.get("code", [None])[0]

        if not code:
            print(f"❌ 'code' param not found in redirect URL: {redirect_url}")
            return None

        print(f"✅ Authorization code received: {code[:8]}...")

    except Exception as e:
        print(f"❌ Authorization request failed: {e}")
        return None

    # Step 2: Exchange code for access_token
    token_url = f"https://{shop_name}.myshopify.com/admin/oauth/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "code": code,
        "grant_type": "authorization_code",
    }
    print(f"🔗 Step 2 - Exchanging code for token: {token_url}")

    try:
        resp = requests.post(token_url, json=payload, timeout=30)

        if resp.status_code in (200, 201):
            data = resp.json()
            access_token = data.get("access_token")
            if access_token:
                print(f"✅ Access Token obtained: {access_token[:8]}...")
                return access_token
            else:
                print(f"❌ 'access_token' not in response: {data}")
                return None
        else:
            print(f"❌ Token exchange failed: {resp.status_code}")
            print(f"   Response: {resp.text[:500]}")
            return None

    except Exception as e:
        print(f"❌ Token exchange request failed: {e}")
        return None


def process_event(data: dict) -> str | None:
    shop_name = data.get("shop_name")
    encrypted_client_id = data.get("client_id")
    encrypted_client_secret = data.get("client_secret")

    if not all([shop_name, encrypted_client_id, encrypted_client_secret]):
        print("⚠️ Missing required fields: shop_name, client_id, client_secret")
        return None

    client_id = decrypt_value(encrypted_client_id)
    client_secret = decrypt_value(encrypted_client_secret)
    return generate_access_token(shop_name, client_id, client_secret)


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
