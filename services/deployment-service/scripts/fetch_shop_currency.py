# -*- coding: utf-8 -*-
"""
Fetch the Shopify shop default currency for onboarding.
"""

import os
import sys

import requests

service_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from dotenv import load_dotenv

load_dotenv(os.path.join(root_dir, ".env"))

from shared.utils.logger import get_logger


logger = get_logger("deployment-service.shop-currency")


def fetch_shop_currency(shop_name: str, access_token: str, api_version: str) -> str | None:
    if not shop_name or not access_token or not api_version:
        logger.error("Missing shop_name/access_token/api_version for shop currency fetch")
        return None

    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/shop.json"
    try:
        resp = requests.get(
            url,
            headers={"X-Shopify-Access-Token": access_token},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.error(f"Shop currency fetch failed: {resp.status_code} {resp.text[:500]}")
            return None
        shop = resp.json().get("shop") or {}
        currency = str(shop.get("currency") or "").strip().upper()
        if not currency:
            logger.error("Shop currency missing in Shopify shop response")
            return None
        return currency
    except Exception as exc:
        logger.error(f"Shop currency fetch request failed: {exc}")
        return None


def process_event(data: dict) -> str | None:
    shop_name = data.get("shop_name")
    access_token = data.get("access_token")
    api_version = data.get("api_version") or os.getenv("API_VERSION", "2024-04")
    return fetch_shop_currency(shop_name, access_token, api_version)
