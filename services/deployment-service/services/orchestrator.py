import os
import json
import ast
import time
import requests
from typing import Any, Callable, Dict, TypeVar

from shared.utils.logger import get_logger
from services.workflow import (
    STATE_INITIATED,
    STATE_ACCESS_TOKEN_GENERATED,
    STATE_DATABASE_CREATED,
    STATE_SPEED_KEY_GENERATED,
    STATE_TENANT_RECORD_CREATED,
    STATE_PIPELINE_CREDENTIALS_CREATED,
    STATE_DATA_BACKFILL_COMPLETED,
    STATE_TENANT_ADDED,
    ensure_workflow_payloads,
    set_access_token,
    set_shop_currency,
    set_speed_key,
)

from scripts.generate_access_token import process_event as process_access_token
from scripts.fetch_shop_currency import process_event as process_shop_currency
from scripts.clone_database import process_event as process_clone_db
from scripts.generate_speed_token import process_event as process_speed_token
from scripts.create_tenant_record import process_event as process_tenant_record
from scripts.create_pipelinecreds import process_event as process_pipelinecreds
from scripts.backfill_data import process_event as process_backfill

logger = get_logger("deployment-service.orchestrator")
T = TypeVar("T")


def _send_external_log(brand_id: str, brand_name: str, message: str) -> None:
    try:
        logs_url_str = (os.getenv("LOGS_URL", "") or "").strip()
        if not logs_url_str:
            return

        try:
            urls = json.loads(logs_url_str)
        except json.JSONDecodeError:
            try:
                urls = ast.literal_eval(logs_url_str)
            except (ValueError, SyntaxError):
                urls = [logs_url_str]

        auth_header = os.getenv("PIPELINE_AUTH_HEADER", "")

        if isinstance(urls, str):
            urls = [urls]

        if not isinstance(urls, list):
            return

        payload = {
            "brand_id": brand_id,
            "brand_name": brand_name,
            "log": message,
        }
        headers = {
            "x-pipeline-key": auth_header,
            "Content-Type": "application/json",
        }

        for url in urls:
            try:
                requests.post(url, json=payload, headers=headers, timeout=5)
            except Exception as exc:
                logger.error(f"Failed to send log to {url}: {exc}")
    except Exception as exc:
        logger.error(f"Error in _send_external_log: {exc}")


def _prefix(job_id: str) -> str:
    return f"[job_id={job_id}] " if job_id else ""


def _num_retries() -> int:
    raw = os.getenv("NUM_OF_RETRIES", "0").strip()
    try:
        value = int(raw)
    except ValueError:
        logger.warning(f"Invalid NUM_OF_RETRIES={raw!r}; defaulting to 0")
        return 0
    return max(0, value)


def _run_with_retries(
    action: Callable[[], T],
    *,
    step_label: str,
    brand_id: str,
    brand_name: str,
    log_prefix: str,
) -> T | None:
    retries = _num_retries()
    attempts = retries + 1
    for attempt in range(1, attempts + 1):
        try:
            result = action()
            if result:
                if attempt > 1:
                    msg = f"{log_prefix}{step_label} succeeded on retry attempt {attempt}/{attempts}"
                    logger.info(msg)
                    _send_external_log(brand_id, brand_name, msg)
                return result
            msg = f"{log_prefix}{step_label} returned empty/failed result on attempt {attempt}/{attempts}"
            logger.warning(msg)
            _send_external_log(brand_id, brand_name, msg)
        except Exception as exc:
            msg = f"{log_prefix}{step_label} raised error on attempt {attempt}/{attempts}: {exc}"
            logger.exception(msg)
            _send_external_log(brand_id, brand_name, msg)

        if attempt < attempts:
            retry_msg = f"{log_prefix}Retrying {step_label} (next attempt {attempt + 1}/{attempts})"
            logger.info(retry_msg)
            _send_external_log(brand_id, brand_name, retry_msg)

    return None


def run_pipeline(initial_data: Dict[str, Any]) -> bool:
    brand_id = str(initial_data.get("brand_id", "unknown"))
    brand_name = initial_data.get("brand_name", "Unknown Brand")
    job_id = str(initial_data.get("job_id", "")).strip()
    log_prefix = _prefix(job_id)

    start_msg = f"{log_prefix}Starting pipeline for {brand_name}"
    print(f"\n{start_msg}")
    _send_external_log(brand_id, brand_name, start_msg)

    try:
        step1_msg = f"{log_prefix}Step 1/6: Generating Shopify access token"
        print(step1_msg)
        _send_external_log(brand_id, brand_name, step1_msg)

        data = ensure_workflow_payloads(initial_data)
        data["state"] = STATE_INITIATED

        access_token = _run_with_retries(
            lambda: process_access_token(data),
            step_label="Step 1 access token generation",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        )
        if not access_token:
            err_msg = f"{log_prefix}Failed at Step 1: Access token generation failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        data = set_access_token(data, access_token)
        shop_currency = _run_with_retries(
            lambda: process_shop_currency(data),
            step_label="Step 1 shop currency fetch",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        )
        if not shop_currency:
            err_msg = f"{log_prefix}Failed at Step 1: Shop currency fetch failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        currency_msg = f"{log_prefix}Shop currency fetched: {shop_currency}"
        print(currency_msg)
        _send_external_log(brand_id, brand_name, currency_msg)
        data = set_shop_currency(data, shop_currency)
        data["state"] = STATE_ACCESS_TOKEN_GENERATED
        ok_msg = f"{log_prefix}Step 1 Complete: Access token generated and shop currency resolved to {shop_currency}"
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        step2_msg = f"{log_prefix}Step 2/6: Cloning database"
        print(step2_msg)
        _send_external_log(brand_id, brand_name, step2_msg)

        if not _run_with_retries(
            lambda: process_clone_db(data),
            step_label="Step 2 database clone",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        ):
            err_msg = f"{log_prefix}Failed at Step 2: Database cloning failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        data["state"] = STATE_DATABASE_CREATED
        ok_msg = f"{log_prefix}Step 2 Complete: Database cloned"
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        step3_msg = f"{log_prefix}Step 3/6: Generating speed token"
        print(step3_msg)
        _send_external_log(brand_id, brand_name, step3_msg)

        speed_key = _run_with_retries(
            lambda: process_speed_token(data),
            step_label="Step 3 speed token generation",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        )
        if not speed_key:
            err_msg = f"{log_prefix}Failed at Step 3: Speed token generation failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        data = set_speed_key(data, speed_key)
        data["state"] = STATE_SPEED_KEY_GENERATED
        ok_msg = f"{log_prefix}Step 3 Complete: Speed token generated"
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        step4_msg = f"{log_prefix}Step 4/6: Creating tenant record"
        print(step4_msg)
        _send_external_log(brand_id, brand_name, step4_msg)

        if not _run_with_retries(
            lambda: process_tenant_record(data),
            step_label="Step 4 tenant record creation",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        ):
            err_msg = f"{log_prefix}Failed at Step 4: Tenant record creation failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        data["state"] = STATE_TENANT_RECORD_CREATED
        ok_msg = f"{log_prefix}Step 4 Complete: Tenant record created"
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        step5_msg = f"{log_prefix}Step 5/6: Creating pipeline credentials"
        print(step5_msg)
        _send_external_log(brand_id, brand_name, step5_msg)

        if not _run_with_retries(
            lambda: process_pipelinecreds(data),
            step_label="Step 5 pipeline credentials creation",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        ):
            err_msg = f"{log_prefix}Failed at Step 5: Pipeline credentials creation failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        data["state"] = STATE_PIPELINE_CREDENTIALS_CREATED
        ok_msg = f"{log_prefix}Step 5 Complete: Pipeline credentials created"
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        step6_msg = f"{log_prefix}Step 6/6: Starting data backfill"
        print(step6_msg)
        _send_external_log(brand_id, brand_name, step6_msg)

        if not _run_with_retries(
            lambda: process_backfill(data),
            step_label="Step 6 data backfill",
            brand_id=brand_id,
            brand_name=brand_name,
            log_prefix=log_prefix,
        ):
            err_msg = f"{log_prefix}Failed at Step 6: Data backfill failed"
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return False

        data["state"] = STATE_DATA_BACKFILL_COMPLETED
        ok_msg = f"{log_prefix}Step 6 Complete: Data backfill finished"
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        time.sleep(2)
        data["state"] = STATE_TENANT_ADDED
        final_msg = f"{log_prefix}Pipeline successfully completed for {brand_name}"
        print(final_msg)
        _send_external_log(brand_id, brand_name, final_msg)
        logger.info(final_msg)
        return True

    except Exception as exc:
        err_msg = f"{log_prefix}Critical error in pipeline for {brand_name}: {exc}"
        print(err_msg)
        _send_external_log(brand_id, brand_name, err_msg)
        logger.exception(f"Pipeline failed for {brand_name}")
        return False
