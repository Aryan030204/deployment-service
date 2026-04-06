import os
import json
import time
import requests
from typing import Any, Dict

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
    set_speed_key,
)

# Import script process functions
from scripts.generate_access_token import process_event as process_access_token
from scripts.clone_database import process_event as process_clone_db
from scripts.generate_speed_token import process_event as process_speed_token
from scripts.create_tenant_record import process_event as process_tenant_record
from scripts.create_pipelinecreds import process_event as process_pipelinecreds
from scripts.backfill_data import process_event as process_backfill

logger = get_logger("deployment-service.orchestrator")

def _send_external_log(brand_id: str, brand_name: str, message: str) -> None:
    """Helper to send logs to external endpoints specified in LOGS_URL."""
    try:
        logs_url_str = os.getenv("LOGS_URL", "[]")
        urls = json.loads(logs_url_str)
        auth_header = os.getenv("PIPELINE_AUTH_HEADER", "")
        
        if not isinstance(urls, list):
            return

        payload = {
            "brand_id": brand_id,
            "brand_name": brand_name,
            "log": message
        }
        headers = {
            "x-pipeline-key": auth_header,
            "Content-Type": "application/json"
        }

        for url in urls:
            try:
                requests.post(url, json=payload, headers=headers, timeout=5)
            except Exception as e:
                logger.error(f"Failed to send log to {url}: {e}")
    except Exception as e:
        logger.error(f"Error in _send_external_log: {e}")

def run_pipeline(initial_data: Dict[str, Any]) -> None:
    """
    Main orchestrator that runs the entire brand onboarding pipeline in sequence.
    Prints status messages to console AND sends to external logging URLs.
    """
    brand_id = str(initial_data.get("brand_id", "unknown"))
    brand_name = initial_data.get("brand_name", "Unknown Brand")
    
    start_msg = f"🚀 [ORCHESTRATOR] Starting pipeline for {brand_name}"
    print(f"\n{start_msg}")
    _send_external_log(brand_id, brand_name, start_msg)
    
    try:
        # Step 1: Shopify Access Token
        step1_msg = "📦 [ORCHESTRATOR] Step 1/6: Generating Shopify Access Token..."
        print(step1_msg)
        _send_external_log(brand_id, brand_name, step1_msg)
        
        data = ensure_workflow_payloads(initial_data)
        data["state"] = STATE_INITIATED
        
        access_token = process_access_token(data)
        if not access_token:
            err_msg = "❌ [ORCHESTRATOR] Failed at Step 1: Access token generation failed."
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return
        
        data = set_access_token(data, access_token)
        data["state"] = STATE_ACCESS_TOKEN_GENERATED
        ok_msg = "✅ [ORCHESTRATOR] Step 1 Complete: Access token generated."
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        # Step 2: Clone Database
        step2_msg = "📦 [ORCHESTRATOR] Step 2/6: Cloning Database..."
        print(step2_msg)
        _send_external_log(brand_id, brand_name, step2_msg)
        
        if not process_clone_db(data):
            err_msg = "❌ [ORCHESTRATOR] Failed at Step 2: Database cloning failed."
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return
        
        data["state"] = STATE_DATABASE_CREATED
        ok_msg = "✅ [ORCHESTRATOR] Step 2 Complete: Database cloned."
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        # Step 3: Generate Speed Token
        step3_msg = "📦 [ORCHESTRATOR] Step 3/6: Generating Speed Token..."
        print(step3_msg)
        _send_external_log(brand_id, brand_name, step3_msg)
        
        speed_key = process_speed_token(data)
        if not speed_key:
            err_msg = "❌ [ORCHESTRATOR] Failed at Step 3: Speed token generation failed."
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return
        
        data = set_speed_key(data, speed_key)
        data["state"] = STATE_SPEED_KEY_GENERATED
        ok_msg = "✅ [ORCHESTRATOR] Step 3 Complete: Speed token generated."
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        # Step 4: Create Tenant Record
        step4_msg = "📦 [ORCHESTRATOR] Step 4/6: Creating Tenant Record..."
        print(step4_msg)
        _send_external_log(brand_id, brand_name, step4_msg)
        
        if not process_tenant_record(data):
            err_msg = "❌ [ORCHESTRATOR] Failed at Step 4: Tenant record creation failed."
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return
        
        data["state"] = STATE_TENANT_RECORD_CREATED
        ok_msg = "✅ [ORCHESTRATOR] Step 4 Complete: Tenant record created."
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        # Step 5: Create Pipeline Credentials
        step5_msg = "📦 [ORCHESTRATOR] Step 5/6: Creating Pipeline Credentials..."
        print(step5_msg)
        _send_external_log(brand_id, brand_name, step5_msg)
        
        if not process_pipelinecreds(data):
            err_msg = "❌ [ORCHESTRATOR] Failed at Step 5: Pipeline credentials creation failed."
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return
        
        data["state"] = STATE_PIPELINE_CREDENTIALS_CREATED
        ok_msg = "✅ [ORCHESTRATOR] Step 5 Complete: Pipeline credentials created."
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        # Step 6: Data Backfill
        step6_msg = "📦 [ORCHESTRATOR] Step 6/6: Starting Data Backfill..."
        print(step6_msg)
        _send_external_log(brand_id, brand_name, step6_msg)
        
        if not process_backfill(data):
            err_msg = "❌ [ORCHESTRATOR] Failed at Step 6: Data backfill failed."
            print(err_msg)
            _send_external_log(brand_id, brand_name, err_msg)
            return
        
        data["state"] = STATE_DATA_BACKFILL_COMPLETED
        ok_msg = "✅ [ORCHESTRATOR] Step 6 Complete: Data backfill finished."
        print(ok_msg)
        _send_external_log(brand_id, brand_name, ok_msg)

        # Final State
        time.sleep(2) 
        data["state"] = STATE_TENANT_ADDED
        final_msg = f"🎉 [ORCHESTRATOR] Pipeline Successfully Completed for {brand_name}!"
        print(final_msg)
        _send_external_log(brand_id, brand_name, final_msg)
        logger.info(f"Pipeline successfully completed for {brand_name}")

    except Exception as e:
        err_msg = f"🔥 [ORCHESTRATOR] Critical Error in pipeline for {brand_name}: {e}"
        print(err_msg)
        _send_external_log(brand_id, brand_name, err_msg)
        logger.exception(f"Pipeline failed for {brand_name}")
