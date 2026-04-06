import time
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

def run_pipeline(initial_data: Dict[str, Any]) -> None:
    """
    Main orchestrator that runs the entire brand onboarding pipeline in sequence.
    Prints status messages to the console for tracking.
    """
    brand_name = initial_data.get("brand_name", "Unknown Brand")
    print(f"\n🚀 [ORCHESTRATOR] Starting pipeline for {brand_name}")
    
    try:
        # Step 1: Shopify Access Token
        print("📦 [ORCHESTRATOR] Step 1/6: Generating Shopify Access Token...")
        data = ensure_workflow_payloads(initial_data)
        data["state"] = STATE_INITIATED
        
        access_token = process_access_token(data)
        if not access_token:
            print("❌ [ORCHESTRATOR] Failed at Step 1: Access token generation failed.")
            return
        
        data = set_access_token(data, access_token)
        data["state"] = STATE_ACCESS_TOKEN_GENERATED
        print("✅ [ORCHESTRATOR] Step 1 Complete: Access token generated.")

        # Step 2: Clone Database
        print("📦 [ORCHESTRATOR] Step 2/6: Cloning Database...")
        if not process_clone_db(data):
            print("❌ [ORCHESTRATOR] Failed at Step 2: Database cloning failed.")
            return
        
        data["state"] = STATE_DATABASE_CREATED
        print("✅ [ORCHESTRATOR] Step 2 Complete: Database cloned.")

        # Step 3: Generate Speed Token
        print("📦 [ORCHESTRATOR] Step 3/6: Generating Speed Token...")
        speed_key = process_speed_token(data)
        if not speed_key:
            print("❌ [ORCHESTRATOR] Failed at Step 3: Speed token generation failed.")
            return
        
        data = set_speed_key(data, speed_key)
        data["state"] = STATE_SPEED_KEY_GENERATED
        print("✅ [ORCHESTRATOR] Step 3 Complete: Speed token generated.")

        # Step 4: Create Tenant Record
        print("📦 [ORCHESTRATOR] Step 4/6: Creating Tenant Record...")
        if not process_tenant_record(data):
            print("❌ [ORCHESTRATOR] Failed at Step 4: Tenant record creation failed.")
            return
        
        data["state"] = STATE_TENANT_RECORD_CREATED
        print("✅ [ORCHESTRATOR] Step 4 Complete: Tenant record created.")

        # Step 5: Create Pipeline Credentials
        print("📦 [ORCHESTRATOR] Step 5/6: Creating Pipeline Credentials...")
        if not process_pipelinecreds(data):
            print("❌ [ORCHESTRATOR] Failed at Step 5: Pipeline credentials creation failed.")
            return
        
        data["state"] = STATE_PIPELINE_CREDENTIALS_CREATED
        print("✅ [ORCHESTRATOR] Step 5 Complete: Pipeline credentials created.")

        # Step 6: Data Backfill
        print("📦 [ORCHESTRATOR] Step 6/6: Starting Data Backfill...")
        if not process_backfill(data):
            print("❌ [ORCHESTRATOR] Failed at Step 6: Data backfill failed.")
            return
        
        data["state"] = STATE_DATA_BACKFILL_COMPLETED
        print("✅ [ORCHESTRATOR] Step 6 Complete: Data backfill finished.")

        # Final State
        time.sleep(2) 
        data["state"] = STATE_TENANT_ADDED
        print(f"🎉 [ORCHESTRATOR] Pipeline Successfully Completed for {brand_name}!")
        logger.info(f"Pipeline successfully completed for {brand_name}")

    except Exception as e:
        print(f"🔥 [ORCHESTRATOR] Critical Error in pipeline for {brand_name}: {e}")
        logger.exception(f"Pipeline failed for {brand_name}")
