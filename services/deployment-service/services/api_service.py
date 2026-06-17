import os
import json
import uvicorn
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel

from services.onboarding_queue import DuplicateJobError, OnboardingQueue
from shared.utils.encryption import encrypt
from shared.utils.logger import get_logger
from scripts.delete_tenant_logic import delete_tenant_logic

logger = get_logger("deployment-service.api")

app = FastAPI(title="Deployment Service Webhook API")

class OnboardRequest(BaseModel):
    brand_id: str
    brand_name: str
    client_id: str
    client_secret: str
    shop_name: str
    code: Optional[str] = None
    brand_url: Optional[str] = None
    website_url: Optional[str] = None
    app_id_mapping: Optional[dict] = None

@app.post("/tenant/onboard", status_code=status.HTTP_202_ACCEPTED)
async def onboard_tenant(
    request: OnboardRequest,
    x_pipeline_key: str = Header(None, alias="x-pipeline-key")
):
    # 1. Validate Header
    required_key = os.getenv("PIPELINE_AUTH_HEADER")
    if not required_key:
        logger.error("PIPELINE_AUTH_HEADER environment variable is not set")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server configuration error: PIPELINE_AUTH_HEADER is missing"
        )
        
    if x_pipeline_key != required_key:
        logger.warning("Unauthorized access attempt with invalid pipeline key")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid x-pipeline-key"
        )

    try:
        # 2. Encrypt sensitive fields
        encrypted_client_id = encrypt(request.client_id)
        encrypted_client_secret = encrypt(request.client_secret)
        
        # 3. Create payload
        payload = {
            "brand_id": request.brand_id,
            "brand_name": request.brand_name,
            "client_id": encrypted_client_id,
            "client_secret": encrypted_client_secret,
            "shop_name": request.shop_name
        }
        if request.code:
            payload["code"] = request.code
        if request.brand_url:
            payload["brand_url"] = request.brand_url
        if request.website_url:
            payload["website_url"] = request.website_url
        if request.app_id_mapping:
            payload["app_id_mapping"] = json.dumps(request.app_id_mapping)
        
        # 4. Enqueue onboarding job
        queue = OnboardingQueue()
        job_id = queue.enqueue(payload)
        logger.info(f"Queued onboarding pipeline for {request.brand_name} with job_id={job_id}")

        return {
            "status": "queued",
            "job_id": job_id,
            "brand_id": request.brand_id,
            "message": "Onboarding pipeline queued successfully",
        }

    except DuplicateJobError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "status": "conflict",
                "brand_id": request.brand_id,
                "active_job_id": e.active_job_id,
                "message": "An onboarding job is already queued or running for this brand",
            },
        )

    except Exception as e:
        logger.exception("Failed to trigger onboarding pipeline")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error: {str(e)}"
        )

@app.delete("/tenant/{brand_id}", status_code=status.HTTP_200_OK)
async def delete_tenant_endpoint(
    brand_id: int,
    x_pipeline_key: str = Header(None, alias="x-pipeline-key")
):
    # (Existing delete logic preserved)
    required_key = os.getenv("PIPELINE_AUTH_HEADER")
    if not required_key:
        raise HTTPException(status_code=500, detail="PIPELINE_AUTH_HEADER missing")
    if x_pipeline_key != required_key:
        raise HTTPException(status_code=401, detail="Invalid key")

    try:
        success, message = delete_tenant_logic(brand_id)
        if not success:
            raise HTTPException(status_code=500, detail=message)
        return {"status": "success", "message": f"Tenant {brand_id} deleted."}
    except Exception as e:
        logger.exception(f"Internal error during deletion of tenant {brand_id}")
        raise HTTPException(status_code=500, detail=str(e))

def run() -> None:
    """Run the FastAPI application via uvicorn."""
    port = int(os.getenv("PORT", 8000))
    logger.info(f"Starting Webhook API server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
