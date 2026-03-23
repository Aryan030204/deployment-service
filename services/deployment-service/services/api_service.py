import os
import uvicorn
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, status
from pydantic import BaseModel

from kafka_client.producer import publish_brand_event
from shared.utils.encryption import encrypt
from shared.utils.logger import get_logger

logger = get_logger("deployment-service.api")

app = FastAPI(title="Deployment Service Webhook API")

class BrandEventRequest(BaseModel):
    brand_id: str
    brand_name: str
    client_id: str
    client_secret: str
    shop_name: str
    code: Optional[str] = None
    brand_url: Optional[str] = None
    website_url: Optional[str] = None

@app.post("/push-brand-event", status_code=status.HTTP_200_OK)
async def push_brand_event(
    request: BrandEventRequest,
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
        
        # 3. Create Kafka payload
        payload = {
            "brand_id": request.brand_id,
            "brand_name": request.brand_name,
            "client_id": encrypted_client_id,
            "client_secret": encrypted_client_secret,
            "shop_name": request.shop_name
        }
        if request.code:
            payload["code"] = request.code
            logger.info(f"📝 Appending code to payload for Kafka: {request.code[:8]}...")
        else:
            logger.info("ℹ️ No code parameter found in API request payload")
            
        if request.brand_url:
            payload["brand_url"] = request.brand_url
            logger.info(f"📝 Appending brand_url to payload: {request.brand_url}")
        
        # 4. Publish to Kafka
        publish_brand_event(payload)
        
        logger.info(f"Successfully pushed brand event for {request.brand_name} ({request.brand_id})")
        return {"status": "success", "message": "Brand event pushed to Kafka"}

    except Exception as e:
        logger.exception("Failed to process brand event")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal error: {str(e)}"
        )

def run() -> None:
    """Run the FastAPI application via uvicorn."""
    logger.info("Starting Webhook API server on port 8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
