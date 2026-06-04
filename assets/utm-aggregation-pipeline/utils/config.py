import os
from dotenv import load_dotenv

# Load .env first
load_dotenv(".env")

# Only load .env.local for TEST_MODE to avoid overriding production values.
_test_mode = os.getenv("TEST_MODE", "false").lower() == "true"
if _test_mode:
    load_dotenv(".env.local")

class Config:
    PORT = int(os.getenv("PORT", "8000"))
    HOST = os.getenv("HOST", "0.0.0.0")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    RETRY_ATTEMPTS = int(os.getenv("RETRY_ATTEMPTS", "3"))

    # Multi-brand and Test Mode
    TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
    GET_BRANDS_API = os.getenv("GET_BRANDS_API", "")
    PIPELINE_AUTH_HEADER = os.getenv("PIPELINE_AUTH_HEADER", "")
    PASSWORD_AES_KEY = os.getenv("PASSWORD_AES_KEY", "")
    ACTIVE_BRAND_IDS = os.getenv("ACTIVE_BRAND_IDS", "")

    # Backfill
    BACKFILL_MODE = os.getenv("BACKFILL_MODE", "false").lower() == "true"
    BACKFILL_START_IST = os.getenv("BACKFILL_START_IST", "")
    BACKFILL_END_IST = os.getenv("BACKFILL_END_IST", "")

    # Free Run (runs autonomously without Kafka)
    FREE_RUN = os.getenv("FREE_RUN", "false").lower() == "true"
    FREE_RUN_START_DATE = os.getenv("FREE_RUN_START_DATE", "")
    FREE_RUN_END_DATE = os.getenv("FREE_RUN_END_DATE", "")

    # Self Run (internal scheduler)
    SELF_RUN = os.getenv("SELF_RUN", "true").lower() == "true"
    PIPELINE_SCHEDULE = os.getenv("PIPELINE_SCHEDULE", "0 * * * *")  # Default: Hourly

    # Manager Endpoints
    MANAGER_STARTED_ENDPOINT = os.getenv("MANAGER_STARTED_ENDPOINT", "")
    MANAGER_COMPLETED_ENDPOINT = os.getenv("MANAGER_COMPLETED_ENDPOINT", "")
    MANAGER_FAILED_ENDPOINT = os.getenv("MANAGER_FAILED_ENDPOINT", "")
    PIPELINE_ID = os.getenv("PIPELINE_ID", "")
