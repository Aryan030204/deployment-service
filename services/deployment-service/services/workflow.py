import os
from copy import deepcopy


STATE_INITIATED = "initiated"
STATE_ACCESS_TOKEN_GENERATED = "access_token_generated"
STATE_DATABASE_CREATED = "database_created"
STATE_SPEED_KEY_GENERATED = "speed_key_generated"
STATE_TENANT_RECORD_CREATED = "tenant_record_created"
STATE_PIPELINE_CREDENTIALS_CREATED = "pipeline_credentials_created"
STATE_DATA_BACKFILL_COMPLETED = "data_backfill_completed"
STATE_TENANT_ADDED = "tenant_added"


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


def ensure_workflow_payloads(event: dict) -> dict:
    data = deepcopy(event)

    brand_id = data.get("brand_id")
    brand_name = data.get("brand_name")
    shop_name = data.get("shop_name")
    brand_tag = str(brand_name).lower() if brand_name else ""
    brand_name_upper = str(brand_name).upper() if brand_name else ""

    tenant_payload = data.get("tenant_payload") or {
        "brand_id": brand_name_upper,
        "shard_id": brand_tag,
        "rds_proxy_endpoint": _env("READ_REPLICA"),
        "database": brand_name_upper,
        "db_host": _env("DB_HOST"),
        "brand_num": brand_id,
        "shop_name": shop_name,
        "api_version": _env("API_VERSION"),
        "access_token": "",
        "session_url": "",
        "status": "active",
        "port": 3306,
        "user": "admin",
        "password": _env("DB_PASSWORD"),
        "app_id_mapping": data.get("app_id_mapping") or "",
    }

    DEFAULT_APP_ID_MAPPING = """{
    "238372913153": "GoKwik",
    "580111": "Online Store",
    "1354745": "Draft Order"
}"""
    app_id_mapping_val = data.get("app_id_mapping") or DEFAULT_APP_ID_MAPPING

    db_user = "admin"
    db_host = _env("DB_HOST")
    db_port = int(_env("DB_PORT", "3306"))
    db_password = _env("DB_PASSWORD")
    pipelinecreds_payload = data.get("pipelinecreds_payload") or {
        "brand_id": brand_id,
        "brand_name": brand_name_upper,
        "db_host": db_host,
        "port": db_port,
        "db_password": db_password,
        "db_user": db_user,
        "access_token": "",
        "api_version": _env("API_VERSION"),
        "app_id_mapping": app_id_mapping_val,
        "brand_tag": brand_tag,
        "db_database": brand_name_upper,
        "my_sql_url": f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{brand_name_upper}?charset=utf8mb4" + ("&ssl=true" if db_host.lower() not in ["localhost", "127.0.0.1"] else ""),
        "shop_name": shop_name,
        "speed_key": "",
    }

    data["brand_tag"] = brand_tag
    data["tenant_payload"] = tenant_payload
    data["pipelinecreds_payload"] = pipelinecreds_payload
    return data


def set_access_token(data: dict, access_token: str) -> dict:
    updated = ensure_workflow_payloads(data)
    updated["tenant_payload"]["access_token"] = access_token
    updated["pipelinecreds_payload"]["access_token"] = access_token
    return updated


def set_speed_key(data: dict, speed_key: str) -> dict:
    updated = ensure_workflow_payloads(data)
    updated["pipelinecreds_payload"]["speed_key"] = speed_key
    if "speed_key" in updated["tenant_payload"]:
        updated["tenant_payload"]["speed_key"] = speed_key
    return updated

