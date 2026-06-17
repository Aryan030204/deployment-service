import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from redis import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError

from shared.utils.logger import get_logger


logger = get_logger("deployment-service.queue")

QUEUE_KEY = "deployment-service:onboarding:queue"
JOB_KEY_PREFIX = "deployment-service:onboarding:job"
ACTIVE_KEY_PREFIX = "deployment-service:onboarding:active"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class DuplicateJobError(Exception):
    def __init__(self, brand_id: str, active_job_id: Optional[str]):
        self.brand_id = brand_id
        self.active_job_id = active_job_id
        super().__init__(f"Brand {brand_id} already has an active onboarding job")


class OnboardingQueue:
    def __init__(self, redis_uri: Optional[str] = None):
        self.redis_uri = redis_uri or os.getenv("REDIS_URI")
        if not self.redis_uri:
            raise RuntimeError("REDIS_URI environment variable is required")
        self.client = Redis.from_url(
            self.redis_uri,
            decode_responses=True,
            socket_connect_timeout=5,
            health_check_interval=30,
        )
        self.blocking_client = Redis.from_url(
            self.redis_uri,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=30,
            health_check_interval=30,
        )

    def _job_key(self, job_id: str) -> str:
        return f"{JOB_KEY_PREFIX}:{job_id}"

    def _active_key(self, brand_id: str) -> str:
        return f"{ACTIVE_KEY_PREFIX}:{brand_id}"

    def enqueue(self, payload: Dict[str, Any]) -> str:
        brand_id = str(payload.get("brand_id") or "").strip()
        if not brand_id:
            raise ValueError("brand_id is required to enqueue an onboarding job")

        active_key = self._active_key(brand_id)
        existing_job_id = self.client.get(active_key)
        if existing_job_id:
            raise DuplicateJobError(brand_id, existing_job_id)

        job_id = uuid4().hex
        queued_at = _utc_now_iso()
        payload_with_job = dict(payload)
        payload_with_job["job_id"] = job_id

        if not self.client.set(active_key, job_id, nx=True):
            raise DuplicateJobError(brand_id, self.client.get(active_key))

        job_key = self._job_key(job_id)
        try:
            self.client.hset(
                job_key,
                mapping={
                    "job_id": job_id,
                    "brand_id": brand_id,
                    "brand_name": str(payload.get("brand_name") or ""),
                    "status": "queued",
                    "queued_at": queued_at,
                    "payload": json.dumps(payload_with_job),
                },
            )
            self.client.rpush(QUEUE_KEY, json.dumps(payload_with_job))
            logger.info(f"Queued onboarding job {job_id} for brand_id={brand_id}")
            return job_id
        except Exception:
            current_job_id = self.client.get(active_key)
            if current_job_id == job_id:
                self.client.delete(active_key)
            self.client.delete(job_key)
            raise

    def pop(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        try:
            item = self.blocking_client.blpop(QUEUE_KEY, timeout=timeout)
        except RedisTimeoutError:
            logger.warning("Redis BLPOP socket timed out while waiting for onboarding jobs; continuing")
            return None
        if not item:
            return None
        _, raw_payload = item
        return json.loads(raw_payload)

    def mark_running(self, job_id: str) -> None:
        self.client.hset(
            self._job_key(job_id),
            mapping={
                "status": "running",
                "started_at": _utc_now_iso(),
            },
        )

    def mark_finished(self, job_id: str, status: str, error: str = "") -> None:
        job_key = self._job_key(job_id)
        job_meta = self.client.hgetall(job_key)
        brand_id = job_meta.get("brand_id")
        pipe = self.client.pipeline()
        pipe.hset(
            job_key,
            mapping={
                "status": status,
                "finished_at": _utc_now_iso(),
                "error": error,
            },
        )
        if brand_id:
            active_key = self._active_key(brand_id)
            active_job_id = self.client.get(active_key)
            if active_job_id == job_id:
                pipe.delete(active_key)
        pipe.execute()
