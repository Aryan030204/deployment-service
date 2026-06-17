import os
import threading
import time

from services.onboarding_queue import OnboardingQueue
from services.orchestrator import run_pipeline
from shared.utils.logger import get_logger


logger = get_logger("deployment-service.worker")


def _worker_loop(queue: OnboardingQueue, stop_event: threading.Event, worker_id: int) -> None:
    logger.info(f"Starting onboarding worker loop {worker_id}")
    while not stop_event.is_set():
        try:
            job = queue.pop(timeout=5)
            if not job:
                continue

            job_id = job.get("job_id", "")
            brand_id = str(job.get("brand_id") or "")
            logger.info(f"[job_id={job_id}][brand_id={brand_id}] Picked onboarding job")
            queue.mark_running(job_id)

            ok = run_pipeline(job)
            queue.mark_finished(job_id, "succeeded" if ok else "failed", "" if ok else "Pipeline returned failed status")
        except Exception as exc:
            job_id = ""
            brand_id = ""
            if "job" in locals() and isinstance(job, dict):
                job_id = job.get("job_id", "")
                brand_id = str(job.get("brand_id") or "")
            logger.exception(f"[job_id={job_id}][brand_id={brand_id}] Worker loop error: {exc}")
            if job_id:
                queue.mark_finished(job_id, "failed", error=str(exc))
            time.sleep(1)


def run_worker() -> None:
    redis_uri = os.getenv("REDIS_URI")
    if not redis_uri:
        raise RuntimeError("REDIS_URI environment variable is required")

    parallel_workers = int(os.getenv("PARALLEL_WORKERS", "1"))
    if parallel_workers < 1:
        raise RuntimeError("PARALLEL_WORKERS must be >= 1")

    queue = OnboardingQueue(redis_uri)
    stop_event = threading.Event()
    threads = []

    logger.info(f"Starting onboarding worker service with {parallel_workers} parallel workers")
    for worker_id in range(1, parallel_workers + 1):
        thread = threading.Thread(
            target=_worker_loop,
            args=(queue, stop_event, worker_id),
            daemon=True,
            name=f"onboarding-worker-{worker_id}",
        )
        thread.start()
        threads.append(thread)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Stopping onboarding worker service")
        stop_event.set()
        for thread in threads:
            thread.join(timeout=10)
