import time
from functools import wraps
from app.utils.config import Config
from app.utils.logger import get_logger

logger = get_logger(__name__)

def retry_db_operation(max_attempts=Config.RETRY_ATTEMPTS, delay=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    logger.warning(f"Attempt {attempts}/{max_attempts} failed for {func.__name__}: {e}")
                    if attempts >= max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts.")
                        raise e
                    time.sleep(delay * attempts)  # exponential backoff
        return wrapper
    return decorator
