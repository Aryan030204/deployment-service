import logging
import sys
from collections import deque
from app.utils.config import Config

_recent_logs = deque(maxlen=200)


class RecentLogHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = self.format(record)
        except Exception:
            message = record.getMessage()
        _recent_logs.append(message)

def get_logger(name: str):
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        logger.setLevel(Config.LOG_LEVEL)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(Config.LOG_LEVEL)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        recent_handler = RecentLogHandler()
        recent_handler.setLevel(Config.LOG_LEVEL)
        recent_handler.setFormatter(formatter)
        logger.addHandler(recent_handler)
    return logger


def get_recent_logs(count: int):
    if count <= 0:
        return []
    return list(_recent_logs)[-count:]
