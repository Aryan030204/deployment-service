from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
ONBOARDING_BACKFILL_LOOKBACK_DAYS = 60


def now_ist() -> datetime:
    return datetime.now(IST)


def get_onboarding_backfill_window() -> Tuple[str, str]:
    yesterday = now_ist() - timedelta(days=1)
    start_dt = yesterday - timedelta(days=ONBOARDING_BACKFILL_LOOKBACK_DAYS)
    return start_dt.strftime("%Y-%m-%d"), yesterday.strftime("%Y-%m-%d")


def build_inclusive_date_range(start_ymd: str, end_ymd: str) -> List[str]:
    start_date = datetime.strptime(start_ymd, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_ymd, "%Y-%m-%d").date()
    if start_date > end_date:
        raise ValueError(f"start date {start_ymd} is after end date {end_ymd}")

    dates: List[str] = []
    current = start_date
    while current <= end_date:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates
