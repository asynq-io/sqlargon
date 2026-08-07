from datetime import datetime

from croniter import croniter


def validate_schedule(schedule: str) -> str:
    """Return ``schedule`` unchanged, raising ``ValueError`` if it is not a
    valid cron expression."""
    if not croniter.is_valid(schedule):
        msg = f"Invalid cron expression: {schedule!r}"
        raise ValueError(msg)
    return schedule


def next_run_time(schedule: str, after: datetime) -> datetime:
    """Return the first run time of ``schedule`` strictly after ``after``."""
    return croniter(schedule, after).get_next(datetime)
