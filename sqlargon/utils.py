from datetime import datetime, timezone
from typing import Any

import orjson
from pydantic_core import to_jsonable_python


def json_dumps(data: Any) -> str:
    return orjson.dumps(data, default=to_jsonable_python).decode("utf-8")


def json_loads(data: str) -> Any:
    return orjson.loads(data)


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
