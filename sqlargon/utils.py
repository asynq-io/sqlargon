from datetime import datetime, timezone
from hashlib import md5
from typing import Any

from pydantic_core import to_jsonable_python

try:
    import orjson

    def json_dumps(data: Any) -> str:
        return orjson.dumps(data, default=to_jsonable_python).decode("utf-8")

    def json_loads(data: str) -> Any:
        return orjson.loads(data)

except ImportError:
    import json

    def json_dumps(data: Any) -> str:
        return json.dumps(data, default=to_jsonable_python)

    def json_loads(data: str) -> Any:
        return json.loads(data)


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


INT64_SIZE = 2**63 - 1


def key_to_int(key: str) -> int:
    return (
        int(md5(key.encode("utf-8"), usedforsecurity=False).hexdigest(), 16)
        % INT64_SIZE
    )
