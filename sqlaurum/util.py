from typing import Any

import orjson
from pydantic.json import pydantic_encoder


def json_dumps(data: Any) -> str:
    return orjson.dumps(data, default=pydantic_encoder).decode("utf-8")


def json_loads(data: str) -> Any:
    return orjson.loads(data)
