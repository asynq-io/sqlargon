from typing import Any

from pydantic.json import pydantic_encoder

try:
    import orjson

    def json_dumps(data: Any) -> str:
        return orjson.dumps(data, default=pydantic_encoder).decode("utf-8")

    def json_loads(data: str) -> Any:
        return orjson.loads(data)

except ImportError:
    import json

    def json_dumps(data: Any) -> str:
        return json.dumps(data, default=pydantic_encoder)

    def json_loads(data: str) -> Any:
        return json.loads(data)
