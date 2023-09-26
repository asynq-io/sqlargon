from typing import Any

try:
    import orjson

    def json_dumps(data: Any) -> str:
        return orjson.dumps(data).decode("utf-8")

    def json_loads(data: str) -> Any:
        return orjson.loads(data)

except ImportError:
    import json

    def json_dumps(data: Any) -> str:
        return json.dumps(data)

    def json_loads(data: str) -> Any:
        return json.loads(data)
