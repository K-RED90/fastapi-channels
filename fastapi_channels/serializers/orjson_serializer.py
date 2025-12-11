from __future__ import annotations

from typing import Any

try:
    import orjson  # type: ignore
except ImportError:
    orjson = None

from fastapi_channels.serializers import BaseSerializer


class ORJSONSerializer(BaseSerializer):
    """Serializer backed by orjson for speed."""

    binary = False

    def dumps(self, data: Any) -> bytes:
        if orjson is None:
            raise ImportError("orjson is not installed")
        return orjson.dumps(data)

    def loads(self, data: bytes | str) -> Any:
        if orjson is None:
            raise ImportError("orjson is not installed")
        return orjson.loads(data)
