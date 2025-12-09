from __future__ import annotations

import json
from typing import Any

from core.serializers.base import BaseSerializer


class JSONSerializer(BaseSerializer):
    """Serializer using the standard library json module."""

    binary = False

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def dumps(self, data: Any) -> str:
        return json.dumps(data)

    def loads(self, data: bytes | str) -> Any:
        if isinstance(data, bytes):
            data = data.decode(self.encoding)
        return json.loads(data)
