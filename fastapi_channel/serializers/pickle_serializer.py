from __future__ import annotations

import pickle
from typing import Any

from core.serializers.base import BaseSerializer


class PickleSerializer(BaseSerializer):
    """Serializer using pickle for arbitrary Python objects."""

    binary = True

    def __init__(self, protocol: int | None = None, encoding: str = "latin-1"):
        self.protocol = protocol
        self.encoding = encoding

    def dumps(self, data: Any) -> bytes:
        return pickle.dumps(data, protocol=self.protocol)

    def loads(self, data: bytes | str) -> Any:
        if isinstance(data, str):
            # Latin-1 preserves byte values 1:1 so round-trips decoded strings safely
            data = data.encode(self.encoding)
        return pickle.loads(data)
