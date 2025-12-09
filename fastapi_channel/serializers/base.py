from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
    """Abstract serializer interface."""

    # When True, consumers should avoid Unicode decoding (e.g., Redis decode_responses)
    binary: bool = False

    @abstractmethod
    def dumps(self, data: Any) -> bytes | str:
        """Serialize Python data into bytes or string."""
        raise NotImplementedError

    @abstractmethod
    def loads(self, data: bytes | str) -> Any:
        """Deserialize data produced by dumps back into Python objects."""
        raise NotImplementedError
