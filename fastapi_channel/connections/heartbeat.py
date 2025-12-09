import time
from dataclasses import dataclass, field


@dataclass
class HeartbeatMonitor:
    interval: int = 30
    timeout: int = 60
    last_ping: float = field(default_factory=time.time)
    last_pong: float = field(default_factory=time.time)
    missed_pings: int = 0
    max_missed_pings: int = 3

    def should_ping(self) -> bool:
        return time.time() - self.last_ping >= self.interval

    def record_ping(self) -> None:
        self.last_ping = time.time()

    def record_pong(self) -> None:
        self.last_pong = time.time()
        self.missed_pings = 0

    def is_alive(self) -> bool:
        if self.missed_pings >= self.max_missed_pings:
            return False
        return time.time() - self.last_pong < self.timeout

    def increment_missed(self) -> None:
        self.missed_pings += 1
