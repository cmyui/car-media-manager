import time
from collections import deque
from dataclasses import dataclass
from dataclasses import field
from typing import Callable

ProgressCallback = Callable[[int], None]


@dataclass
class SpeedTracker:
    _samples: deque[tuple[float, int]] = field(
        default_factory=lambda: deque(maxlen=500),
    )

    def record(self, bytes_transferred: int) -> None:
        self._samples.append((time.monotonic(), bytes_transferred))

    def bytes_per_second(self, window: float = 30.0) -> float:
        now = time.monotonic()
        cutoff = now - window
        relevant = [(t, b) for t, b in self._samples if t >= cutoff]
        if len(relevant) < 2:
            return 0.0
        total_bytes = sum(b for _, b in relevant)
        elapsed = now - relevant[0][0]
        return total_bytes / elapsed if elapsed > 0 else 0.0

    def eta_seconds(self, remaining_bytes: int) -> float | None:
        speed = self.bytes_per_second()
        if speed <= 0:
            return None
        return remaining_bytes / speed


ingest_tracker = SpeedTracker()
upload_tracker = SpeedTracker()
