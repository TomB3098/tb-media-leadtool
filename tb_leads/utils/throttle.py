from __future__ import annotations

import threading
import time
from collections import deque


class RateLimiter:
    """Simple max-requests-per-minute limiter.

    Uses a rolling 60s window and blocks when budget is exhausted.
    """

    def __init__(self, max_requests_per_minute: int = 60):
        self.max_requests_per_minute = max(1, int(max_requests_per_minute))
        self._events = deque()
        self._lock = threading.Lock()

    def acquire(self) -> float:
        """Acquire one request slot.

        Returns the waited time in seconds.
        """
        waited = 0.0
        while True:
            now = time.monotonic()
            with self._lock:
                while self._events and now - self._events[0] >= 60:
                    self._events.popleft()

                if len(self._events) < self.max_requests_per_minute:
                    self._events.append(now)
                    return waited

                earliest = self._events[0]
                sleep_for = max(0.01, 60 - (now - earliest))

            time.sleep(sleep_for)
            waited += sleep_for
