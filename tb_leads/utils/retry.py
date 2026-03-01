from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

T = TypeVar("T")


@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay_s: float = 0.35
    max_delay_s: float = 4.0
    jitter_s: float = 0.2


def exponential_delay(attempt: int, policy: RetryPolicy) -> float:
    delay = min(policy.max_delay_s, policy.base_delay_s * (2 ** max(0, attempt - 1)))
    jitter = random.uniform(0, policy.jitter_s) if policy.jitter_s > 0 else 0.0
    return delay + jitter


def retry_call(
    fn: Callable[[], T],
    should_retry: Callable[[Exception], bool],
    policy: RetryPolicy,
) -> T:
    last_exc: Exception | None = None
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt >= policy.max_attempts or not should_retry(exc):
                raise
            time.sleep(exponential_delay(attempt, policy))

    # Defensive fallback
    if last_exc:
        raise last_exc
    raise RuntimeError("retry_call failed without exception")
