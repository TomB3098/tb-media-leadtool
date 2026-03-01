from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class RunLogger:
    def __init__(self, run_id: str, logs_dir: str = "logs"):
        self.run_id = run_id
        self.logs_dir = Path(logs_dir)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.path = self.logs_dir / f"run-{run_id}.jsonl"

    def event(self, stage: str, event: str, payload: dict[str, Any] | None = None) -> None:
        row = {
            "ts": datetime.now(UTC).isoformat(),
            "run_id": self.run_id,
            "stage": stage,
            "event": event,
            "payload": payload or {},
        }
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
