from __future__ import annotations

import re
from typing import Any

PRIVATE_EMAIL_PATTERNS = [
    r"@gmail\.com$",
    r"@gmx\.de$",
    r"@hotmail\.com$",
    r"@outlook\.com$",
]


def validate_source(source_name: str, allowed_sources: list[str]) -> tuple[bool, str | None]:
    if source_name not in allowed_sources:
        return False, f"Source '{source_name}' ist nicht auf der Allowlist."
    return True, None


def detect_private_email(text: str) -> bool:
    candidates = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text or "")
    for mail in candidates:
        for pattern in PRIVATE_EMAIL_PATTERNS:
            if re.search(pattern, mail, re.IGNORECASE):
                return True
    return False


def basic_record_checks(record: dict[str, Any], allowed_sources: list[str]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    source = record.get("source_primary", "unknown")

    ok, msg = validate_source(source, allowed_sources)
    if not ok:
        events.append(
            {
                "severity": "error",
                "rule_id": "source_allowlist",
                "message": msg,
                "context": {"source": source},
            }
        )

    if detect_private_email(str(record)):
        events.append(
            {
                "severity": "warn",
                "rule_id": "pii_private_email_detected",
                "message": "Mögliche private E-Mail im Record gefunden.",
                "context": {"name": record.get("name")},
            }
        )

    return events
