from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
PHONE_CLEAN_RE = re.compile(r"[^0-9+()\-/\s]")


@dataclass
class ValidationResult:
    valid: bool
    normalized: dict
    errors: list[str]


def _clean_text(value: str | None, max_len: int = 255) -> str | None:
    if value is None:
        return None
    s = " ".join(str(value).strip().split())
    if not s:
        return None
    return s[:max_len]


def _normalize_website(url: str | None) -> str | None:
    u = _clean_text(url, max_len=512)
    if not u:
        return None
    if not u.startswith(("http://", "https://")):
        u = f"https://{u}"
    parsed = urlparse(u)
    if parsed.scheme not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return u


def _normalize_email(value: str | None) -> str | None:
    v = _clean_text(value, max_len=200)
    if not v:
        return None
    v = v.lower()
    if not EMAIL_RE.match(v):
        return None
    if v.endswith(".invalid"):
        return None
    return v


def _normalize_phone(value: str | None) -> str | None:
    v = _clean_text(value, max_len=80)
    if not v:
        return None
    if PHONE_CLEAN_RE.search(v):
        return None
    digits = re.sub(r"\D", "", v)
    if len(digits) < 5:
        return None
    return v


def validate_lead_record(record: dict) -> ValidationResult:
    errors: list[str] = []

    name = _clean_text(record.get("name"), max_len=200)
    city = _clean_text(record.get("city"), max_len=120)
    industry = _clean_text(record.get("industry"), max_len=120) or "Dienstleister"

    if not name:
        errors.append("VALIDATION:NAME_MISSING")
    if not city:
        errors.append("VALIDATION:CITY_MISSING")

    website = _normalize_website(record.get("website_url"))
    email = _normalize_email(record.get("email"))
    phone = _normalize_phone(record.get("phone"))

    if record.get("website_url") and not website:
        errors.append("VALIDATION:WEBSITE_INVALID")
    if record.get("email") and not email:
        errors.append("VALIDATION:EMAIL_INVALID")
    if record.get("phone") and not phone:
        errors.append("VALIDATION:PHONE_INVALID")

    normalized = {
        "name": name,
        "industry": industry,
        "city": city,
        "postal_code": _clean_text(record.get("postal_code"), max_len=20),
        "address": _clean_text(record.get("address"), max_len=255),
        "website_url": website,
        "phone": phone,
        "email": email,
        "source_primary": _clean_text(record.get("source_primary"), max_len=80) or "unknown",
        "source_ref": _clean_text(record.get("source_ref"), max_len=255),
        "is_public_b2b": 1,
    }

    return ValidationResult(valid=not errors and bool(name and city), normalized=normalized, errors=errors)
