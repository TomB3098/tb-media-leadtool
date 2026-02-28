from __future__ import annotations

from typing import Any


def _score_website_present(audit: dict[str, Any]) -> int:
    return 20 if audit.get("website_present") else 0


def _score_pagespeed(audit: dict[str, Any]) -> int:
    value = int(audit.get("mobile_pagespeed_score") or 0)
    if value >= 90:
        return 25
    if value >= 70:
        return 15
    if value >= 50:
        return 8
    return 2 if value > 0 else 0


def _score_seo(audit: dict[str, Any]) -> int:
    seo = int(audit.get("seo_score") or 0)
    return round((seo / 100) * 20)


def _score_contact(audit: dict[str, Any]) -> int:
    cta = 10 if audit.get("has_contact_cta") else 0
    form = 10 if audit.get("has_contact_form") else 0
    return cta + form


def _score_tech(audit: dict[str, Any]) -> int:
    th = int(audit.get("tech_health_score") or 0)
    return round((th / 100) * 15)


def classify(total: int) -> str:
    if total >= 80:
        return "A"
    if total >= 50:
        return "B"
    return "C"


def score_lead(audit: dict[str, Any]) -> dict[str, Any]:
    breakdown = {
        "website_present": _score_website_present(audit),
        "mobile_pagespeed": _score_pagespeed(audit),
        "seo_basics": _score_seo(audit),
        "contact_path": _score_contact(audit),
        "tech_health": _score_tech(audit),
    }
    total = sum(breakdown.values())
    return {
        "total": total,
        "class": classify(total),
        "breakdown": breakdown,
    }
