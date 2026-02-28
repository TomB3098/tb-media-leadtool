from __future__ import annotations

import re


def seo_score_from_html(html: str) -> int:
    h = html or ""
    score = 0

    if re.search(r"<title>[^<]{10,}</title>", h, re.IGNORECASE):
        score += 35
    if re.search(r"<meta[^>]+name=[\"']description[\"'][^>]+content=[\"'][^\"']{40,}[\"']", h, re.IGNORECASE):
        score += 30
    if re.search(r"<h1[^>]*>[^<]{3,}</h1>", h, re.IGNORECASE):
        score += 20
    if "viewport" in h.lower():
        score += 15

    return min(score, 100)
