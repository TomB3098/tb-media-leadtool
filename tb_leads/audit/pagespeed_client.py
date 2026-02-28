from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any


def _heuristic_pagespeed(response_time_ms: int | None) -> tuple[int, int, int, float, int]:
    rt = response_time_ms or 1500
    if rt <= 500:
        return 90, 2600, 1200, 0.03, 120
    if rt <= 900:
        return 75, 3200, 1800, 0.06, 220
    if rt <= 1400:
        return 60, 4200, 2400, 0.12, 380
    return 40, 5600, 3300, 0.2, 620


def fetch_pagespeed(url: str | None, api_key: str | None, strategy: str = "mobile", response_time_ms: int | None = None) -> dict[str, Any]:
    if not url:
        score, lcp, fcp, cls, tbt = _heuristic_pagespeed(response_time_ms)
        return {
            "mobile_pagespeed_score": score,
            "cwv_lcp_ms": lcp,
            "cwv_tbt_ms": tbt,
            "cwv_cls": cls,
            "source": "heuristic",
        }

    if not api_key:
        score, lcp, fcp, cls, tbt = _heuristic_pagespeed(response_time_ms)
        return {
            "mobile_pagespeed_score": score,
            "cwv_lcp_ms": lcp,
            "cwv_tbt_ms": tbt,
            "cwv_cls": cls,
            "source": "heuristic",
        }

    endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = urllib.parse.urlencode({"url": url, "strategy": strategy, "key": api_key})
    req_url = f"{endpoint}?{params}"

    try:
        req = urllib.request.Request(req_url, headers={"User-Agent": "tb-leads/0.1"})
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        categories = data.get("lighthouseResult", {}).get("categories", {})
        audits = data.get("lighthouseResult", {}).get("audits", {})

        perf = int(round(float(categories.get("performance", {}).get("score", 0)) * 100))
        lcp = int(float(audits.get("largest-contentful-paint", {}).get("numericValue", 0)))
        tbt = int(float(audits.get("total-blocking-time", {}).get("numericValue", 0)))
        cls = float(audits.get("cumulative-layout-shift", {}).get("numericValue", 0.0))

        return {
            "mobile_pagespeed_score": perf,
            "cwv_lcp_ms": lcp,
            "cwv_tbt_ms": tbt,
            "cwv_cls": cls,
            "source": "google_pagespeed",
        }
    except Exception:
        score, lcp, fcp, cls, tbt = _heuristic_pagespeed(response_time_ms)
        return {
            "mobile_pagespeed_score": score,
            "cwv_lcp_ms": lcp,
            "cwv_tbt_ms": tbt,
            "cwv_cls": cls,
            "source": "heuristic_fallback",
        }
