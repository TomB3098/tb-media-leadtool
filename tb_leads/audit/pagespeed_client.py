from __future__ import annotations

import json
import urllib.parse
from typing import Any

from tb_leads.utils.errors import ToolError
from tb_leads.utils.http import HttpClient


def _heuristic_pagespeed(response_time_ms: int | None) -> tuple[int, int, int, float, int]:
    rt = response_time_ms or 1500
    if rt <= 500:
        return 90, 2600, 1200, 0.03, 120
    if rt <= 900:
        return 75, 3200, 1800, 0.06, 220
    if rt <= 1400:
        return 60, 4200, 2400, 0.12, 380
    return 40, 5600, 3300, 0.2, 620


def _heuristic_result(response_time_ms: int | None, source: str, warning: str | None = None, error_code: str | None = None) -> dict[str, Any]:
    score, lcp, _fcp, cls, tbt = _heuristic_pagespeed(response_time_ms)
    warnings = [warning] if warning else []
    codes = [error_code] if error_code else []
    return {
        "mobile_pagespeed_score": score,
        "cwv_lcp_ms": lcp,
        "cwv_tbt_ms": tbt,
        "cwv_cls": cls,
        "source": source,
        "warnings": warnings,
        "error_codes": codes,
    }


def fetch_pagespeed(
    url: str | None,
    api_key: str | None,
    http_client: HttpClient,
    strategy: str = "mobile",
    response_time_ms: int | None = None,
) -> dict[str, Any]:
    if not url:
        return _heuristic_result(response_time_ms, source="heuristic_no_url", warning="PAGESPEED:NO_URL")

    if not api_key:
        return _heuristic_result(response_time_ms, source="heuristic_no_api_key", warning="PAGESPEED:API_KEY_MISSING")

    endpoint = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"
    params = urllib.parse.urlencode({"url": url, "strategy": strategy, "key": api_key})
    req_url = f"{endpoint}?{params}"

    try:
        body = http_client.get_text(req_url)
        data = json.loads(body)

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
            "warnings": [],
            "error_codes": [],
        }
    except ToolError as exc:
        return _heuristic_result(
            response_time_ms,
            source="heuristic_fallback",
            warning=f"PAGESPEED:FALLBACK:{exc.code}",
            error_code=exc.code,
        )
    except json.JSONDecodeError:
        return _heuristic_result(
            response_time_ms,
            source="heuristic_bad_json",
            warning="PAGESPEED:BAD_JSON",
            error_code="PAGESPEED_BAD_JSON",
        )
