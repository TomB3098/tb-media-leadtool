from __future__ import annotations

import time
from typing import Any

from tb_leads.utils.errors import ToolError
from tb_leads.utils.http import HttpClient


def probe_website(url: str | None, http_client: HttpClient) -> dict[str, Any]:
    if not url:
        return {
            "website_present": False,
            "http_status": None,
            "response_time_ms": None,
            "html": "",
            "warnings": ["INPUT:WEBSITE_MISSING"],
            "error_codes": [],
        }

    start = time.perf_counter()
    try:
        html = http_client.get_text(url)
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return {
            "website_present": True,
            "http_status": 200,
            "response_time_ms": elapsed_ms,
            "html": html,
            "warnings": [],
            "error_codes": [],
        }
    except ToolError as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        return {
            "website_present": True,
            "http_status": None,
            "response_time_ms": elapsed_ms,
            "html": "",
            "warnings": [f"NETWORK:{exc.code}"],
            "error_codes": [exc.code],
        }
