from __future__ import annotations

import time
import urllib.error
import urllib.request
from typing import Any


def probe_website(url: str | None, timeout: float = 6.0) -> dict[str, Any]:
    if not url:
        return {
            "website_present": False,
            "http_status": None,
            "response_time_ms": None,
            "html": "",
            "warnings": ["website_missing"],
        }

    start = time.perf_counter()
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "tb-leads/0.1"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read(200000)
            elapsed_ms = int((time.perf_counter() - start) * 1000)
            html = body.decode("utf-8", errors="ignore")
            return {
                "website_present": True,
                "http_status": int(resp.status),
                "response_time_ms": elapsed_ms,
                "html": html,
                "warnings": [],
            }
    except urllib.error.HTTPError as e:
        return {
            "website_present": True,
            "http_status": int(e.code),
            "response_time_ms": int((time.perf_counter() - start) * 1000),
            "html": "",
            "warnings": [f"http_error_{e.code}"],
        }
    except Exception:
        return {
            "website_present": True,
            "http_status": None,
            "response_time_ms": int((time.perf_counter() - start) * 1000),
            "html": "",
            "warnings": ["network_error"],
        }
