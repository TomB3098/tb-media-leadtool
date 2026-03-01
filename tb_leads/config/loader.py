from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _fallback_config() -> dict[str, Any]:
    return {
        "default_region": "Krefeld",
        "default_radius_km": 30,
        "default_limit": 30,
        "min_score_for_sync": 50,
        "pagespeed": {"strategy": "mobile"},
        "notion": {"enabled": True, "api_base_url": "https://api.notion.com/v1"},
        "compliance": {
            "allowed_sources": ["manual_public_csv", "seed_public_demo", "osm_overpass_public", "nominatim_public"],
            "max_requests_per_minute": 30,
            "disallow_private_emails": True,
        },
        "network": {
            "timeout_seconds": 10,
            "max_retries": 3,
            "backoff_base_seconds": 0.35,
            "backoff_max_seconds": 4.0,
            "jitter_seconds": 0.2,
        },
        "run": {
            "max_errors_per_run": 50,
            "max_network_errors_per_run": 20,
        },
        "filters": {
            "require_website_for_sync": False,
            "require_contact_for_sync": False,
            "require_email_for_sync": False,
        },
        "enrichment": {
            "max_pages": 4,
        },
    }


def load_config(path: str | None = None) -> dict[str, Any]:
    cfg = _fallback_config()
    if path is None:
        path = str(Path("config/default.yaml"))

    cfg_path = Path(path)
    if cfg_path.exists():
        try:
            import yaml  # type: ignore

            loaded = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
            if isinstance(loaded, dict):
                cfg = deep_merge(cfg, loaded)
        except Exception:
            # keep fallback defaults if YAML is unavailable or malformed
            pass

    db_path = os.getenv("TB_LEADS_DB_PATH")
    if db_path:
        cfg["db_path"] = db_path
    else:
        cfg.setdefault("db_path", "tb_leads.db")

    cfg["page_speed_api_key"] = os.getenv("PAGE_SPEED_API_KEY")
    cfg["notion_token"] = os.getenv("NOTION_TOKEN")
    cfg["notion_db_id"] = os.getenv("NOTION_DB_ID")

    notion_base = os.getenv("NOTION_API_BASE_URL")
    if notion_base:
        cfg.setdefault("notion", {})["api_base_url"] = notion_base.rstrip("/")

    # optional overrides
    if os.getenv("TB_LEADS_MAX_REQUESTS_PER_MINUTE"):
        cfg.setdefault("compliance", {})["max_requests_per_minute"] = int(os.getenv("TB_LEADS_MAX_REQUESTS_PER_MINUTE", "30"))
    if os.getenv("TB_LEADS_MAX_ERRORS_PER_RUN"):
        cfg.setdefault("run", {})["max_errors_per_run"] = int(os.getenv("TB_LEADS_MAX_ERRORS_PER_RUN", "50"))
    if os.getenv("TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN"):
        cfg.setdefault("run", {})["max_network_errors_per_run"] = int(os.getenv("TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN", "20"))

    if os.getenv("TB_LEADS_TIMEOUT_SECONDS"):
        cfg.setdefault("network", {})["timeout_seconds"] = float(os.getenv("TB_LEADS_TIMEOUT_SECONDS", "10"))
    if os.getenv("TB_LEADS_MAX_RETRIES"):
        cfg.setdefault("network", {})["max_retries"] = int(os.getenv("TB_LEADS_MAX_RETRIES", "3"))
    if os.getenv("TB_LEADS_BACKOFF_BASE_SECONDS"):
        cfg.setdefault("network", {})["backoff_base_seconds"] = float(os.getenv("TB_LEADS_BACKOFF_BASE_SECONDS", "0.35"))
    if os.getenv("TB_LEADS_BACKOFF_MAX_SECONDS"):
        cfg.setdefault("network", {})["backoff_max_seconds"] = float(os.getenv("TB_LEADS_BACKOFF_MAX_SECONDS", "4.0"))
    if os.getenv("TB_LEADS_JITTER_SECONDS"):
        cfg.setdefault("network", {})["jitter_seconds"] = float(os.getenv("TB_LEADS_JITTER_SECONDS", "0.2"))

    if os.getenv("TB_LEADS_ENRICHMENT_MAX_PAGES"):
        cfg.setdefault("enrichment", {})["max_pages"] = int(os.getenv("TB_LEADS_ENRICHMENT_MAX_PAGES", "4"))

    return cfg


def deep_merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
