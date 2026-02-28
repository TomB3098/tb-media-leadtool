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
        "notion": {"enabled": True},
        "compliance": {
            "allowed_sources": ["manual_public_csv", "seed_public_demo"],
            "max_requests_per_minute": 30,
            "disallow_private_emails": True,
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
    return cfg


def deep_merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result
