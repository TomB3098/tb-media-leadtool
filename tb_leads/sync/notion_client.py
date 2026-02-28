from __future__ import annotations

import json
import urllib.request
from typing import Any


class NotionClient:
    def __init__(self, token: str | None, database_id: str | None):
        self.token = token
        self.database_id = database_id

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.database_id)

    def upsert_lead(self, lead: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "skipped", "reason": "notion_credentials_missing"}

        url = "https://api.notion.com/v1/pages"
        payload = {
            "parent": {"database_id": self.database_id},
            "properties": {
                "Name": {"title": [{"text": {"content": lead.get("name", "Unbekannt")}}]},
                "Stadt": {"rich_text": [{"text": {"content": lead.get("city", "")}}]},
                "Branche": {"rich_text": [{"text": {"content": lead.get("industry", "")}}]},
                "Score": {"number": float(lead.get("score_total", 0))},
                "Klasse": {"select": {"name": lead.get("score_class", "C")}},
                "Website": {"url": lead.get("website_url") or None},
                "Telefon": {"phone_number": lead.get("phone") or None},
            },
        }

        req = urllib.request.Request(
            url,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload).encode("utf-8"),
        )

        try:
            with urllib.request.urlopen(req, timeout=12) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return {"status": "success", "notion_page_id": data.get("id")}
        except Exception as exc:
            return {"status": "failed", "error": str(exc)}
