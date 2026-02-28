from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse
import urllib.error
import urllib.request


@dataclass
class PropertyMap:
    title: str | None
    company: str | None
    contact_email: str | None
    phone: str | None
    source: str | None
    priority: str | None
    status: str | None
    stage: str | None
    notes: str | None


class NotionClient:
    def __init__(self, token: str | None, database_id: str | None):
        self.token = token
        self.database_id = database_id
        self._database_schema: dict[str, Any] | None = None

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.database_id)

    def _request(self, method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self.enabled:
            raise RuntimeError("Notion credentials fehlen")

        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(
            url,
            method=method,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            },
            data=data,
        )

        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _database(self) -> dict[str, Any]:
        if self._database_schema is None:
            self._database_schema = self._request("GET", f"https://api.notion.com/v1/databases/{self.database_id}")
        return self._database_schema

    def _properties(self) -> dict[str, Any]:
        return self._database().get("properties", {})

    def _find_prop(self, candidates: list[str], allowed_types: set[str] | None = None) -> str | None:
        props = self._properties()
        for c in candidates:
            if c in props and (allowed_types is None or props[c].get("type") in allowed_types):
                return c
        lower_map = {k.lower(): k for k in props.keys()}
        for c in candidates:
            key = lower_map.get(c.lower())
            if key and (allowed_types is None or props[key].get("type") in allowed_types):
                return key
        return None

    def _build_property_map(self) -> PropertyMap:
        props = self._properties()
        title_prop = None
        for name, meta in props.items():
            if meta.get("type") == "title":
                title_prop = name
                break

        return PropertyMap(
            title=title_prop,
            company=self._find_prop(["Company", "Kunde"], {"rich_text", "title"}),
            contact_email=self._find_prop(["Contact Email", "Email", "E-Mail"], {"email", "rich_text"}),
            phone=self._find_prop(["Phone", "Telefon"], {"phone_number", "rich_text"}),
            source=self._find_prop(["Source", "Quelle"], {"select", "rich_text"}),
            priority=self._find_prop(["Priority", "Priorität"], {"select", "rich_text"}),
            status=self._find_prop(["Status"], {"select", "rich_text"}),
            stage=self._find_prop(["Stage", "Phase"], {"select", "rich_text"}),
            notes=self._find_prop(["Notes", "Notizen", "Bemerkungen"], {"rich_text"}),
        )

    def _safe_text(self, value: str | None, max_len: int = 1800) -> str:
        return (value or "").strip()[:max_len]

    def _domain_from_url(self, url: str | None) -> str:
        if not url:
            return ""
        try:
            parsed = urlparse(url)
            return (parsed.netloc or "").lower().strip()
        except Exception:
            return ""

    def _company_marker(self, lead: dict[str, Any]) -> str:
        domain = lead.get("website_domain") or self._domain_from_url(lead.get("website_url"))
        return f"{lead.get('name','').strip()} | {lead.get('city','').strip()} | {domain}".strip()

    def _text_content(self, page: dict[str, Any], prop_name: str) -> str:
        prop = page.get("properties", {}).get(prop_name, {})
        ptype = prop.get("type")

        if ptype == "title":
            return "".join([x.get("plain_text", "") for x in prop.get("title", [])]).strip()
        if ptype == "rich_text":
            return "".join([x.get("plain_text", "") for x in prop.get("rich_text", [])]).strip()
        if ptype == "email":
            return (prop.get("email") or "").strip()
        if ptype == "phone_number":
            return (prop.get("phone_number") or "").strip()
        if ptype == "url":
            return (prop.get("url") or "").strip()
        if ptype == "select":
            return (prop.get("select") or {}).get("name", "")
        return ""

    def _query_database(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", f"https://api.notion.com/v1/databases/{self.database_id}/query", payload)

    def _find_existing_page_id(self, lead: dict[str, Any], pmap: PropertyMap) -> str | None:
        if not pmap.title:
            return None

        title_value = self._safe_text(lead.get("name"), max_len=200)
        marker = self._safe_text(self._company_marker(lead), max_len=200)

        filters = [{"property": pmap.title, "title": {"equals": title_value}}]

        if pmap.company:
            company_type = self._properties()[pmap.company].get("type")
            if company_type == "rich_text":
                filters.append({"property": pmap.company, "rich_text": {"equals": marker}})
            elif company_type == "title":
                filters.append({"property": pmap.company, "title": {"equals": marker}})

        query_payload = {"page_size": 10, "filter": {"and": filters} if len(filters) > 1 else filters[0]}

        data = self._query_database(query_payload)
        results = data.get("results", [])
        if results:
            return results[0].get("id")

        fallback_payload = {"page_size": 10, "filter": {"property": pmap.title, "title": {"equals": title_value}}}
        data = self._query_database(fallback_payload)
        for page in data.get("results", []):
            if pmap.company:
                if self._text_content(page, pmap.company) == marker:
                    return page.get("id")
            else:
                return page.get("id")

        return None

    def _select_option_name(self, prop_name: str, preferred: str) -> str | None:
        meta = self._properties().get(prop_name, {})
        options = meta.get("select", {}).get("options", []) if meta.get("type") == "select" else []
        names = [o.get("name") for o in options if o.get("name")]
        if preferred in names:
            return preferred
        return names[0] if names else None

    def _build_properties_payload(self, lead: dict[str, Any], pmap: PropertyMap) -> dict[str, Any]:
        props_meta = self._properties()
        payload: dict[str, Any] = {}

        def set_title(prop_name: str, value: str) -> None:
            payload[prop_name] = {"title": [{"text": {"content": self._safe_text(value)}}]}

        def set_rich(prop_name: str, value: str) -> None:
            payload[prop_name] = {"rich_text": [{"text": {"content": self._safe_text(value)}}]}

        def set_select(prop_name: str, value: str) -> None:
            payload[prop_name] = {"select": {"name": self._safe_text(value, max_len=100)}}

        if pmap.title:
            set_title(pmap.title, lead.get("name") or "Unbekannt")

        marker = self._company_marker(lead)
        if pmap.company:
            ptype = props_meta[pmap.company].get("type")
            if ptype == "rich_text":
                set_rich(pmap.company, marker)
            elif ptype == "title":
                set_title(pmap.company, marker)

        if pmap.contact_email and lead.get("email"):
            ptype = props_meta[pmap.contact_email].get("type")
            if ptype == "email":
                payload[pmap.contact_email] = {"email": self._safe_text(lead.get("email"), max_len=200)}
            elif ptype == "rich_text":
                set_rich(pmap.contact_email, lead.get("email"))

        if pmap.phone and lead.get("phone"):
            ptype = props_meta[pmap.phone].get("type")
            if ptype == "phone_number":
                payload[pmap.phone] = {"phone_number": self._safe_text(lead.get("phone"), max_len=50)}
            elif ptype == "rich_text":
                set_rich(pmap.phone, lead.get("phone"))

        if pmap.source:
            source_value = "Website"
            ptype = props_meta[pmap.source].get("type")
            if ptype == "select":
                option = self._select_option_name(pmap.source, source_value)
                if option:
                    set_select(pmap.source, option)
            elif ptype == "rich_text":
                set_rich(pmap.source, source_value)

        if pmap.priority:
            priority_map = {"A": "High", "B": "Medium", "C": "Low"}
            priority_value = priority_map.get((lead.get("score_class") or "B").upper(), "Medium")
            ptype = props_meta[pmap.priority].get("type")
            if ptype == "select":
                option = self._select_option_name(pmap.priority, priority_value)
                if option:
                    set_select(pmap.priority, option)
            elif ptype == "rich_text":
                set_rich(pmap.priority, priority_value)

        if pmap.status:
            status_value = "Active"
            ptype = props_meta[pmap.status].get("type")
            if ptype == "select":
                option = self._select_option_name(pmap.status, status_value)
                if option:
                    set_select(pmap.status, option)
            elif ptype == "rich_text":
                set_rich(pmap.status, status_value)

        if pmap.stage:
            stage_value = "Lead"
            ptype = props_meta[pmap.stage].get("type")
            if ptype == "select":
                option = self._select_option_name(pmap.stage, stage_value)
                if option:
                    set_select(pmap.stage, option)
            elif ptype == "rich_text":
                set_rich(pmap.stage, stage_value)

        if pmap.notes:
            notes = (
                f"Leadtool-Import\n"
                f"Stadt: {lead.get('city') or '-'}\n"
                f"Branche: {lead.get('industry') or '-'}\n"
                f"Score: {lead.get('score_total')} ({lead.get('score_class')})\n"
                f"Website: {lead.get('website_url') or '-'}\n"
                f"E-Mail: {lead.get('email') or '-'}\n"
                f"Adresse: {lead.get('address') or '-'}"
            )
            set_rich(pmap.notes, notes)

        return payload

    def upsert_lead(self, lead: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "skipped", "reason": "notion_credentials_missing"}

        try:
            pmap = self._build_property_map()
            props_payload = self._build_properties_payload(lead, pmap)
            existing_id = self._find_existing_page_id(lead, pmap)

            if existing_id:
                self._request("PATCH", f"https://api.notion.com/v1/pages/{existing_id}", {"properties": props_payload})
                return {"status": "success", "action": "updated", "notion_page_id": existing_id}

            data = self._request(
                "POST",
                "https://api.notion.com/v1/pages",
                {"parent": {"database_id": self.database_id}, "properties": props_payload},
            )
            return {"status": "success", "action": "created", "notion_page_id": data.get("id")}

        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore") if hasattr(exc, "read") else str(exc)
            return {"status": "failed", "error": f"HTTP {exc.code}: {detail}"}
        except Exception as exc:
            return {"status": "failed", "error": str(exc)}
