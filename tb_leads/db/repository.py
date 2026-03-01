from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def normalize_name(value: str) -> str:
    return " ".join(value.lower().strip().split())


def domain_of(url: str | None) -> str | None:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        host = parsed.netloc.lower().strip()
        return host or None
    except Exception:
        return None


@dataclass
class CompanyRecord:
    id: str
    name: str
    industry: str
    city: str
    postal_code: str | None
    address: str | None
    address_enriched: str | None
    website_url: str | None
    website_domain: str | None
    phone: str | None
    email: str | None
    source_primary: str
    source_ref: str | None
    contact_source_url: str | None


class Repository:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def create_run(self, region: str, industry: str, limit: int, resumed_from_run_id: str | None = None) -> str:
        run_id = str(uuid.uuid4())
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO runs(
                    id, started_at, status, region, industry, limit_requested,
                    network_error_count, last_stage, resumed_from_run_id
                )
                VALUES(?, ?, 'running', ?, ?, ?, 0, 'init', ?)
                """,
                (run_id, utcnow_iso(), region, industry, limit, resumed_from_run_id),
            )
            conn.commit()
        return run_id

    def set_run_stage(self, run_id: str, stage: str) -> None:
        with self._conn() as conn:
            conn.execute("UPDATE runs SET last_stage=? WHERE id=?", (stage, run_id))
            conn.commit()

    def append_run_note(self, run_id: str, note: str) -> None:
        with self._conn() as conn:
            current = conn.execute("SELECT notes FROM runs WHERE id=?", (run_id,)).fetchone()
            prev = (current[0] if current and current[0] else "").strip()
            merged = f"{prev}\n{note}".strip() if prev else note
            conn.execute("UPDATE runs SET notes=? WHERE id=?", (merged, run_id))
            conn.commit()

    def finish_run(self, run_id: str, status: str = "completed", notes: str | None = None) -> None:
        status = status if status in {"running", "completed", "partial", "failed", "success"} else "failed"
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE runs
                SET finished_at=?, status=?, notes=COALESCE(?, notes)
                WHERE id=?
                """,
                (utcnow_iso(), status, notes, run_id),
            )
            conn.commit()

    def update_run_counts(
        self,
        run_id: str,
        collected_count: int | None = None,
        scored_count: int | None = None,
        synced_count: int | None = None,
        error_count: int | None = None,
        network_error_count: int | None = None,
    ) -> None:
        updates: list[str] = []
        values: list[Any] = []
        if collected_count is not None:
            updates.append("collected_count=?")
            values.append(collected_count)
        if scored_count is not None:
            updates.append("scored_count=?")
            values.append(scored_count)
        if synced_count is not None:
            updates.append("synced_count=?")
            values.append(synced_count)
        if error_count is not None:
            updates.append("error_count=?")
            values.append(error_count)
        if network_error_count is not None:
            updates.append("network_error_count=?")
            values.append(network_error_count)

        if not updates:
            return

        with self._conn() as conn:
            conn.execute(f"UPDATE runs SET {', '.join(updates)} WHERE id=?", (*values, run_id))
            conn.commit()

    def upsert_company(self, payload: dict[str, Any]) -> str:
        name = payload["name"]
        city = payload["city"]
        website_url = payload.get("website_url")
        website_domain = domain_of(website_url)
        n_name = normalize_name(name)
        domain_norm = website_domain or ""

        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT id FROM companies
                WHERE name_normalized=? AND city=? AND website_domain_norm=?
                """,
                (n_name, city, domain_norm),
            )
            row = cur.fetchone()
            if row:
                company_id = row["id"]
                enrichment_present = any(payload.get(k) for k in ("email", "address_enriched", "contact_source_url"))
                conn.execute(
                    """
                    UPDATE companies
                    SET industry=?, postal_code=?, address=?, website_url=?, website_domain=?, website_domain_norm=?,
                        phone=?,
                        email=COALESCE(?, email),
                        address_enriched=COALESCE(?, address_enriched),
                        contact_source_url=COALESCE(?, contact_source_url),
                        enrichment_updated_at=CASE WHEN ? THEN ? ELSE enrichment_updated_at END,
                        source_primary=?, source_ref=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        payload.get("industry", "Unbekannt"),
                        payload.get("postal_code"),
                        payload.get("address"),
                        website_url,
                        website_domain,
                        domain_norm,
                        payload.get("phone"),
                        payload.get("email"),
                        payload.get("address_enriched"),
                        payload.get("contact_source_url"),
                        1 if enrichment_present else 0,
                        utcnow_iso(),
                        payload.get("source_primary", "unknown"),
                        payload.get("source_ref"),
                        utcnow_iso(),
                        company_id,
                    ),
                )
            else:
                company_id = str(uuid.uuid4())
                enrichment_present = any(payload.get(k) for k in ("email", "address_enriched", "contact_source_url"))
                conn.execute(
                    """
                    INSERT INTO companies(
                      id, name, name_normalized, industry, city, postal_code, address,
                      website_url, website_domain, website_domain_norm, phone,
                      email, address_enriched, contact_source_url, enrichment_updated_at,
                      source_primary, source_ref, is_public_b2b, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                    """,
                    (
                        company_id,
                        name,
                        n_name,
                        payload.get("industry", "Unbekannt"),
                        city,
                        payload.get("postal_code"),
                        payload.get("address"),
                        website_url,
                        website_domain,
                        domain_norm,
                        payload.get("phone"),
                        payload.get("email"),
                        payload.get("address_enriched"),
                        payload.get("contact_source_url"),
                        utcnow_iso() if enrichment_present else None,
                        payload.get("source_primary", "unknown"),
                        payload.get("source_ref"),
                        utcnow_iso(),
                        utcnow_iso(),
                    ),
                )
            conn.commit()
        return company_id

    def update_company_enrichment(
        self,
        company_id: str,
        email: str | None,
        address_enriched: str | None,
        contact_source_url: str | None,
    ) -> None:
        if not any([email, address_enriched, contact_source_url]):
            return

        with self._conn() as conn:
            conn.execute(
                """
                UPDATE companies
                SET email=COALESCE(?, email),
                    address_enriched=COALESCE(?, address_enriched),
                    contact_source_url=COALESCE(?, contact_source_url),
                    enrichment_updated_at=?,
                    updated_at=?
                WHERE id=?
                """,
                (email, address_enriched, contact_source_url, utcnow_iso(), utcnow_iso(), company_id),
            )
            conn.commit()

    def insert_source_record(self, company_id: str, run_id: str, source_name: str, source_url: str | None, raw_payload: dict[str, Any]) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO source_records(id, company_id, source_name, source_url, raw_payload_json, collected_at, run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    company_id,
                    source_name,
                    source_url,
                    json.dumps(raw_payload, ensure_ascii=False),
                    utcnow_iso(),
                    run_id,
                ),
            )
            conn.commit()

    def get_companies_for_run(self, run_id: str) -> list[CompanyRecord]:
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT DISTINCT c.*
                FROM companies c
                JOIN source_records s ON s.company_id = c.id
                WHERE s.run_id = ?
                ORDER BY c.name ASC
                """,
                (run_id,),
            )
            rows = cur.fetchall()
        return [
            CompanyRecord(
                id=r["id"],
                name=r["name"],
                industry=r["industry"],
                city=r["city"],
                postal_code=r["postal_code"],
                address=r["address"],
                address_enriched=r["address_enriched"],
                website_url=r["website_url"],
                website_domain=r["website_domain"],
                phone=r["phone"],
                email=r["email"],
                source_primary=r["source_primary"],
                source_ref=r["source_ref"],
                contact_source_url=r["contact_source_url"],
            )
            for r in rows
        ]

    def clear_run_audits(self, run_id: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM website_audits WHERE run_id=?", (run_id,))
            conn.commit()

    def clear_run_scores(self, run_id: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM lead_scores WHERE run_id=?", (run_id,))
            conn.commit()

    def clear_run_sync_logs(self, run_id: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM notion_sync WHERE run_id=?", (run_id,))
            conn.commit()

    def insert_website_audit(self, company_id: str, run_id: str, audit: dict[str, Any]) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO website_audits(
                  id, company_id, run_id, http_status, website_present, mobile_pagespeed_score,
                  seo_score, cwv_lcp_ms, cwv_cls, cwv_tbt_ms, has_contact_cta, has_contact_form,
                  tech_health_score, audit_warnings_json, audited_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    company_id,
                    run_id,
                    audit.get("http_status"),
                    int(bool(audit.get("website_present"))),
                    audit.get("mobile_pagespeed_score"),
                    audit.get("seo_score"),
                    audit.get("cwv_lcp_ms"),
                    audit.get("cwv_cls"),
                    audit.get("cwv_tbt_ms"),
                    int(bool(audit.get("has_contact_cta"))),
                    int(bool(audit.get("has_contact_form"))),
                    audit.get("tech_health_score"),
                    json.dumps(audit.get("warnings", []), ensure_ascii=False),
                    utcnow_iso(),
                ),
            )
            conn.commit()

    def latest_audit_for_run(self, run_id: str) -> dict[str, dict[str, Any]]:
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT wa.*
                FROM website_audits wa
                JOIN (
                    SELECT company_id, MAX(audited_at) max_audited
                    FROM website_audits
                    WHERE run_id=?
                    GROUP BY company_id
                ) x ON x.company_id=wa.company_id AND x.max_audited=wa.audited_at
                WHERE wa.run_id=?
                """,
                (run_id, run_id),
            )
            rows = cur.fetchall()

        out: dict[str, dict[str, Any]] = {}
        for r in rows:
            out[r["company_id"]] = dict(r)
        return out

    def insert_lead_score(self, company_id: str, run_id: str, score_total: int, score_class: str, breakdown: dict[str, Any], priority_rank: int | None) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO lead_scores(id, company_id, run_id, score_total, score_class, score_breakdown_json, priority_rank, scored_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    company_id,
                    run_id,
                    score_total,
                    score_class,
                    json.dumps(breakdown, ensure_ascii=False),
                    priority_rank,
                    utcnow_iso(),
                ),
            )
            conn.commit()

    def get_scored_leads_for_run(self, run_id: str, min_class: str = "C") -> list[dict[str, Any]]:
        class_rank = {"A": 3, "B": 2, "C": 1}
        min_rank = class_rank.get(min_class.upper(), 1)

        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT
                  ls.*,
                  c.name,
                  c.industry,
                  c.city,
                  c.website_url,
                  c.website_domain,
                  c.phone,
                  c.email,
                  c.address AS address_source,
                  c.address_enriched,
                  COALESCE(c.address_enriched, c.address) AS address,
                  c.contact_source_url
                FROM lead_scores ls
                JOIN companies c ON c.id = ls.company_id
                WHERE ls.run_id=?
                ORDER BY ls.score_total DESC
                """,
                (run_id,),
            )
            rows = [dict(r) for r in cur.fetchall()]

        filtered = [r for r in rows if class_rank.get(r["score_class"], 0) >= min_rank]
        return filtered

    def insert_notion_sync(self, company_id: str, run_id: str, status: str, notion_page_id: str | None = None, sync_error: str | None = None) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO notion_sync(id, company_id, run_id, notion_page_id, sync_status, sync_error, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), company_id, run_id, notion_page_id, status, sync_error, utcnow_iso()),
            )
            conn.commit()

    def insert_compliance_event(self, run_id: str, severity: str, rule_id: str, message: str, context: dict[str, Any] | None = None) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO compliance_events(id, run_id, severity, rule_id, message, context_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    run_id,
                    severity,
                    rule_id,
                    message,
                    json.dumps(context or {}, ensure_ascii=False),
                    utcnow_iso(),
                ),
            )
            conn.commit()

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM runs WHERE id=?", (run_id,))
            row = cur.fetchone()
        return dict(row) if row else None

    def get_latest_resumable_run(self) -> dict[str, Any] | None:
        with self._conn() as conn:
            cur = conn.execute(
                """
                SELECT * FROM runs
                WHERE status IN ('running', 'partial', 'failed')
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
        return dict(row) if row else None
