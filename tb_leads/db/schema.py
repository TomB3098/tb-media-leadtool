from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL CHECK(status IN ('running','completed','partial','failed','success')),
    region TEXT NOT NULL,
    industry TEXT NOT NULL,
    limit_requested INTEGER NOT NULL,
    collected_count INTEGER NOT NULL DEFAULT 0,
    scored_count INTEGER NOT NULL DEFAULT 0,
    synced_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    network_error_count INTEGER NOT NULL DEFAULT 0,
    last_stage TEXT NOT NULL DEFAULT 'init',
    resumed_from_run_id TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_normalized TEXT NOT NULL,
    industry TEXT NOT NULL,
    city TEXT NOT NULL,
    postal_code TEXT,
    address TEXT,
    website_url TEXT,
    website_domain TEXT,
    website_domain_norm TEXT NOT NULL DEFAULT '',
    phone TEXT,
    email TEXT,
    address_enriched TEXT,
    contact_source_url TEXT,
    enrichment_updated_at TEXT,
    source_primary TEXT NOT NULL,
    source_ref TEXT,
    is_public_b2b INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(name_normalized, city, website_domain_norm)
);

CREATE TABLE IF NOT EXISTS source_records (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT,
    raw_payload_json TEXT NOT NULL,
    collected_at TEXT NOT NULL,
    run_id TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS website_audits (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    http_status INTEGER,
    website_present INTEGER NOT NULL DEFAULT 0,
    mobile_pagespeed_score INTEGER,
    seo_score INTEGER,
    cwv_lcp_ms INTEGER,
    cwv_cls REAL,
    cwv_tbt_ms INTEGER,
    has_contact_cta INTEGER NOT NULL DEFAULT 0,
    has_contact_form INTEGER NOT NULL DEFAULT 0,
    tech_health_score INTEGER,
    audit_warnings_json TEXT,
    audited_at TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS lead_scores (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    score_total INTEGER NOT NULL,
    score_class TEXT NOT NULL CHECK(score_class IN ('A','B','C')),
    score_breakdown_json TEXT NOT NULL,
    priority_rank INTEGER,
    scored_at TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS notion_sync (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    notion_page_id TEXT,
    sync_status TEXT NOT NULL CHECK(sync_status IN ('pending','success','failed','skipped')),
    sync_error TEXT,
    synced_at TEXT,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS outreach_drafts (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT 'email',
    subject TEXT,
    draft_text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(company_id) REFERENCES companies(id),
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE TABLE IF NOT EXISTS compliance_events (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    severity TEXT NOT NULL CHECK(severity IN ('info','warn','error')),
    rule_id TEXT NOT NULL,
    message TEXT NOT NULL,
    context_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(website_domain);
CREATE INDEX IF NOT EXISTS idx_companies_city_industry ON companies(city, industry);
CREATE INDEX IF NOT EXISTS idx_scores_class_total ON lead_scores(score_class, score_total DESC);
CREATE INDEX IF NOT EXISTS idx_runs_started_at ON runs(started_at DESC);
"""


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type_sql: str) -> None:
    if not _column_exists(conn, table, column):
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type_sql}")


def _ensure_runs_status_constraint(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='runs'"
    ).fetchone()
    sql = (row[0] if row and row[0] else "").lower()
    if "completed" in sql:
        return

    # Rebuild runs table with updated CHECK constraint.
    conn.execute("ALTER TABLE runs RENAME TO runs_old")
    conn.execute(
        """
        CREATE TABLE runs (
            id TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL CHECK(status IN ('running','completed','partial','failed','success')),
            region TEXT NOT NULL,
            industry TEXT NOT NULL,
            limit_requested INTEGER NOT NULL,
            collected_count INTEGER NOT NULL DEFAULT 0,
            scored_count INTEGER NOT NULL DEFAULT 0,
            synced_count INTEGER NOT NULL DEFAULT 0,
            error_count INTEGER NOT NULL DEFAULT 0,
            network_error_count INTEGER NOT NULL DEFAULT 0,
            last_stage TEXT NOT NULL DEFAULT 'init',
            resumed_from_run_id TEXT,
            notes TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO runs(
            id, started_at, finished_at, status, region, industry, limit_requested,
            collected_count, scored_count, synced_count, error_count, network_error_count,
            last_stage, resumed_from_run_id, notes
        )
        SELECT
            id,
            started_at,
            finished_at,
            CASE WHEN status='success' THEN 'completed' ELSE status END,
            region,
            industry,
            limit_requested,
            collected_count,
            scored_count,
            synced_count,
            error_count,
            COALESCE(network_error_count, 0),
            COALESCE(last_stage, 'init'),
            resumed_from_run_id,
            notes
        FROM runs_old
        """
    )
    conn.execute("DROP TABLE runs_old")


def _apply_migrations(conn: sqlite3.Connection) -> None:
    # Backward-compatible migration path for already initialized DB files
    _ensure_column(conn, "companies", "website_domain_norm", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(conn, "companies", "email", "TEXT")
    _ensure_column(conn, "companies", "address_enriched", "TEXT")
    _ensure_column(conn, "companies", "contact_source_url", "TEXT")
    _ensure_column(conn, "companies", "enrichment_updated_at", "TEXT")

    _ensure_column(conn, "runs", "network_error_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(conn, "runs", "last_stage", "TEXT NOT NULL DEFAULT 'init'")
    _ensure_column(conn, "runs", "resumed_from_run_id", "TEXT")

    _ensure_runs_status_constraint(conn)

    # Fill website_domain_norm for older rows
    conn.execute("UPDATE companies SET website_domain_norm='' WHERE website_domain_norm IS NULL")


def init_db(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        _apply_migrations(conn)
        conn.commit()
    finally:
        conn.close()
