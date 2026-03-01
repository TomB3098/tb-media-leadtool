# TB Media Leadtool

Interne MVP-CLI zur Lead-Generierung für TB MEDIA UG.

Das Tool sammelt öffentlich verfügbare B2B-Daten, führt einen technischen Website-Quick-Check aus, scored Leads (A/B/C) und kann Ergebnisse in Notion synchronisieren.

## MVP-Funktionen (v1)

- CLI `tb-leads` mit Subcommands:
  - `init-db`
  - `collect`
  - `audit`
  - `score`
  - `sync`
  - `report`
  - `run` (End-to-End)
- SQLite-Datenmodell (Runs, Companies, Audits, Scores, Sync-Log)
- Dedup (Name + Ort + Domain)
- Enrichment: E-Mail + Adresse aus Impressum/Kontaktseiten
- Regelbasierte Score-Engine (0–100, A/B/C)
- Compliance-Basischecks (Source-Allowlist, simple PII-Checks, Event-Log)
- Notion-Sync mit idempotentem Upsert (Create/Update)
- Netzwerk-Hardening: Retry + Exponential Backoff + Jitter + Timeouts
- Globales Throttling via `max_requests_per_minute`
- Run-Schutzgrenzen (max errors / max network errors) + sauberer Run-Status
- CSV-Report + Terminal-Summary (inkl. E-Mail/Adresse)

## Quickstart

```bash
python -m tb_leads.cli.main init-db
# reale öffentliche Quelle (OSM)
python -m tb_leads.cli.main run --region "Krefeld" --industry "Dienstleister" --limit 10 --source osm --radius-km 20
# alternative Public-Quelle
python -m tb_leads.cli.main run --region "Krefeld" --industry "Dienstleister" --limit 10 --source nominatim
# oder CSV-Quelle
python -m tb_leads.cli.main run --region "Krefeld" --industry "Arztpraxen" --limit 15 --source csv --csv-path examples/public_companies_sample.csv
python -m tb_leads.cli.main report --run-id <RUN_ID> --out reports
```

## Konfiguration

Optional über `config/default.yaml` und ENV-Variablen:

- `TB_LEADS_DB_PATH` (default: `./tb_leads.db`)
- `PAGE_SPEED_API_KEY`
- `NOTION_TOKEN`
- `NOTION_DB_ID`
- `NOTION_API_BASE_URL` (optional, z. B. für Tests)
- `TB_LEADS_MAX_REQUESTS_PER_MINUTE`
- `TB_LEADS_MAX_ERRORS_PER_RUN`
- `TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN`
- `TB_LEADS_TIMEOUT_SECONDS`
- `TB_LEADS_MAX_RETRIES`
- `TB_LEADS_BACKOFF_BASE_SECONDS`
- `TB_LEADS_BACKOFF_MAX_SECONDS`
- `TB_LEADS_JITTER_SECONDS`
- `TB_LEADS_ENRICHMENT_MAX_PAGES`

Wichtige Sync-Steuerung über Config:
- `min_score_for_sync` (Score-Schwelle für Sync)
- `filters.require_website_for_sync`
- `filters.require_contact_for_sync`
- `filters.require_email_for_sync`

## Rechtlicher Rahmen

- Nur öffentlich verfügbare B2B-Daten
- Keine aggressiven Scraping-Methoden
- Keine Umgehung von Schutzmaßnahmen
- Compliance-Events werden pro Run protokolliert

## Status

Production-Hardening läuft in Teilphasen; OSM-Collector + resilientere Pipeline sind integriert. Weitere Quellenadapter folgen iterativ.
