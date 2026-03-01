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
python -m tb_leads.cli.main run --region "Krefeld" --industry "Arztpraxen" --limit 15
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

## Rechtlicher Rahmen

- Nur öffentlich verfügbare B2B-Daten
- Keine aggressiven Scraping-Methoden
- Keine Umgehung von Schutzmaßnahmen
- Compliance-Events werden pro Run protokolliert

## Status

MVP-Basis ist lauffähig; produktive Quellenadapter/Outreach-Automation werden iterativ ergänzt.
