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
- Regelbasierte Score-Engine (0–100, A/B/C)
- Compliance-Basischecks (Source-Allowlist, simple PII-Checks, Event-Log)
- CSV-Report + Terminal-Summary

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

## Rechtlicher Rahmen

- Nur öffentlich verfügbare B2B-Daten
- Keine aggressiven Scraping-Methoden
- Keine Umgehung von Schutzmaßnahmen
- Compliance-Events werden pro Run protokolliert

## Status

MVP-Basis ist lauffähig; produktive Quellenadapter/Outreach-Automation werden iterativ ergänzt.
