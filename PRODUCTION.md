# PRODUCTION.md — tb-media-leadtool Runbook

## 1. Ziel
Dieses Dokument beschreibt den produktionsnahen Betrieb des `tb-leads` CLI-Tools für echte Internet-Leadgenerierung.

---

## 2. Betriebsmodell

`tb-leads` läuft batch-orientiert (z. B. per Cron) und schreibt einen nachvollziehbaren Audit-Trail in SQLite:
- Run-Status
- Fehlerzähler (gesamt + Netzwerk)
- Compliance-Events
- Sync-Logs (Notion)

### Run-Status
- `running` — laufend
- `completed` — vollständig erfolgreich
- `partial` — teilweise erfolgreich, mit Abbruchgrenze/Teilfehlern
- `failed` — hart fehlgeschlagen

---

## 3. Pflicht-Umgebungsvariablen (Produktiv)

## 3.1 Core
- `TB_LEADS_DB_PATH` (z. B. `/var/lib/tb-leads/tb_leads.db`)

## 3.2 Notion-Sync
- `NOTION_TOKEN`
- `NOTION_DB_ID`

## 3.3 Optional
- `PAGE_SPEED_API_KEY`
- `NOTION_API_BASE_URL` (nur für Tests/Mocking)
- `TB_LEADS_MAX_REQUESTS_PER_MINUTE`
- `TB_LEADS_MAX_ERRORS_PER_RUN`
- `TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN`

---

## 4. Wichtige Config-Parameter (`config/default.yaml`)

## 4.1 Netzwerk
- `network.timeout_seconds`
- `network.max_retries`
- `network.backoff_base_seconds`
- `network.backoff_max_seconds`
- `network.jitter_seconds`

## 4.2 Throttling
- `compliance.max_requests_per_minute`

## 4.3 Abbruchgrenzen
- `run.max_errors_per_run`
- `run.max_network_errors_per_run`

Empfehlung Startwerte:
- `max_requests_per_minute`: 20–40
- `timeout_seconds`: 8–12
- `max_retries`: 3

---

## 5. Standardbetrieb

## 5.1 Initialisierung
```bash
python -m tb_leads.cli.main init-db
```

## 5.2 End-to-End Lauf (CSV-Quelle)
```bash
python -m tb_leads.cli.main run \
  --region "Krefeld" \
  --industry "Dienstleister" \
  --limit 20 \
  --source csv \
  --csv-path examples/public_companies_sample.csv \
  --min-class B \
  --out reports
```

## 5.3 End-to-End Lauf (echte öffentliche Quelle: OSM)
```bash
python -m tb_leads.cli.main run \
  --region "Krefeld" \
  --industry "Dienstleister" \
  --limit 10 \
  --source osm \
  --radius-km 20 \
  --min-class B \
  --out reports
```

## 5.4 Resume nach Teilfehlern
```bash
python -m tb_leads.cli.main run --resume-latest --min-class B --out reports
# oder explizit
python -m tb_leads.cli.main run --resume-run-id <RUN_ID> --min-class B --out reports
```

---

## 6. Monitoring / Beobachtung

## 6.1 Wichtige Kennzahlen je Run
- Laufzeit (`elapsed_seconds` in run notes)
- `error_count`
- `network_error_count`
- `collected_count`, `scored_count`, `synced_count`
- Notion-Sync-Verteilung (`created/updated/failed/skipped`)
- JSONL-Runlog pro Lauf: `logs/run-<RUN_ID>.jsonl`

## 6.2 SQL-Checks
```sql
SELECT id, status, started_at, finished_at, error_count, network_error_count,
       collected_count, scored_count, synced_count, last_stage
FROM runs
ORDER BY started_at DESC
LIMIT 20;
```

```sql
SELECT run_id, severity, rule_id, message, created_at
FROM compliance_events
ORDER BY created_at DESC
LIMIT 50;
```

---

## 7. Fehlerbehandlung

## 7.1 Netzwerkfehler
Automatisch abgefedert durch:
- Retry
- Exponential Backoff
- Jitter
- Throttling

Bei Überschreitung von Abbruchgrenzen wird der Run als `partial` beendet.

### Fallback-Strategien
- Collector-Fallback: Für kritische Kampagnen OSM + CSV kombinieren (CSV als Backup-Quelle).
- Sync-Fallback: Bei Notion-Ausfall mit `--skip-sync` laufen lassen und später `tb-leads sync --run-id ...` nachziehen.
- Resume-Fallback: `--resume-latest` oder `--resume-run-id` nutzen, statt den gesamten Run neu zu starten.

## 7.2 Notion-spezifische Fehler
- `NOTION_AUTH` (401)
- `NOTION_FORBIDDEN` (403)
- `NOTION_RATE_LIMITED` (429)
- `NOTION_SERVER_ERROR` (5xx)

Diese Fehler werden als strukturierte Fehlercodes in Sync-Resultat und Run-Notes sichtbar.

---

## 8. Known Limits

1. OSM/Nominatim/Overpass haben faire Nutzungsgrenzen; bei höheren Volumina ist ein Mirror/paid data source empfehlenswert.
2. Adress-Parsing ist heuristisch — für Spezialfälle kann manuelle Nachprüfung nötig sein.
3. Notion-Property-Mapping ist schema-adaptiv, setzt aber vorhandene geeignete Felder im Ziel-CRM voraus.
4. SQLite ist für Single-Worker-Betrieb ausgelegt; für stark parallelen Betrieb später auf Postgres migrieren.
5. Live-Webseiten können langsam oder blockierend reagieren; konservative Limits + kleine Batches sind Pflicht.

---

## 9. Security / Compliance-Hinweise

- Nur öffentliche B2B-Daten verarbeiten.
- Keine privaten/sensiblen Daten sammeln.
- Rate-Limits konservativ halten.
- Token niemals im Repo speichern (nur ENV/Secrets-Store).
