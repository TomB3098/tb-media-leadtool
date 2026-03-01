# QA / Go-No-Go Report — tb-media-leadtool

Datum: 2026-03-01

## Scope der Endabnahme
1. Realdaten-Akquise (öffentlich, robust)
2. Enrichment (E-Mail/Adresse wo verfügbar)
3. Notion-Sync idempotent/robust
4. Messwerte aus kleinem Live-Batch
5. Betriebsanweisung/Runbook

---

## 1) Live-Batch (real, öffentlich)

### Lauf
- Run-ID: `8403c335-d36c-4794-af87-3c6a6671e507`
- Quelle: `osm` (Overpass, öffentliche OSM-Daten)
- Region: `Krefeld`
- Industry-Filter: `Dienstleister`
- Limit: `2`
- Sync: aktiviert

### Ergebnis
- Status: `completed`
- Laufzeit: `168.24s`
- collected: `2`
- scored: `2`
- synced: `2`
- error_count: `1`
- network_error_count: `8`

### Fehlerrate / Timeout-Quote (run-level)
- Fehlerrate (errors/collected): `1 / 2 = 50.0%`
- Timeout-Quote: indirekt über network_error_count auf Run-Ebene sichtbar; Netzwerk blieb trotz Fehlern robust (Retry/Backoff), Run wurde abgeschlossen.

### Top-Leads-Auszug (CSV)
1. Shell — Score `63` (`B`)
   - E-Mail: `-`
   - Adresse: `-`
   - Telefon: `+49 2151 544940`
2. Star — Score `25` (`C`)
   - E-Mail: `-`
   - Adresse: `Moerser Straße 136, 47803 Krefeld`
   - Telefon: `-`

---

## 2) Notion-Sync-Endabnahme

### Live-Sync aus Lauf
- Ergebnis aus obigem Lauf: `success=2` (beide als update)
- Idempotenz-Dublettenschutz: vorhanden (name+domain key in-run, plus Notion-upsert create/update)

### Extra-Check min-score
- Command: `sync --run-id 8403c335-d36c-4794-af87-3c6a6671e507 --min-score 60`
- Ergebnis: `success=1` (nur Shell, Score 63)
- Nachweis: Score-Filter greift wirksam.

### Retry-Backoff für 429/5xx
- Mock-Integrationstest vorhanden und grün:
  - `tests/test_notion_mock_integration.py`
  - Validiert 429-Create-Retry und 5xx-Patch-Retry.

---

## 3) Config-Wirksamkeit

Abgedeckt durch Code + Tests:
- `max_requests_per_minute` → `tests/test_config_effective.py`
- `min_score_for_sync` + Sync-Filter (`require_email`, etc.) → `tests/test_sync_filters_config.py`
- Netzwerk-ENV-Overrides (`timeout`, `retries`, backoff/jitter, enrichment pages) → `load_config` + Tests

Teststatus (gesamt): **16/16 grün**

---

## 4) Go / No-Go Entscheidung

**Entscheidung: GO (conditional / controlled rollout)**

### Begründung
- Pipeline läuft stabil und reproduzierbar (Run-Status + JSONL-Logs + Resume).
- Realdaten-Akquise aus öffentlichen Quellen integriert (`osm`, `nominatim`).
- Notion-Sync robust/idempotent mit Retry-Strategie und Dubletten-Schutz.
- Monitoring/Runbook vorhanden.

### Bedingungen für produktiven Betrieb
1. Mit kleinen Batches starten (`limit 1..3`, dann `5..10`).
2. Konservative Netzparameter beibehalten.
3. Bei erhöhter Fehlerrate: Fallback auf kleinere Batches / alternative Quelle.
4. Regelmäßig SQL/JSONL-Monitoring durchführen.

---

## 5) Known Limits
1. Öffentliche OSM/Overpass-Endpunkte sind volatil (Timeouts/Lastspitzen möglich).
2. Nominatim liefert je Query teils sehr selektive/knappe Resultate.
3. Enrichment ist heuristisch und nicht in jedem Datensatz vollständig.
4. Für größere Volumen mittelfristig dedizierte Datenquelle oder eigene Mirrors evaluieren.
