from __future__ import annotations

import argparse
import sys
from typing import Any

from tb_leads.audit.service import run_audit
from tb_leads.collectors.manual_public_csv import collect_from_csv
from tb_leads.collectors.seed_public_demo import collect as seed_collect
from tb_leads.compliance.checker import basic_record_checks
from tb_leads.config.loader import load_config
from tb_leads.db.repository import Repository
from tb_leads.db.schema import init_db
from tb_leads.reporting.csv_exporter import export_scored_leads
from tb_leads.reporting.summary import summarize
from tb_leads.scoring.engine import score_lead
from tb_leads.sync.notion_client import NotionClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tb-leads", description="TB Media Leadtool MVP CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialisiert die SQLite-Datenbank")

    collect = sub.add_parser("collect", help="Sammelt Leads und erstellt einen Run")
    collect.add_argument("--region", required=True)
    collect.add_argument("--industry", required=True)
    collect.add_argument("--limit", type=int, default=30)
    collect.add_argument("--source", choices=["seed", "csv"], default="seed")
    collect.add_argument("--csv-path", default="examples/public_companies_sample.csv")

    audit = sub.add_parser("audit", help="Auditiert Websites für bestehenden Run")
    audit.add_argument("--run-id", required=True)

    score = sub.add_parser("score", help="Scored Leads für bestehenden Run")
    score.add_argument("--run-id", required=True)

    sync = sub.add_parser("sync", help="Synchronisiert Leads nach Notion")
    sync.add_argument("--run-id", required=True)
    sync.add_argument("--min-class", choices=["A", "B", "C"], default="B")

    report = sub.add_parser("report", help="Exportiert CSV + Summary")
    report.add_argument("--run-id", required=True)
    report.add_argument("--out", default="reports")

    run = sub.add_parser("run", help="End-to-End Pipeline")
    run.add_argument("--region", required=True)
    run.add_argument("--industry", required=True)
    run.add_argument("--limit", type=int, default=30)
    run.add_argument("--source", choices=["seed", "csv"], default="seed")
    run.add_argument("--csv-path", default="examples/public_companies_sample.csv")
    run.add_argument("--min-class", choices=["A", "B", "C"], default="B")
    run.add_argument("--out", default="reports")
    run.add_argument("--skip-sync", action="store_true")

    return parser


def _collect_records(args: argparse.Namespace, run_id: str, cfg: dict[str, Any], repo: Repository) -> int:
    if args.source == "csv":
        records = collect_from_csv(args.csv_path, args.region, args.industry, args.limit)
    else:
        records = seed_collect(args.region, args.industry, args.limit)

    allowed = cfg.get("compliance", {}).get("allowed_sources", [])
    for record in records:
        events = basic_record_checks(record, allowed)
        for ev in events:
            repo.insert_compliance_event(
                run_id=run_id,
                severity=ev["severity"],
                rule_id=ev["rule_id"],
                message=ev["message"],
                context=ev.get("context"),
            )

        if any(ev["severity"] == "error" for ev in events):
            continue

        company_id = repo.upsert_company(record)
        repo.insert_source_record(
            company_id=company_id,
            run_id=run_id,
            source_name=record.get("source_primary", "unknown"),
            source_url=record.get("source_ref"),
            raw_payload=record,
        )

    companies = repo.get_companies_for_run(run_id)
    repo.update_run_counts(run_id, collected_count=len(companies))
    return len(companies)


def _audit_records(run_id: str, cfg: dict[str, Any], repo: Repository) -> dict[str, int]:
    companies = repo.get_companies_for_run(run_id)
    strategy = cfg.get("pagespeed", {}).get("strategy", "mobile")
    key = cfg.get("page_speed_api_key")

    enriched_count = 0
    for company in companies:
        audit = run_audit(company.website_url, key, strategy=strategy)
        repo.insert_website_audit(company.id, run_id, audit)

        email = audit.get("enriched_email")
        address = audit.get("enriched_address")
        source_url = audit.get("enriched_contact_source_url")
        if any([email, address]):
            enriched_count += 1

        repo.update_company_enrichment(
            company_id=company.id,
            email=email,
            address_enriched=address,
            contact_source_url=source_url,
        )

    return {"audited": len(companies), "enriched": enriched_count}


def _score_records(run_id: str, repo: Repository) -> int:
    companies = repo.get_companies_for_run(run_id)
    audits = repo.latest_audit_for_run(run_id)

    scored = []
    for company in companies:
        audit = audits.get(company.id)
        if not audit:
            continue
        result = score_lead(audit)
        scored.append((company.id, result))

    scored.sort(key=lambda x: x[1]["total"], reverse=True)
    for rank, (company_id, result) in enumerate(scored, start=1):
        repo.insert_lead_score(
            company_id=company_id,
            run_id=run_id,
            score_total=result["total"],
            score_class=result["class"],
            breakdown=result["breakdown"],
            priority_rank=rank,
        )

    repo.update_run_counts(run_id, scored_count=len(scored))
    return len(scored)


def _sync_records(run_id: str, min_class: str, cfg: dict[str, Any], repo: Repository) -> dict[str, Any]:
    leads = repo.get_scored_leads_for_run(run_id, min_class=min_class)
    notion = NotionClient(token=cfg.get("notion_token"), database_id=cfg.get("notion_db_id"))

    result_counts = {
        "success": 0,
        "created": 0,
        "updated": 0,
        "failed": 0,
        "skipped": 0,
    }
    example_lines: list[str] = []

    for lead in leads:
        result = notion.upsert_lead(lead)
        status = result.get("status", "failed")
        action = result.get("action")

        if status == "success":
            result_counts["success"] += 1
            if action == "created":
                result_counts["created"] += 1
            if action == "updated":
                result_counts["updated"] += 1
        elif status == "skipped":
            result_counts["skipped"] += 1
        else:
            result_counts["failed"] += 1

        if len(example_lines) < 5:
            example_lines.append(
                f"- {lead.get('name')} | {lead.get('score_class')} {lead.get('score_total')} | "
                f"email={lead.get('email') or '-'} | address={lead.get('address') or '-'} | "
                f"sync={status}/{action or '-'}"
            )

        repo.insert_notion_sync(
            company_id=lead["company_id"],
            run_id=run_id,
            status=status,
            notion_page_id=result.get("notion_page_id"),
            sync_error=result.get("error") or result.get("reason"),
        )

    repo.update_run_counts(run_id, synced_count=result_counts["success"])
    return {"counts": result_counts, "examples": example_lines}


def _report(run_id: str, out: str, repo: Repository) -> str:
    leads = repo.get_scored_leads_for_run(run_id, min_class="C")
    path = export_scored_leads(leads, out, run_id)
    print(summarize(leads))
    print(f"CSV: {path}")
    return path


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config()
    db_path = cfg.get("db_path", "tb_leads.db")

    if args.command == "init-db":
        init_db(db_path)
        print(f"DB initialisiert: {db_path}")
        return 0

    # All other commands require DB
    init_db(db_path)
    repo = Repository(db_path)

    if args.command == "collect":
        run_id = repo.create_run(args.region, args.industry, args.limit)
        count = _collect_records(args, run_id, cfg, repo)
        repo.finish_run(run_id, status="success", notes="collect completed")
        print(f"Run erstellt: {run_id}")
        print(f"Collect abgeschlossen. Leads: {count}")
        return 0

    if args.command == "audit":
        audit_result = _audit_records(args.run_id, cfg, repo)
        print(
            f"Audit abgeschlossen für {audit_result['audited']} Companies "
            f"(Enrichment mit E-Mail/Adresse: {audit_result['enriched']}) (run_id={args.run_id})"
        )
        return 0

    if args.command == "score":
        scored = _score_records(args.run_id, repo)
        print(f"Scoring abgeschlossen. Scores: {scored} (run_id={args.run_id})")
        return 0

    if args.command == "sync":
        sync_result = _sync_records(args.run_id, args.min_class, cfg, repo)
        c = sync_result["counts"]
        print(
            "Sync abgeschlossen. "
            f"success={c['success']} (created={c['created']}, updated={c['updated']}) "
            f"failed={c['failed']} skipped={c['skipped']} (run_id={args.run_id})"
        )
        if sync_result["examples"]:
            print("Sync-Beispiele:")
            for line in sync_result["examples"]:
                print(line)
        return 0

    if args.command == "report":
        _report(args.run_id, args.out, repo)
        return 0

    if args.command == "run":
        run_id = repo.create_run(args.region, args.industry, args.limit)
        try:
            collected = _collect_records(args, run_id, cfg, repo)
            audit_result = _audit_records(run_id, cfg, repo)
            scored = _score_records(run_id, repo)
            sync_result = {"counts": {"success": 0, "created": 0, "updated": 0, "failed": 0, "skipped": 0}, "examples": []}
            if not args.skip_sync:
                sync_result = _sync_records(run_id, args.min_class, cfg, repo)
            _report(run_id, args.out, repo)
            repo.finish_run(run_id, status="success", notes="run completed")
            c = sync_result["counts"]
            print(f"Run erfolgreich: {run_id}")
            print(
                f"collect={collected} audit={audit_result['audited']} enriched={audit_result['enriched']} "
                f"score={scored} sync_success={c['success']}"
            )
            if sync_result["examples"]:
                print("Sync-Beispiele:")
                for line in sync_result["examples"]:
                    print(line)
            return 0
        except Exception as exc:
            repo.finish_run(run_id, status="failed", notes=str(exc))
            print(f"Run fehlgeschlagen: {run_id} - {exc}", file=sys.stderr)
            return 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
