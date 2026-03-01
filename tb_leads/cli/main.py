from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Any

from tb_leads.audit.service import run_audit
from tb_leads.collectors.manual_public_csv import collect_from_csv
from tb_leads.collectors.seed_public_demo import collect as seed_collect
from tb_leads.collectors.public_osm import collect_osm_public
from tb_leads.collectors.public_nominatim import collect_nominatim_public
from tb_leads.compliance.checker import basic_record_checks
from tb_leads.config.loader import load_config
from tb_leads.db.repository import Repository
from tb_leads.db.schema import init_db
from tb_leads.reporting.csv_exporter import export_scored_leads
from tb_leads.reporting.summary import summarize
from tb_leads.scoring.engine import score_lead
from tb_leads.sync.notion_client import NotionClient
from tb_leads.enrich.validators import validate_lead_record
from tb_leads.utils.errors import ErrorCode, ToolError
from tb_leads.utils.http import HttpClient
from tb_leads.utils.retry import RetryPolicy
from tb_leads.utils.throttle import RateLimiter
from tb_leads.utils.runlog import RunLogger


@dataclass
class RunCounters:
    collected: int = 0
    audited: int = 0
    enriched: int = 0
    scored: int = 0
    sync_success: int = 0
    sync_created: int = 0
    sync_updated: int = 0
    sync_failed: int = 0
    sync_skipped: int = 0
    error_count: int = 0
    network_error_count: int = 0


@dataclass
class RunLimits:
    max_errors_per_run: int
    max_network_errors_per_run: int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="tb-leads", description="TB Media Leadtool CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="Initialisiert die SQLite-Datenbank")

    collect = sub.add_parser("collect", help="Sammelt Leads und erstellt einen Run")
    collect.add_argument("--region", required=True)
    collect.add_argument("--industry", required=True)
    collect.add_argument("--limit", type=int, default=30)
    collect.add_argument("--source", choices=["seed", "csv", "osm", "nominatim"], default="seed")
    collect.add_argument("--csv-path", default="examples/public_companies_sample.csv")
    collect.add_argument("--radius-km", type=int, default=20)

    audit = sub.add_parser("audit", help="Auditiert Websites für bestehenden Run")
    audit.add_argument("--run-id", required=True)

    score = sub.add_parser("score", help="Scored Leads für bestehenden Run")
    score.add_argument("--run-id", required=True)

    sync = sub.add_parser("sync", help="Synchronisiert Leads nach Notion")
    sync.add_argument("--run-id", required=True)
    sync.add_argument("--min-class", choices=["A", "B", "C"], default=None)
    sync.add_argument("--min-score", type=int, default=None)

    report = sub.add_parser("report", help="Exportiert CSV + Summary")
    report.add_argument("--run-id", required=True)
    report.add_argument("--out", default="reports")

    run = sub.add_parser("run", help="End-to-End Pipeline")
    run.add_argument("--region")
    run.add_argument("--industry")
    run.add_argument("--limit", type=int)
    run.add_argument("--source", choices=["seed", "csv", "osm", "nominatim"], default="seed")
    run.add_argument("--csv-path", default="examples/public_companies_sample.csv")
    run.add_argument("--radius-km", type=int, default=20)
    run.add_argument("--min-class", choices=["A", "B", "C"], default=None)
    run.add_argument("--min-score", type=int, default=None)
    run.add_argument("--out", default="reports")
    run.add_argument("--skip-sync", action="store_true")
    run.add_argument("--resume-run-id")
    run.add_argument("--resume-latest", action="store_true")

    return parser


def _make_http_client(cfg: dict[str, Any]) -> HttpClient:
    max_rpm = int(cfg.get("compliance", {}).get("max_requests_per_minute", 30))
    network_cfg = cfg.get("network", {})

    retry_policy = RetryPolicy(
        max_attempts=int(network_cfg.get("max_retries", 3)),
        base_delay_s=float(network_cfg.get("backoff_base_seconds", 0.35)),
        max_delay_s=float(network_cfg.get("backoff_max_seconds", 4.0)),
        jitter_s=float(network_cfg.get("jitter_seconds", 0.2)),
    )

    return HttpClient(
        timeout_s=float(network_cfg.get("timeout_seconds", 10)),
        rate_limiter=RateLimiter(max_requests_per_minute=max_rpm),
        retry_policy=retry_policy,
    )


def _run_limits(cfg: dict[str, Any]) -> RunLimits:
    run_cfg = cfg.get("run", {})
    return RunLimits(
        max_errors_per_run=int(run_cfg.get("max_errors_per_run", 50)),
        max_network_errors_per_run=int(run_cfg.get("max_network_errors_per_run", 20)),
    )


def _check_abort_thresholds(run_id: str, counters: RunCounters, limits: RunLimits, repo: Repository) -> None:
    if counters.error_count > limits.max_errors_per_run:
        msg = f"{ErrorCode.RUN_ABORT_THRESHOLD}: max_errors_per_run exceeded ({counters.error_count}>{limits.max_errors_per_run})"
        repo.append_run_note(run_id, msg)
        raise ToolError(ErrorCode.RUN_ABORT_THRESHOLD, msg)
    if counters.network_error_count > limits.max_network_errors_per_run:
        msg = (
            f"{ErrorCode.RUN_ABORT_THRESHOLD}: max_network_errors_per_run exceeded "
            f"({counters.network_error_count}>{limits.max_network_errors_per_run})"
        )
        repo.append_run_note(run_id, msg)
        raise ToolError(ErrorCode.RUN_ABORT_THRESHOLD, msg)


def _collect_records(
    args: argparse.Namespace,
    run_id: str,
    cfg: dict[str, Any],
    repo: Repository,
    counters: RunCounters,
    http_client: HttpClient,
) -> int:
    repo.set_run_stage(run_id, "collect")

    if args.source == "csv":
        records = collect_from_csv(args.csv_path, args.region, args.industry, args.limit)
    elif args.source == "osm":
        records = collect_osm_public(
            region=args.region,
            industry=args.industry,
            limit=args.limit,
            http_client=http_client,
            radius_km=int(args.radius_km or 20),
        )
    elif args.source == "nominatim":
        records = collect_nominatim_public(
            region=args.region,
            industry=args.industry,
            limit=args.limit,
            http_client=http_client,
        )
    else:
        records = seed_collect(args.region, args.industry, args.limit)

    allowed = cfg.get("compliance", {}).get("allowed_sources", [])
    seen_keys: set[tuple[str, str | None, str]] = set()

    for record in records:
        validation = validate_lead_record(record)
        if not validation.valid:
            for code in validation.errors:
                repo.insert_compliance_event(
                    run_id=run_id,
                    severity="error",
                    rule_id=code,
                    message="Lead record validation failed",
                    context={"record": record.get("name"), "source": record.get("source_primary")},
                )
                counters.error_count += 1
            continue

        normalized = validation.normalized

        dedupe_key = (
            (normalized.get("name") or "").lower(),
            (normalized.get("website_url") or "").lower() or None,
            (normalized.get("city") or "").lower(),
        )
        if dedupe_key in seen_keys:
            repo.insert_compliance_event(
                run_id=run_id,
                severity="info",
                rule_id="DEDUP:IN_RUN_DUPLICATE",
                message="Duplicate lead dropped within current run",
                context={"name": normalized.get("name"), "city": normalized.get("city")},
            )
            continue
        seen_keys.add(dedupe_key)

        events = basic_record_checks(normalized, allowed)
        for ev in events:
            repo.insert_compliance_event(
                run_id=run_id,
                severity=ev["severity"],
                rule_id=ev["rule_id"],
                message=ev["message"],
                context=ev.get("context"),
            )
            if ev["severity"] == "error":
                counters.error_count += 1

        if any(ev["severity"] == "error" for ev in events):
            continue

        company_id = repo.upsert_company(normalized)
        repo.insert_source_record(
            company_id=company_id,
            run_id=run_id,
            source_name=normalized.get("source_primary", "unknown"),
            source_url=normalized.get("source_ref"),
            raw_payload=normalized,
        )

    companies = repo.get_companies_for_run(run_id)
    counters.collected = len(companies)
    repo.update_run_counts(run_id, collected_count=counters.collected, error_count=counters.error_count)
    return counters.collected


def _audit_records(
    run_id: str,
    cfg: dict[str, Any],
    repo: Repository,
    counters: RunCounters,
    http_client: HttpClient,
) -> dict[str, int]:
    repo.set_run_stage(run_id, "audit")

    companies = repo.get_companies_for_run(run_id)
    strategy = cfg.get("pagespeed", {}).get("strategy", "mobile")
    key = cfg.get("page_speed_api_key")
    enrichment_max_pages = int(cfg.get("enrichment", {}).get("max_pages", 4))

    repo.clear_run_audits(run_id)
    enriched_count = 0
    for company in companies:
        audit = run_audit(
            company.website_url,
            key,
            http_client=http_client,
            strategy=strategy,
            enrichment_max_pages=enrichment_max_pages,
        )
        repo.insert_website_audit(company.id, run_id, audit)

        email = audit.get("enriched_email")
        address = audit.get("enriched_address")
        source_url = audit.get("enriched_contact_source_url")
        if any([email, address]):
            enriched_count += 1

        counters.network_error_count += int(audit.get("network_error_count") or 0)
        counters.error_count += len(audit.get("error_codes") or [])

        repo.update_company_enrichment(
            company_id=company.id,
            email=email,
            address_enriched=address,
            contact_source_url=source_url,
        )

    counters.audited = len(companies)
    counters.enriched = enriched_count
    repo.update_run_counts(
        run_id,
        error_count=counters.error_count,
        network_error_count=counters.network_error_count,
    )
    return {"audited": counters.audited, "enriched": counters.enriched}


def _score_records(run_id: str, repo: Repository, counters: RunCounters) -> int:
    repo.set_run_stage(run_id, "score")

    companies = repo.get_companies_for_run(run_id)
    audits = repo.latest_audit_for_run(run_id)

    repo.clear_run_scores(run_id)
    scored: list[tuple[str, dict[str, Any]]] = []
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

    counters.scored = len(scored)
    repo.update_run_counts(run_id, scored_count=counters.scored)
    return counters.scored


def _sync_records(
    run_id: str,
    min_class: str | None,
    min_score: int | None,
    cfg: dict[str, Any],
    repo: Repository,
    counters: RunCounters,
    http_client: HttpClient,
) -> dict[str, Any]:
    repo.set_run_stage(run_id, "sync")

    effective_min_score = int(min_score if min_score is not None else cfg.get("min_score_for_sync", 0))
    if min_class:
        effective_min_class = min_class
    else:
        if effective_min_score >= 80:
            effective_min_class = "A"
        elif effective_min_score >= 50:
            effective_min_class = "B"
        else:
            effective_min_class = "C"

    leads = repo.get_scored_leads_for_run(run_id, min_class=effective_min_class)

    if effective_min_score > 0:
        leads = [lead for lead in leads if int(lead.get("score_total") or 0) >= effective_min_score]

    filters = cfg.get("filters", {})
    if filters.get("require_website_for_sync"):
        leads = [lead for lead in leads if lead.get("website_url")]
    if filters.get("require_contact_for_sync"):
        leads = [lead for lead in leads if lead.get("email") or lead.get("phone")]
    if filters.get("require_email_for_sync"):
        leads = [lead for lead in leads if lead.get("email")]

    notion = NotionClient(
        token=cfg.get("notion_token"),
        database_id=cfg.get("notion_db_id"),
        http_client=http_client,
        api_base_url=cfg.get("notion", {}).get("api_base_url", "https://api.notion.com/v1"),
    )

    repo.clear_run_sync_logs(run_id)

    result_counts = {
        "success": 0,
        "created": 0,
        "updated": 0,
        "failed": 0,
        "skipped": 0,
    }
    example_lines: list[str] = []

    seen_sync_keys: set[tuple[str, str]] = set()

    for lead in leads:
        sync_key = ((lead.get("name") or "").strip().lower(), (lead.get("website_domain") or lead.get("website_url") or "").strip().lower())
        if sync_key in seen_sync_keys:
            result = {"status": "skipped", "reason": "in_run_duplicate_sync_key", "action": "dedupe"}
        else:
            seen_sync_keys.add(sync_key)
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
            counters.error_count += 1
            if str(result.get("error_code", "")).startswith("NOTION_"):
                counters.network_error_count += 1

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

    counters.sync_success = result_counts["success"]
    counters.sync_created = result_counts["created"]
    counters.sync_updated = result_counts["updated"]
    counters.sync_failed = result_counts["failed"]
    counters.sync_skipped = result_counts["skipped"]

    repo.update_run_counts(
        run_id,
        synced_count=counters.sync_success,
        error_count=counters.error_count,
        network_error_count=counters.network_error_count,
    )
    return {"counts": result_counts, "examples": example_lines}


def _report(run_id: str, out: str, repo: Repository) -> str:
    repo.set_run_stage(run_id, "report")
    leads = repo.get_scored_leads_for_run(run_id, min_class="C")
    path = export_scored_leads(leads, out, run_id)
    print(summarize(leads))
    print(f"CSV: {path}")
    return path


def _resolve_run_for_execution(args: argparse.Namespace, cfg: dict[str, Any], repo: Repository) -> tuple[str, bool]:
    """Returns (run_id, resumed)."""
    if getattr(args, "resume_run_id", None):
        run = repo.get_run(args.resume_run_id)
        if not run:
            raise ToolError("RUN_NOT_FOUND", f"Run {args.resume_run_id} wurde nicht gefunden")
        repo.set_run_stage(args.resume_run_id, "resume")
        repo.append_run_note(args.resume_run_id, "Resumed via --resume-run-id")
        return args.resume_run_id, True

    if getattr(args, "resume_latest", False):
        run = repo.get_latest_resumable_run()
        if run:
            repo.set_run_stage(run["id"], "resume")
            repo.append_run_note(run["id"], "Resumed via --resume-latest")
            return run["id"], True

    region = args.region or cfg.get("default_region")
    industry = args.industry or "Dienstleister"
    limit = args.limit or int(cfg.get("default_limit", 30))
    run_id = repo.create_run(region, industry, limit)
    return run_id, False


def _print_sync_result(run_id: str, sync_result: dict[str, Any]) -> None:
    c = sync_result["counts"]
    print(
        "Sync abgeschlossen. "
        f"success={c['success']} (created={c['created']}, updated={c['updated']}) "
        f"failed={c['failed']} skipped={c['skipped']} (run_id={run_id})"
    )
    if sync_result["examples"]:
        print("Sync-Beispiele:")
        for line in sync_result["examples"]:
            print(line)


def _run_pipeline(args: argparse.Namespace, cfg: dict[str, Any], repo: Repository, http_client: HttpClient) -> int:
    run_id, resumed = _resolve_run_for_execution(args, cfg, repo)
    run = repo.get_run(run_id) or {}
    run_logger = RunLogger(run_id=run_id)
    # backfill args when resuming
    if resumed:
        args.region = args.region or run.get("region")
        args.industry = args.industry or run.get("industry")
        args.limit = args.limit or int(run.get("limit_requested") or cfg.get("default_limit", 30))

    counters = RunCounters(
        collected=int(run.get("collected_count") or 0),
        scored=int(run.get("scored_count") or 0),
        sync_success=int(run.get("synced_count") or 0),
        error_count=int(run.get("error_count") or 0),
        network_error_count=int(run.get("network_error_count") or 0),
    )
    limits = _run_limits(cfg)

    started = time.monotonic()
    partial = False
    run_logger.event("run", "start", {"resumed": resumed, "source": args.source, "limit": args.limit})
    try:
        if not resumed:
            _collect_records(args, run_id, cfg, repo, counters, http_client)
            run_logger.event("collect", "done", {"count": counters.collected, "errors": counters.error_count})
        else:
            run_logger.event("collect", "skipped", {"reason": "resumed"})
        _check_abort_thresholds(run_id, counters, limits, repo)

        _audit_records(run_id, cfg, repo, counters, http_client)
        run_logger.event(
            "audit",
            "done",
            {
                "audited": counters.audited,
                "enriched": counters.enriched,
                "errors": counters.error_count,
                "network_errors": counters.network_error_count,
            },
        )
        _check_abort_thresholds(run_id, counters, limits, repo)

        _score_records(run_id, repo, counters)
        run_logger.event("score", "done", {"scored": counters.scored})
        _check_abort_thresholds(run_id, counters, limits, repo)

        sync_result = {"counts": {"success": 0, "created": 0, "updated": 0, "failed": 0, "skipped": 0}, "examples": []}
        if not args.skip_sync:
            sync_result = _sync_records(run_id, args.min_class, args.min_score, cfg, repo, counters, http_client)
            run_logger.event("sync", "done", sync_result.get("counts", {}))
            _check_abort_thresholds(run_id, counters, limits, repo)
        else:
            run_logger.event("sync", "skipped", {"reason": "--skip-sync"})

        _report(run_id, args.out, repo)
        run_logger.event("report", "done", {"out": args.out})

        elapsed = time.monotonic() - started
        repo.append_run_note(run_id, f"elapsed_seconds={elapsed:.2f}")

        final_status = "partial" if sync_result["counts"].get("failed", 0) > 0 else "completed"
        repo.finish_run(run_id, status=final_status, notes=f"run finished in {elapsed:.2f}s")
        run_logger.event("run", "finish", {"status": final_status, "elapsed_seconds": round(elapsed, 2)})

        print(f"Run abgeschlossen: {run_id} [{final_status}]")
        print(f"Run-Log: {run_logger.path}")
        print(
            f"collect={counters.collected} audit={counters.audited} enriched={counters.enriched} "
            f"score={counters.scored} sync_success={counters.sync_success} errors={counters.error_count} "
            f"network_errors={counters.network_error_count} elapsed={elapsed:.2f}s"
        )

        if sync_result["examples"]:
            print("Sync-Beispiele:")
            for line in sync_result["examples"]:
                print(line)

        return 0

    except ToolError as exc:
        partial = True
        repo.append_run_note(run_id, f"{exc.code}: {exc.message}")
        repo.update_run_counts(
            run_id,
            error_count=counters.error_count + 1,
            network_error_count=counters.network_error_count,
        )
        repo.finish_run(run_id, status="partial", notes=f"pipeline interrupted: {exc.code}")
        run_logger.event("run", "partial", {"code": exc.code, "message": exc.message})
        print(f"Run partial: {run_id} - {exc.code} {exc.message}")
        return 2
    except Exception as exc:  # noqa: BLE001
        repo.append_run_note(run_id, f"UNHANDLED: {exc}")
        repo.finish_run(run_id, status="failed", notes=str(exc))
        run_logger.event("run", "failed", {"error": str(exc)})
        print(f"Run fehlgeschlagen: {run_id} - {exc}")
        return 1
    finally:
        if partial:
            repo.set_run_stage(run_id, "partial")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    cfg = load_config()
    db_path = cfg.get("db_path", "tb_leads.db")

    if args.command == "init-db":
        init_db(db_path)
        print(f"DB initialisiert: {db_path}")
        return 0

    init_db(db_path)
    repo = Repository(db_path)
    http_client = _make_http_client(cfg)

    if args.command == "collect":
        run_id = repo.create_run(args.region, args.industry, args.limit)
        counters = RunCounters()
        _collect_records(args, run_id, cfg, repo, counters, http_client)
        repo.finish_run(run_id, status="completed", notes="collect completed")
        print(f"Run erstellt: {run_id}")
        print(f"Collect abgeschlossen. Leads: {counters.collected}")
        return 0

    if args.command == "audit":
        counters = RunCounters()
        result = _audit_records(args.run_id, cfg, repo, counters, http_client)
        print(
            f"Audit abgeschlossen für {result['audited']} Companies "
            f"(Enrichment mit E-Mail/Adresse: {result['enriched']}) "
            f"errors={counters.error_count} net_errors={counters.network_error_count}"
        )
        return 0

    if args.command == "score":
        counters = RunCounters()
        scored = _score_records(args.run_id, repo, counters)
        print(f"Scoring abgeschlossen. Scores: {scored} (run_id={args.run_id})")
        return 0

    if args.command == "sync":
        counters = RunCounters()
        sync_result = _sync_records(args.run_id, args.min_class, args.min_score, cfg, repo, counters, http_client)
        _print_sync_result(args.run_id, sync_result)
        return 0

    if args.command == "report":
        _report(args.run_id, args.out, repo)
        return 0

    if args.command == "run":
        return _run_pipeline(args, cfg, repo, http_client)

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
