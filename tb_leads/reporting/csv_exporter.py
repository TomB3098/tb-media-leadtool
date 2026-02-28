from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def export_scored_leads(scored: list[dict[str, Any]], out_dir: str, run_id: str) -> str:
    path = Path(out_dir)
    path.mkdir(parents=True, exist_ok=True)
    out_file = path / f"tb-leads-{run_id}.csv"

    fieldnames = [
        "company_id",
        "name",
        "industry",
        "city",
        "website_url",
        "phone",
        "score_total",
        "score_class",
        "priority_rank",
    ]

    with out_file.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in scored:
            writer.writerow(
                {
                    "company_id": row.get("company_id"),
                    "name": row.get("name"),
                    "industry": row.get("industry"),
                    "city": row.get("city"),
                    "website_url": row.get("website_url"),
                    "phone": row.get("phone"),
                    "score_total": row.get("score_total"),
                    "score_class": row.get("score_class"),
                    "priority_rank": row.get("priority_rank"),
                }
            )
    return str(out_file)
