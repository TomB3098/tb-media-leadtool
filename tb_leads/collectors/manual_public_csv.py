from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def collect_from_csv(csv_path: str, region: str, industry: str, limit: int) -> list[dict[str, Any]]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV nicht gefunden: {csv_path}")

    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(out) >= limit:
                break
            city = (row.get("city") or "").strip() or region
            row_industry = (row.get("industry") or "").strip() or industry
            if region and city.lower() != region.lower():
                continue
            if industry and row_industry.lower() != industry.lower():
                continue

            out.append(
                {
                    "name": (row.get("name") or "").strip(),
                    "industry": row_industry,
                    "city": city,
                    "postal_code": (row.get("postal_code") or "").strip() or None,
                    "address": (row.get("address") or "").strip() or None,
                    "website_url": (row.get("website_url") or "").strip() or None,
                    "phone": (row.get("phone") or "").strip() or None,
                    "source_primary": "manual_public_csv",
                    "source_ref": (row.get("source_ref") or "").strip() or None,
                }
            )

    return [x for x in out if x.get("name")]
