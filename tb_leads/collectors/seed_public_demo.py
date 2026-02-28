from __future__ import annotations

from typing import Any


def collect(region: str, industry: str, limit: int) -> list[dict[str, Any]]:
    base_names = [
        "Praxis am Stadtpark",
        "Malerbetrieb Niederrhein",
        "Kanzlei Rheinblick",
        "Zahnarztzentrum Krefeld",
        "Elektrotechnik Weber",
        "Steuerkanzlei König",
        "Physio am Ring",
        "Dachbau Lorenz",
        "Hausarztpraxis Süd",
        "Sanitär Becker",
    ]
    out: list[dict[str, Any]] = []
    for i, name in enumerate(base_names[: max(1, limit)]):
        out.append(
            {
                "name": name,
                "industry": industry,
                "city": region,
                "postal_code": f"47{790 + i}",
                "address": f"Musterstraße {i+1}",
                "website_url": f"https://example{i+1}.com",
                "phone": f"02151-{100000 + i}",
                "source_primary": "seed_public_demo",
                "source_ref": f"seed:{i+1}",
            }
        )
    return out
