from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from tb_leads.utils.errors import ErrorCode, ToolError
from tb_leads.utils.http import HttpClient


def _norm_text(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = " ".join(value.strip().split())
    return cleaned or None


def _industry_tokens(industry: str) -> list[str]:
    raw = (industry or "").lower()
    tokens = [raw]
    mapping = {
        "arzt": ["doctor", "clinic", "dentist", "physiotherapist"],
        "praxis": ["doctor", "clinic", "dentist", "physiotherapist"],
        "handwerk": ["craft", "electrician", "plumber", "carpenter", "roofer", "painter"],
        "kanzlei": ["lawyer", "attorney", "notary"],
        "dienstleister": ["office", "company", "business"],
    }
    for key, vals in mapping.items():
        if key in raw:
            tokens.extend(vals)
    # unique preserve order
    out: list[str] = []
    seen = set()
    for t in tokens:
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)
    return out


def _get_region_center(region: str, http_client: HttpClient) -> tuple[float, float]:
    url = (
        "https://nominatim.openstreetmap.org/search?format=jsonv2&limit=1"
        f"&q={quote_plus(region)}"
    )
    headers = {"Accept-Language": "de", "User-Agent": "tb-leads/1.0 (+public-leadtool)"}
    payload = http_client.get_json(url, headers=headers)
    if not isinstance(payload, list) or not payload:
        raise ToolError(ErrorCode.NETWORK_HTTP_4XX, f"Nominatim returned no results for region={region}")

    row = payload[0]
    try:
        lat = float(row["lat"])
        lon = float(row["lon"])
    except Exception as exc:  # noqa: BLE001
        raise ToolError(ErrorCode.NETWORK_HTTP_4XX, "Invalid geocode payload") from exc

    return lat, lon


def _build_overpass_query(lat: float, lon: float, radius_m: int, industry: str) -> str:
    tokens = _industry_tokens(industry)
    token_regex = "|".join(tokens) if tokens else ".*"

    # Pull public POIs with name and at least website/email/phone.
    return f"""
[out:json][timeout:30];
(
  node["name"]["website"](around:{radius_m},{lat},{lon});
  way["name"]["website"](around:{radius_m},{lat},{lon});
  relation["name"]["website"](around:{radius_m},{lat},{lon});
  node["name"]["contact:website"](around:{radius_m},{lat},{lon});
  way["name"]["contact:website"](around:{radius_m},{lat},{lon});
  relation["name"]["contact:website"](around:{radius_m},{lat},{lon});
  node["name"]["contact:email"](around:{radius_m},{lat},{lon});
  way["name"]["contact:email"](around:{radius_m},{lat},{lon});
  relation["name"]["contact:email"](around:{radius_m},{lat},{lon});
);
out center tags;
""".strip()


def collect_osm_public(
    region: str,
    industry: str,
    limit: int,
    http_client: HttpClient,
    radius_km: int = 20,
) -> list[dict[str, Any]]:
    lat, lon = _get_region_center(region, http_client=http_client)

    query = _build_overpass_query(lat=lat, lon=lon, radius_m=max(1000, int(radius_km * 1000)), industry=industry)

    overpass_endpoints = [
        "https://overpass-api.de/api/interpreter",
        "https://overpass.kumi.systems/api/interpreter",
        "https://overpass.openstreetmap.ru/api/interpreter",
    ]

    data: dict[str, Any] | None = None
    errors: list[str] = []
    for endpoint in overpass_endpoints:
        overpass_url = endpoint + "?data=" + quote_plus(query)
        try:
            candidate = http_client.get_json(
                overpass_url,
                headers={"User-Agent": "tb-leads/1.0 (+public-leadtool)"},
            )
            if isinstance(candidate, dict) and isinstance(candidate.get("elements"), list):
                data = candidate
                break
        except ToolError as exc:
            errors.append(f"{endpoint}:{exc.code}")
            continue

    if data is None:
        raise ToolError(
            ErrorCode.NETWORK_MAX_RETRIES,
            "All Overpass endpoints failed",
            detail="; ".join(errors[-3:]) if errors else "no response",
        )

    elements = data.get("elements", []) if isinstance(data, dict) else []

    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()

    for el in elements:
        tags = el.get("tags") or {}
        name = _norm_text(tags.get("name"))
        if not name:
            continue

        website = _norm_text(tags.get("website") or tags.get("contact:website"))
        email = _norm_text(tags.get("email") or tags.get("contact:email"))
        phone = _norm_text(tags.get("phone") or tags.get("contact:phone"))

        # require at least one contact-like field to ensure lead usefulness
        if not any([website, email, phone]):
            continue

        street = _norm_text(tags.get("addr:street"))
        house = _norm_text(tags.get("addr:housenumber"))
        postcode = _norm_text(tags.get("addr:postcode"))
        city = _norm_text(tags.get("addr:city")) or region

        address = None
        if street and house and postcode and city:
            address = f"{street} {house}, {postcode} {city}"
        elif street and house and city:
            address = f"{street} {house}, {city}"

        key = (name.lower(), website.lower() if website else None)
        if key in seen:
            continue
        seen.add(key)

        source_ref = f"osm:{el.get('type')}:{el.get('id')}"
        records.append(
            {
                "name": name,
                "industry": industry,
                "city": city,
                "postal_code": postcode,
                "address": address,
                "website_url": website,
                "phone": phone,
                "email": email,
                "source_primary": "osm_overpass_public",
                "source_ref": source_ref,
                "is_public_b2b": 1,
            }
        )

        if len(records) >= max(1, int(limit)):
            break

    return records
