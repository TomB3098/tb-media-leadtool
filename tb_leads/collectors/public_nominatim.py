from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus, urlparse

from tb_leads.utils.http import HttpClient


def _norm(value: str | None, max_len: int = 255) -> str | None:
    if value is None:
        return None
    out = " ".join(str(value).strip().split())
    return out[:max_len] if out else None


def _normalize_website(url: str | None) -> str | None:
    u = _norm(url, max_len=512)
    if not u:
        return None
    if not u.startswith(("http://", "https://")):
        u = f"https://{u}"
    parsed = urlparse(u)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return u


def collect_nominatim_public(
    region: str,
    industry: str,
    limit: int,
    http_client: HttpClient,
) -> list[dict[str, Any]]:
    q = quote_plus(f"{industry} {region}")
    url = (
        "https://nominatim.openstreetmap.org/search"
        f"?format=jsonv2&addressdetails=1&extratags=1&namedetails=1&limit={max(5, min(50, limit * 3))}&q={q}"
    )

    rows = http_client.get_json(
        url,
        headers={
            "Accept-Language": "de",
            "User-Agent": "tb-leads/1.0 (+public-leadtool)",
        },
    )

    if not isinstance(rows, list):
        return []

    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str | None]] = set()

    for row in rows:
        display_name = _norm(row.get("display_name"))
        name = _norm((row.get("namedetails") or {}).get("name") or display_name)
        if not name:
            continue

        extratags = row.get("extratags") or {}
        website = _normalize_website(extratags.get("website") or extratags.get("contact:website"))
        email = _norm(extratags.get("email") or extratags.get("contact:email"))
        phone = _norm(extratags.get("phone") or extratags.get("contact:phone"))

        address = row.get("address") or {}
        city = _norm(address.get("city") or address.get("town") or address.get("village") or region)
        street = _norm(address.get("road"))
        house = _norm(address.get("house_number"))
        postcode = _norm(address.get("postcode"))

        address_line = None
        if street and house and postcode and city:
            address_line = f"{street} {house}, {postcode} {city}"
        elif street and house and city:
            address_line = f"{street} {house}, {city}"
        elif postcode and city:
            address_line = f"{postcode} {city}"

        if not any([website, email, phone, address_line]):
            continue

        key = (name.lower(), (website or "").lower() or None)
        if key in seen:
            continue
        seen.add(key)

        out.append(
            {
                "name": name,
                "industry": industry,
                "city": city or region,
                "postal_code": postcode,
                "address": address_line,
                "website_url": website,
                "phone": phone,
                "email": email,
                "source_primary": "nominatim_public",
                "source_ref": f"nominatim:{row.get('place_id')}",
                "is_public_b2b": 1,
            }
        )

        if len(out) >= max(1, limit):
            break

    return out
