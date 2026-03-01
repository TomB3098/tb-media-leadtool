from __future__ import annotations

from typing import Any

from tb_leads.audit.cta_checks import detect_contact_signals
from tb_leads.audit.pagespeed_client import fetch_pagespeed
from tb_leads.audit.seo_checks import seo_score_from_html
from tb_leads.audit.website_probe import probe_website
from tb_leads.enrich.contact_enrichment import enrich_contact_data
from tb_leads.utils.http import HttpClient


def run_audit(
    website_url: str | None,
    page_speed_api_key: str | None,
    http_client: HttpClient,
    strategy: str = "mobile",
    enrichment_max_pages: int = 4,
) -> dict[str, Any]:
    probe = probe_website(url=website_url, http_client=http_client)
    html = probe.get("html", "")
    has_cta, has_form = detect_contact_signals(html)
    seo_score = seo_score_from_html(html) if html else 0

    ps = fetch_pagespeed(
        url=website_url,
        api_key=page_speed_api_key,
        http_client=http_client,
        strategy=strategy,
        response_time_ms=probe.get("response_time_ms"),
    )

    enrichment = enrich_contact_data(
        website_url,
        http_client=http_client,
        max_pages=max(1, int(enrichment_max_pages)),
    )

    # Tech health rough aggregation 0..100
    tech_health = int(
        (ps.get("mobile_pagespeed_score", 0) * 0.5)
        + (seo_score * 0.3)
        + (15 if has_form else 0)
        + (5 if has_cta else 0)
    )
    tech_health = max(0, min(100, tech_health))

    warnings = list(probe.get("warnings", []))
    warnings.extend(ps.get("warnings", []))
    warnings.extend(enrichment.warnings)

    error_codes = list(probe.get("error_codes", []))
    error_codes.extend(ps.get("error_codes", []))

    network_error_count = len([c for c in error_codes if c]) + len([w for w in enrichment.warnings if w.startswith("NETWORK:")])

    return {
        "website_present": bool(probe.get("website_present")),
        "http_status": probe.get("http_status"),
        "mobile_pagespeed_score": ps.get("mobile_pagespeed_score"),
        "seo_score": seo_score,
        "cwv_lcp_ms": ps.get("cwv_lcp_ms"),
        "cwv_cls": ps.get("cwv_cls"),
        "cwv_tbt_ms": ps.get("cwv_tbt_ms"),
        "has_contact_cta": has_cta,
        "has_contact_form": has_form,
        "tech_health_score": tech_health,
        "warnings": warnings,
        "error_codes": error_codes,
        "network_error_count": network_error_count,
        "enriched_email": enrichment.email,
        "enriched_address": enrichment.address,
        "enriched_contact_source_url": enrichment.source_url,
        "enrichment_pages_checked": enrichment.pages_checked,
    }
