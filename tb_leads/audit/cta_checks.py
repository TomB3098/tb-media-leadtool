from __future__ import annotations


def detect_contact_signals(html: str) -> tuple[bool, bool]:
    h = (html or "").lower()
    has_form = "<form" in h
    has_cta = any(
        token in h
        for token in [
            "kontakt",
            "termin",
            "anfrage",
            "jetzt anrufen",
            "kostenloses erstgespräch",
            "get in touch",
            "contact",
        ]
    )
    return has_cta, has_form
