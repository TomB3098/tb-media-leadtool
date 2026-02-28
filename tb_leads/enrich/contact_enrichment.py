from __future__ import annotations

import html
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

PRIVATE_EMAIL_DOMAINS = {
    "gmail.com",
    "gmx.de",
    "hotmail.com",
    "outlook.com",
    "yahoo.com",
    "web.de",
}

COMMON_CONTACT_PATHS = [
    "",
    "/impressum",
    "/kontakt",
    "/contact",
    "/imprint",
    "/legal",
    "/about",
    "/ueber-uns",
]

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


@dataclass
class ContactEnrichmentResult:
    email: str | None
    address: str | None
    source_url: str | None
    pages_checked: int
    warnings: list[str]


def _clean_email(value: str) -> str:
    return value.strip().strip(".,;:()[]<>").lower()


def _is_valid_email(value: str) -> bool:
    if not value or len(value) > 254:
        return False
    if "@" not in value:
        return False

    local, _, domain = value.rpartition("@")
    domain = domain.lower().strip()

    if not local or not domain:
        return False
    if "." not in domain:
        return False

    # Filter known placeholders / non-routable testing domains
    if domain.endswith(".invalid"):
        return False
    if domain in {"example.com", "example.org", "example.net", "example.invalid"}:
        return False

    return True


def _score_email(candidate: str, website_domain: str | None) -> int:
    score = 0
    local, _, domain = candidate.partition("@")
    local = local.lower()
    domain = domain.lower()

    if website_domain and domain in website_domain:
        score += 6
    if local.startswith(("info", "kontakt", "contact", "office", "service")):
        score += 4
    if domain in PRIVATE_EMAIL_DOMAINS:
        score -= 3
    if "noreply" in local:
        score -= 2
    return score


def _normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_html_to_text(raw_html: str) -> str:
    content = re.sub(r"<script\b[^>]*>[\s\S]*?</script>", " ", raw_html, flags=re.IGNORECASE)
    content = re.sub(r"<style\b[^>]*>[\s\S]*?</style>", " ", content, flags=re.IGNORECASE)
    content = re.sub(r"<br\s*/?>", "\n", content, flags=re.IGNORECASE)
    content = re.sub(r"</p>|</div>|</li>|</h\d>", "\n", content, flags=re.IGNORECASE)
    content = re.sub(r"<[^>]+>", " ", content)
    content = html.unescape(content)
    return content


def _extract_emails(text: str) -> list[str]:
    found = [_clean_email(x) for x in EMAIL_RE.findall(text or "")]
    unique: list[str] = []
    seen = set()
    for item in found:
        if item in seen:
            continue
        if not _is_valid_email(item):
            continue
        seen.add(item)
        unique.append(item)
    return unique


def _clean_city(city: str) -> str:
    tokens = _normalize_ws(city).split()
    stopwords = {"impressum", "kontakt", "jetzt", "telefon", "email", "e-mail", "mail"}
    cleaned: list[str] = []
    for t in tokens:
        if t.lower() in stopwords:
            break
        cleaned.append(t)
        if len(cleaned) >= 3:
            break
    return " ".join(cleaned) if cleaned else _normalize_ws(city)


def _extract_addresses(text: str) -> list[str]:
    candidates: list[str] = []

    # 1-line pattern: street (with typical German suffix) + house number + zip + city
    pattern_one_line = re.compile(
        r"((?:[A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-]+ ?)+(?:straße|str\.|weg|platz|allee|ring|gasse|ufer|damm|chaussee) +\d+[a-zA-Z]?)\s*,?\s*(\d{5}) +([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\- ]{2,})",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    for m in pattern_one_line.finditer(text):
        street = _normalize_ws(m.group(1))
        postal = m.group(2)
        city = _clean_city(m.group(3))
        candidates.append(f"{street}, {postal} {city}")

    # 2-line pattern: previous line street, current line zip city
    lines = [_normalize_ws(line) for line in text.splitlines() if _normalize_ws(line)]
    zip_city_re = re.compile(r"^(\d{5})\s+([A-ZÄÖÜ][A-Za-zÄÖÜäöüß\-\s]{2,})$")
    street_hint_re = re.compile(r"(straße|str\.|weg|platz|allee|ring|gasse|ufer|damm|chaussee).*(\d+[a-zA-Z]?)", re.IGNORECASE)
    for idx, line in enumerate(lines):
        m = zip_city_re.match(line)
        if not m or idx == 0:
            continue
        prev = lines[idx - 1]
        if street_hint_re.search(prev):
            candidates.append(f"{prev}, {m.group(1)} {_clean_city(m.group(2))}")

    # Deduplicate while preserving order
    deduped: list[str] = []
    seen = set()
    for c in candidates:
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)
    return deduped


def _fetch_html(url: str, timeout: float = 8.0) -> str | None:
    req = urllib.request.Request(url, headers={"User-Agent": "tb-leads/0.2"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read(350_000)
            return body.decode("utf-8", errors="ignore")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError):
        return None
    except Exception:
        return None


def _candidate_urls(website_url: str) -> list[str]:
    parsed = urlparse(website_url)
    if not parsed.scheme:
        website_url = f"https://{website_url}"
        parsed = urlparse(website_url)

    base = f"{parsed.scheme}://{parsed.netloc}"
    out = []
    seen = set()

    for path in COMMON_CONTACT_PATHS:
        if path in ("", "/"):
            candidate = website_url
        else:
            candidate = urljoin(base, path)
        if candidate in seen:
            continue
        seen.add(candidate)
        out.append(candidate)

    return out


def enrich_contact_data(website_url: str | None, max_pages: int = 4) -> ContactEnrichmentResult:
    if not website_url:
        return ContactEnrichmentResult(email=None, address=None, source_url=None, pages_checked=0, warnings=["no_website"])

    parsed = urlparse(website_url if "//" in website_url else f"https://{website_url}")
    website_domain = parsed.netloc.lower()

    emails_scored: list[tuple[int, str, str]] = []  # score, email, source_url
    addresses: list[tuple[str, str]] = []  # address, source_url
    warnings: list[str] = []
    pages_checked = 0

    for url in _candidate_urls(website_url)[:max_pages]:
        html_doc = _fetch_html(url)
        if not html_doc:
            warnings.append(f"fetch_failed:{url}")
            continue

        pages_checked += 1
        text = _strip_html_to_text(html_doc)

        for email in _extract_emails(text):
            emails_scored.append((_score_email(email, website_domain), email, url))

        found_addresses = _extract_addresses(text)
        for addr in found_addresses:
            addresses.append((addr, url))

    best_email = None
    best_email_source = None
    if emails_scored:
        emails_scored.sort(key=lambda x: (x[0], len(x[1])), reverse=True)
        best_email = emails_scored[0][1]
        best_email_source = emails_scored[0][2]

    best_address = None
    best_address_source = None
    if addresses:
        best_address = addresses[0][0]
        best_address_source = addresses[0][1]

    source_url = best_email_source or best_address_source

    return ContactEnrichmentResult(
        email=best_email,
        address=best_address,
        source_url=source_url,
        pages_checked=pages_checked,
        warnings=warnings,
    )
