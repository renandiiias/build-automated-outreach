from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .logging_utils import JsonlLogger

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")


@dataclass
class EnrichmentResult:
    provider: str
    html: str


def enrich_with_website_contacts(rows: list[dict], logger: JsonlLogger, run_id: str) -> list[dict]:
    enriched: list[dict] = []
    for row in rows:
        website = str(row.get("website", "")).strip()
        if not _is_valid_http_url(website):
            row["website_emails"] = ""
            row["website_phones"] = ""
            row["enrichment_provider"] = ""
            enriched.append(row)
            continue

        try:
            fetched = _fetch_website_html(website)
            emails = sorted(set(EMAIL_RE.findall(fetched.html)))
            phones = sorted(set(PHONE_RE.findall(fetched.html)))
            row["website_emails"] = ", ".join(emails[:10])
            row["website_phones"] = ", ".join(phones[:10])
            row["enrichment_provider"] = fetched.provider
            logger.write(
                "lead_enriched",
                {
                    "run_id": run_id,
                    "website": website,
                    "provider": fetched.provider,
                    "emails_found": len(emails),
                    "phones_found": len(phones),
                },
            )
        except Exception as exc:
            row["website_emails"] = ""
            row["website_phones"] = ""
            row["enrichment_provider"] = ""
            logger.write(
                "lead_enrichment_failed",
                {
                    "run_id": run_id,
                    "website": website,
                    "error_type": exc.__class__.__name__,
                    "message": str(exc),
                },
            )

        enriched.append(row)
    return enriched


def _fetch_website_html(url: str) -> EnrichmentResult:
    # Preferred path: Scrapling fetchers, when available (Python 3.10+ and extra deps installed).
    try:
        from scrapling.fetchers import Fetcher  # type: ignore

        page = Fetcher.get(url)
        html = _extract_html_from_page(page)
        if html:
            return EnrichmentResult(provider="scrapling_fetcher", html=html)
    except Exception:
        pass

    # Fallback path: standard HTTP request.
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 LeadGenerator/1.0"})
    try:
        with urlopen(req, timeout=20) as res:
            content = res.read()
        return EnrichmentResult(provider="urllib", html=content.decode("utf-8", errors="ignore"))
    except URLError as exc:
        raise RuntimeError(f"failed to fetch website: {url}") from exc


def _extract_html_from_page(page: object) -> str:
    for attr in ("text", "content", "html"):
        value = getattr(page, attr, None)
        if isinstance(value, str) and value.strip():
            return value
    to_str = str(page)
    return to_str if to_str and "<" in to_str else ""


def _is_valid_http_url(url: str) -> bool:
    if not url:
        return False
    parts = urlparse(url)
    return parts.scheme in {"http", "https"} and bool(parts.netloc)
