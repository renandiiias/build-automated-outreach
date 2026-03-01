from __future__ import annotations

import os
import re
from dataclasses import asdict
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .contact_sources import ContactCandidate, fetch_contacts_for_lead
from .email_validation import validate_email
from .logging_utils import JsonlLogger

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")


@dataclass
class EnrichmentResult:
    provider: str
    html: str


def enrich_with_website_contacts(rows: list[dict], logger: JsonlLogger, run_id: str, store=None) -> list[dict]:
    enriched: list[dict] = []
    external_enabled = os.getenv("LEADGEN_EXTERNAL_ENRICH_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
    external_max = int(os.getenv("LEADGEN_EXTERNAL_ENRICH_MAX_CANDIDATES", "5") or "5")
    external_timeout = int(os.getenv("LEADGEN_EXTERNAL_ENRICH_TIMEOUT_SECONDS", "12") or "12")
    for row in rows:
        website = str(row.get("website", "")).strip()
        all_candidates: list[ContactCandidate] = []
        if not _is_valid_http_url(website):
            row["website_emails"] = ""
            row["website_phones"] = ""
            row["enrichment_provider"] = ""
        else:
            try:
                fetched = _fetch_website_html(website)
                emails = sorted(set(EMAIL_RE.findall(fetched.html)))
                phones = sorted(set(PHONE_RE.findall(fetched.html)))
                row["website_emails"] = ", ".join(emails[:10])
                row["website_phones"] = ", ".join(phones[:10])
                row["enrichment_provider"] = fetched.provider
                for email in emails[:10]:
                    all_candidates.append(
                        ContactCandidate(
                            email=email.lower(),
                            source_type="website",
                            source_name=fetched.provider,
                            source_url=website,
                            confidence=0.50,
                        )
                    )
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

        country_code = str(row.get("country_code", "") or "").strip().upper()
        niche = str(row.get("audience", "") or "").strip()
        if external_enabled:
            logger.write(
                "lead_contact_source_checked",
                {
                    "run_id": run_id,
                    "business_name": str(row.get("name", "") or "").strip(),
                    "country_code": country_code,
                    "niche": niche,
                    "source_pack": os.getenv("LEADGEN_COUNTRY_SOURCE_PACK", "directory_council"),
                },
            )
            external_candidates = fetch_contacts_for_lead(
                lead=row,
                niche=niche,
                country_code=country_code,
                max_candidates=max(1, external_max),
                timeout_seconds=max(2, external_timeout),
            )
            for cand in external_candidates:
                logger.write(
                    "lead_contact_candidate_found",
                    {
                        "run_id": run_id,
                        "business_name": str(row.get("name", "") or "").strip(),
                        "country_code": country_code,
                        "email": cand.email,
                        "source_type": cand.source_type,
                        "source_name": cand.source_name,
                        "source_url": cand.source_url,
                        "confidence": cand.confidence,
                    },
                )
            all_candidates.extend(external_candidates)

        scored: list[tuple[float, ContactCandidate]] = []
        freemail_domains = {
            "gmail.com",
            "hotmail.com",
            "outlook.com",
            "yahoo.com",
            "icloud.com",
            "live.com",
            "aol.com",
        }
        seen: set[str] = set()
        for cand in all_candidates:
            em = (cand.email or "").strip().lower()
            if not em or em in seen:
                continue
            seen.add(em)
            vr = validate_email(em, store=store)
            if vr.mx_cache_hit:
                logger.write("email_mx_cache_hit", {"run_id": run_id, "email": em})
            logger.write(
                "email_mx_checked",
                {
                    "run_id": run_id,
                    "email": em,
                    "validation_status": vr.validation_status,
                    "mx_ok": vr.mx_ok,
                    "cache_hit": vr.mx_cache_hit,
                },
            )
            row.setdefault("contact_candidates", [])
            cand_payload = asdict(cand)
            cand_payload["validation_status"] = vr.validation_status
            cand_payload["mx_ok"] = 1 if vr.mx_ok else 0
            row["contact_candidates"].append(cand_payload)

            if vr.validation_status != "valid":
                logger.write(
                    "lead_contact_candidate_rejected",
                    {
                        "run_id": run_id,
                        "email": em,
                        "source_name": cand.source_name,
                        "validation_status": vr.validation_status,
                    },
                )
                continue
            domain = em.split("@", 1)[-1]
            source_base = {"council": 0.85, "directory": 0.65, "website": 0.50}.get(cand.source_type, 0.50)
            score = source_base + (0.20 * float(cand.confidence)) + (0.10 if vr.mx_ok else 0.0)
            if domain not in freemail_domains:
                score += 0.05
            scored.append((score, cand))

        scored.sort(key=lambda it: it[0], reverse=True)
        selected_email = scored[0][1].email if scored else ""
        selected_source = scored[0][1].source_name if scored else ""
        if selected_email:
            row["email"] = selected_email
            ordered = [selected_email] + [it[1].email for it in scored[1:]]
            row["website_emails"] = ", ".join(ordered[:10])
            logger.write(
                "lead_contact_selected",
                {
                    "run_id": run_id,
                    "business_name": str(row.get("name", "") or "").strip(),
                    "country_code": country_code,
                    "email": selected_email,
                    "source_name": selected_source,
                    "score": scored[0][0],
                },
            )
        else:
            row["email"] = ""

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
