from __future__ import annotations

import random
import re
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from urllib.parse import parse_qs, quote_plus, urlparse
from urllib.request import Request, urlopen


EMAIL_RE = re.compile(r"[A-Z0-9._%+\-']+@[A-Z0-9.\-]+\.[A-Z]{2,24}", re.IGNORECASE)
LINK_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)


@dataclass(frozen=True)
class ContactCandidate:
    email: str
    source_type: str  # directory | council | website
    source_name: str
    source_url: str
    confidence: float


@dataclass(frozen=True)
class SourceAdapter:
    source_type: str
    source_name: str
    allowed_domains: tuple[str, ...]
    confidence: float


class SourceCircuitBreaker:
    def __init__(self) -> None:
        self._failures: dict[str, deque[datetime]] = {}
        self._paused_until: dict[str, datetime] = {}

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def is_paused(self, source_name: str) -> bool:
        until = self._paused_until.get(source_name)
        return bool(until and self._now() < until)

    def register_failure(self, source_name: str) -> None:
        now = self._now()
        q = self._failures.setdefault(source_name, deque())
        q.append(now)
        window_start = now - timedelta(minutes=15)
        while q and q[0] < window_start:
            q.popleft()
        if len(q) >= 5:
            self._paused_until[source_name] = now + timedelta(minutes=30)

    def register_success(self, source_name: str) -> None:
        self._failures.pop(source_name, None)


_BREAKER = SourceCircuitBreaker()


def _country_adapters(country_code: str) -> list[SourceAdapter]:
    cc = (country_code or "").strip().upper()
    if cc == "BR":
        return [
            SourceAdapter("directory", "guiamais", ("guiamais.com.br",), 0.62),
            SourceAdapter("directory", "apontador", ("apontador.com.br",), 0.62),
            SourceAdapter("council", "oab", ("oab.org.br",), 0.82),
            SourceAdapter("council", "crc", ("crc.org.br",), 0.82),
            SourceAdapter("council", "cau", ("cau.br",), 0.80),
        ]
    if cc == "PT":
        return [
            SourceAdapter("directory", "amarelas_pt", ("amarelas.pt",), 0.64),
            SourceAdapter("directory", "sapo_empresas", ("empresas.sapo.pt",), 0.62),
            SourceAdapter("council", "ordem_advogados_pt", ("oa.pt",), 0.82),
            SourceAdapter("council", "occ", ("occ.pt",), 0.80),
        ]
    if cc == "UK":
        return [
            SourceAdapter("directory", "yell", ("yell.com",), 0.65),
            SourceAdapter("directory", "192", ("192.com",), 0.62),
            SourceAdapter("council", "law_society", ("solicitors.lawsociety.org.uk", "lawsociety.org.uk"), 0.84),
            SourceAdapter("council", "icaew", ("icaew.com",), 0.80),
        ]
    if cc == "ES":
        return [
            SourceAdapter("directory", "paginas_amarillas", ("paginasamarillas.es",), 0.65),
            SourceAdapter("directory", "cylex_es", ("cylex.es",), 0.62),
            SourceAdapter("council", "abogacia", ("abogacia.es",), 0.84),
            SourceAdapter("council", "economistas", ("economistas.es",), 0.80),
        ]
    if cc == "US":
        return [
            SourceAdapter("directory", "yelp", ("yelp.com",), 0.66),
            SourceAdapter("directory", "bbb", ("bbb.org",), 0.66),
            SourceAdapter("council", "bar_association", ("americanbar.org", "findlaw.com"), 0.80),
            SourceAdapter("council", "aicpa", ("aicpa-cima.com",), 0.80),
        ]
    return []


def _fetch_text(url: str, timeout_seconds: int = 8) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 LeadGenerator/1.0",
            "Accept": "text/html,application/xhtml+xml",
        },
    )
    with urlopen(req, timeout=max(2, timeout_seconds)) as res:
        return res.read().decode("utf-8", errors="ignore")


def _extract_links_from_duckduckgo(html: str) -> list[str]:
    out: list[str] = []
    for m in LINK_RE.finditer(html or ""):
        href = (m.group(1) or "").strip()
        if not href:
            continue
        if "duckduckgo.com/l/?" in href:
            parsed = urlparse(href)
            target = parse_qs(parsed.query).get("uddg", [""])[0]
            if target:
                out.append(target)
            continue
        if href.startswith("http"):
            out.append(href)
    dedup: list[str] = []
    seen: set[str] = set()
    for it in out:
        if it in seen:
            continue
        seen.add(it)
        dedup.append(it)
    return dedup


def _domain_allowed(url: str, allowed_domains: Iterable[str]) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return False
    for dom in allowed_domains:
        d = dom.lower()
        if host == d or host.endswith(f".{d}"):
            return True
    return False


def _query_text(lead: dict, niche: str, country_code: str) -> str:
    name = str(lead.get("name") or lead.get("business_name") or "").strip()
    address = str(lead.get("address") or "").strip()
    location = address.split(",")[-2].strip() if "," in address else address
    parts = [name, niche or "", location or "", country_code, "contact email"]
    return " ".join([p for p in parts if p]).strip()


def fetch_contacts_for_lead(
    lead: dict,
    niche: str,
    country_code: str,
    max_candidates: int = 5,
    timeout_seconds: int = 12,
) -> list[ContactCandidate]:
    adapters = _country_adapters(country_code)
    if not adapters:
        return []

    out: list[ContactCandidate] = []
    query = _query_text(lead, niche, country_code)
    for adapter in adapters:
        if len(out) >= max_candidates:
            break
        if _BREAKER.is_paused(adapter.source_name):
            continue

        # Jitter anti-ban.
        time.sleep(random.uniform(0.3, 0.9))
        try:
            search_url = f"https://duckduckgo.com/html/?q={quote_plus(f'site:{adapter.allowed_domains[0]} {query}')}"
            search_html = _fetch_text(search_url, timeout_seconds=timeout_seconds)
            links = _extract_links_from_duckduckgo(search_html)
            links = [u for u in links if _domain_allowed(u, adapter.allowed_domains)][:2]
            found = 0
            for link in links:
                if len(out) >= max_candidates:
                    break
                page_html = _fetch_text(link, timeout_seconds=timeout_seconds)
                emails = sorted({e.lower() for e in EMAIL_RE.findall(page_html)})
                for email in emails[:2]:
                    out.append(
                        ContactCandidate(
                            email=email,
                            source_type=adapter.source_type,
                            source_name=adapter.source_name,
                            source_url=link,
                            confidence=adapter.confidence,
                        )
                    )
                    found += 1
                    if len(out) >= max_candidates:
                        break
            if found > 0:
                _BREAKER.register_success(adapter.source_name)
            else:
                _BREAKER.register_failure(adapter.source_name)
        except Exception:
            _BREAKER.register_failure(adapter.source_name)
            continue

    dedup: list[ContactCandidate] = []
    seen_email: set[str] = set()
    for item in out:
        if item.email in seen_email:
            continue
        seen_email.add(item.email)
        dedup.append(item)
    return dedup[:max_candidates]

