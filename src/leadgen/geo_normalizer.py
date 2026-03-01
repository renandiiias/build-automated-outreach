from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def _cache_db_path() -> Path:
    raw = os.getenv("LEADGEN_GEO_CACHE_DB", "logs/geo_cache.db")
    return Path(raw).expanduser().resolve()


def _ensure_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _is_postal_or_noise(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return True
    patterns = [
        re.compile(r"^\d{5}-?\d{3}$"),  # BR
        re.compile(r"^\d{5}(?:-\d{4})?$"),  # US
        re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}$", re.I),  # UK
    ]
    return any(rx.match(v) for rx in patterns)


def _local_city_guess(raw: str) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    parts = [p.strip() for p in text.split(",") if p.strip()]
    if not parts:
        return ""

    country_words = {
        "brazil",
        "brasil",
        "portugal",
        "united kingdom",
        "england",
        "usa",
        "united states",
        "spain",
        "espana",
        "españa",
    }
    parts = [p for p in parts if p.lower() not in country_words] or parts
    street_tokens = {
        "rua",
        "av",
        "avenida",
        "travessa",
        "alameda",
        "rodovia",
        "estrada",
        "street",
        "st",
        "road",
        "rd",
        "avenue",
        "ave",
        "blvd",
        "drive",
        "dr",
        "calle",
        "carrer",
        "plaza",
        "piazza",
    }
    best = ""
    best_score = -999
    for idx, part in enumerate(parts):
        p = part.strip()
        low = p.lower()
        letters = len(re.findall(r"[a-zA-ZÀ-ÿ]", p))
        digits = len(re.findall(r"\d", p))
        score = 0
        if letters > 0:
            score += 4
        if digits > 0:
            score -= 2
        if digits >= letters and digits > 0:
            score -= 3
        if _is_postal_or_noise(p):
            score -= 7
        if any(tok in low for tok in street_tokens):
            score -= 3
        # Prefer middle/end tokens over first street token.
        score += min(idx, 2)
        if score > best_score:
            best_score = score
            best = p
    return "" if _is_postal_or_noise(best) else best


def _geocode_city(raw: str) -> str:
    q = (raw or "").strip()
    if not q:
        return ""
    params = urlencode({"q": q, "format": "jsonv2", "addressdetails": 1, "limit": 1})
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "leadgen-geonormalizer/1.0 (contact: ops@renandias.site)",
        },
    )
    with urlopen(req, timeout=6) as res:
        payload = json.loads(res.read().decode("utf-8", errors="ignore") or "[]")
    if not payload:
        return ""
    item = payload[0] or {}
    address = item.get("address") or {}
    for key in ("city", "town", "village", "municipality", "county", "state_district"):
        val = (address.get(key) or "").strip()
        if val:
            return val
    display = (item.get("display_name") or "").split(",")
    for token in display:
        t = token.strip()
        if t and not _is_postal_or_noise(t):
            return t
    return ""


class GeoCityNormalizer:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or _cache_db_path()
        _ensure_dir(self.db_path)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS geo_city_cache (
                  key TEXT PRIMARY KEY,
                  city TEXT NOT NULL,
                  source TEXT NOT NULL,
                  updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def _key(self, raw: str, locale: str) -> str:
        return f"{(locale or '').strip().lower()}::{(raw or '').strip().lower()}"

    def normalize_city(self, raw: str, locale: str = "en") -> tuple[str, str]:
        key = self._key(raw, locale)
        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute("SELECT city, source FROM geo_city_cache WHERE key=?", (key,)).fetchone()
                if row and str(row[0]).strip():
                    return str(row[0]).strip(), str(row[1] or "cache")

        city = _local_city_guess(raw)
        source = "heuristic"
        if not city and os.getenv("LEADGEN_GEOCODE_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}:
            try:
                city = _geocode_city(raw)
                source = "geocode" if city else source
            except Exception:
                city = city or ""
        if not city:
            city = "your city"
            if str(locale or "").lower().startswith("pt"):
                city = "sua cidade"
            elif str(locale or "").lower().startswith("es"):
                city = "tu ciudad"
            source = "fallback"

        with self._lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    "INSERT OR REPLACE INTO geo_city_cache(key, city, source, updated_at_utc) VALUES (?, ?, ?, datetime('now'))",
                    (key, city, source),
                )
                conn.commit()
        return city, source

