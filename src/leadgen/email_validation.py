from __future__ import annotations

import os
import re
from urllib.parse import unquote
from dataclasses import dataclass


EMAIL_RX = re.compile(r"^[A-Za-z0-9._%+\-']+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,24}$")
BAD_TLDS = {
    "png",
    "jpg",
    "jpeg",
    "gif",
    "webp",
    "svg",
    "bmp",
    "ico",
    "pdf",
    "css",
    "js",
    "json",
    "xml",
    "txt",
    "zip",
    "rar",
    "mp3",
    "mp4",
}

BLOCKED_DOMAINS = {
    "sentry.io",
}

PLACEHOLDER_EMAILS = {
    "example@domain.com",
    "example@server.co.uk",
    "exemplo@gmail.com",
    "ejemplo@domain.com",
    "test@example.com",
}

BLOCKED_LOCAL_TOKENS = {
    "ajax-loader",
    "loader",
    "logo",
    "banner",
    "thumbnail",
    "noreply",
    "no-reply",
    "postmaster",
    "mailer-daemon",
}


@dataclass(frozen=True)
class EmailValidationResult:
    email: str
    valid_format: bool
    mx_ok: bool
    validation_status: str
    reason: str
    mx_cache_hit: bool


def normalize_email(email: str) -> str:
    v = unquote(str(email or "")).strip().lower()
    v = v.replace("mailto:", "").strip()
    v = v.strip("<>\"'`;, ")
    v = v.lstrip(".")
    v = re.sub(r"\s+", "", v)
    if "%" in v:
        v = v.replace("%", "")
    return v


def is_valid_email_candidate(email: str) -> bool:
    raw_input = str(email or "")
    if "%" in raw_input or " " in raw_input:
        return False
    v = normalize_email(raw_input)
    if not v or "@" not in v:
        return False
    if not EMAIL_RX.match(v):
        return False
    if any(ch in v for ch in ["/", "\\", "?", "#", " "]):
        return False
    local, _, domain = v.partition("@")
    if not local or not domain or "." not in domain:
        return False
    if v in PLACEHOLDER_EMAILS:
        return False
    if domain in BLOCKED_DOMAINS:
        return False
    if any(tok in local for tok in BLOCKED_LOCAL_TOKENS):
        return False
    if "@2x" in local:
        return False
    if local.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".bmp", ".ico")):
        return False
    if ".." in v or local.startswith(".") or local.endswith(".") or domain.startswith(".") or domain.endswith("."):
        return False
    tld = domain.rsplit(".", 1)[-1].lower()
    if tld in BAD_TLDS:
        return False
    return True


def _resolve_mx(domain: str) -> bool:
    try:
        import dns.resolver  # type: ignore
    except Exception:
        # Se dnspython indisponível, assume True para não bloquear pipeline.
        return True
    try:
        ans = dns.resolver.resolve(domain, "MX")
        return len(list(ans)) > 0
    except Exception:
        return False


def validate_email(email: str, store=None) -> EmailValidationResult:
    raw = normalize_email(email)
    if not is_valid_email_candidate(raw):
        return EmailValidationResult(
            email=raw,
            valid_format=False,
            mx_ok=False,
            validation_status="invalid_format",
            reason="invalid_format",
            mx_cache_hit=False,
        )
    domain = raw.split("@", 1)[-1].strip().lower()
    mx_enabled = os.getenv("LEADGEN_EMAIL_MX_VALIDATION_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
    ttl = int(os.getenv("LEADGEN_MX_CACHE_TTL_SECONDS", "86400") or "86400")
    if not mx_enabled:
        return EmailValidationResult(
            email=raw,
            valid_format=True,
            mx_ok=True,
            validation_status="valid",
            reason="mx_disabled",
            mx_cache_hit=False,
        )

    cache_hit = False
    mx_ok: bool
    if store is not None:
        cached = store.get_domain_mx_cache(domain, ttl_seconds=ttl)
        if cached is not None:
            cache_hit = True
            mx_ok = bool(cached.get("mx_ok", 0))
        else:
            mx_ok = _resolve_mx(domain)
            store.upsert_domain_mx_cache(domain, mx_ok=mx_ok, ttl_seconds=ttl)
    else:
        mx_ok = _resolve_mx(domain)
    status = "valid" if mx_ok else "invalid_mx"
    return EmailValidationResult(
        email=raw,
        valid_format=True,
        mx_ok=mx_ok,
        validation_status=status,
        reason=status,
        mx_cache_hit=cache_hit,
    )
