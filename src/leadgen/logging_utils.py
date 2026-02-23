from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SENSITIVE_KEYS = {
    "token",
    "password",
    "passwd",
    "secret",
    "cookie",
    "authorization",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
}

BEARER_RE = re.compile(r"(?i)bearer\s+[a-z0-9\-._~+/]+=*")
GENERIC_SECRET_RE = re.compile(r"(?i)(token|password|secret|api[_-]?key)\s*[:=]\s*[^\s,;]+")


@dataclass
class JsonlLogger:
    file_path: Path

    def write(self, event_type: str, payload: dict[str, Any]) -> None:
        record = {
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "event_type": event_type,
            "payload": redact(payload),
        }
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        with self.file_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=True) + "\n")


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            key_l = key.lower()
            if any(secret in key_l for secret in SENSITIVE_KEYS):
                out[key] = "[REDACTED]"
            else:
                out[key] = redact(item)
        return out

    if isinstance(value, list):
        return [redact(item) for item in value]

    if isinstance(value, str):
        value = BEARER_RE.sub("Bearer [REDACTED]", value)
        value = GENERIC_SECRET_RE.sub(r"\1=[REDACTED]", value)
        return value

    return value
