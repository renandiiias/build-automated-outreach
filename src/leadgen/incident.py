from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .config import IncidentPolicy


@dataclass
class IncidentState:
    fingerprint: str
    count_window: int
    level: str
    should_generate_report: bool


class IncidentEngine:
    def __init__(self, db_path: Path, policy: IncidentPolicy, incident_dir: Path) -> None:
        self.db_path = db_path
        self.policy = policy
        self.incident_dir = incident_dir
        self.incident_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fingerprint TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    error_type TEXT NOT NULL,
                    message TEXT NOT NULL
                )
                """
            )
            conn.commit()

    @staticmethod
    def fingerprint(error_type: str, message: str, stack: str, context: dict[str, Any]) -> str:
        base = f"{error_type}|{message}|{stack}|{context}"
        return hashlib.sha256(base.encode("utf-8")).hexdigest()[:20]

    def register(self, fingerprint: str, error_type: str, message: str) -> IncidentState:
        now = datetime.now(UTC)
        window_start = now - timedelta(minutes=self.policy.window_min)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO events (fingerprint, timestamp_utc, error_type, message) VALUES (?, ?, ?, ?)",
                (fingerprint, now.isoformat(), error_type, message),
            )
            conn.execute(
                "DELETE FROM events WHERE timestamp_utc < ?",
                (window_start.isoformat(),),
            )
            row = conn.execute(
                "SELECT COUNT(*) FROM events WHERE fingerprint = ? AND timestamp_utc >= ?",
                (fingerprint, window_start.isoformat()),
            ).fetchone()
            conn.commit()

        count = int(row[0]) if row else 1
        level = "L0"
        if count >= self.policy.l3:
            level = "L3"
        elif count >= self.policy.l2:
            level = "L2"
        elif count >= self.policy.l1:
            level = "L1"

        return IncidentState(
            fingerprint=fingerprint,
            count_window=count,
            level=level,
            should_generate_report=level in {"L2", "L3"},
        )

    def write_report(
        self,
        state: IncidentState,
        error_type: str,
        message: str,
        context: dict[str, Any],
        attempts: list[str],
        impact: str,
        hypothesis: str,
        next_steps: list[str],
        status: str,
    ) -> Path:
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        file_path = self.incident_dir / f"incident-{state.fingerprint}-{ts}.md"
        content = [
            f"# Incident {state.fingerprint}",
            "",
            f"- horario_utc: {datetime.now(UTC).isoformat()}",
            f"- fingerprint: {state.fingerprint}",
            f"- frequencia_janela_15m: {state.count_window}",
            f"- nivel: {state.level}",
            f"- error_type: {error_type}",
            f"- message: {message}",
            f"- impacto: {impact}",
            f"- hipotese: {hypothesis}",
            f"- status: {status}",
            "",
            "## Contexto",
            str(context),
            "",
            "## Tentativas feitas",
            *[f"- {item}" for item in attempts],
            "",
            "## Proximos passos",
            *[f"- {item}" for item in next_steps],
            "",
        ]
        file_path.write_text("\n".join(content), encoding="utf-8")
        return file_path
