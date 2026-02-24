from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from .time_utils import UTC


@dataclass
class ChannelMetrics:
    sent: int
    failed: int
    bounces: int
    complaints: int

    @property
    def bounce_rate(self) -> float:
        return (self.bounces / self.sent) if self.sent else 0.0

    @property
    def complaint_rate(self) -> float:
        return (self.complaints / self.sent) if self.sent else 0.0

    @property
    def fail_rate(self) -> float:
        return (self.failed / self.sent) if self.sent else 0.0


class OperationalState:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS run_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    run_type TEXT NOT NULL,
                    unstable INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_status (
                    channel TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    paused_at_utc TEXT,
                    resumed_at_utc TEXT,
                    cooldown_until_utc TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_metrics_daily (
                    day_utc TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    sent INTEGER NOT NULL DEFAULT 0,
                    failed INTEGER NOT NULL DEFAULT 0,
                    bounces INTEGER NOT NULL DEFAULT 0,
                    complaints INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (day_utc, channel)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS flags (
                    name TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def record_run(self, run_id: str, run_type: str, unstable: bool, reason: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO run_history (run_id, run_type, unstable, reason, timestamp_utc) VALUES (?, ?, ?, ?, ?)",
                (run_id, run_type, 1 if unstable else 0, reason, now),
            )
            conn.commit()

    def unstable_streak(self, run_type: str) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT unstable FROM run_history WHERE run_type = ? ORDER BY id DESC LIMIT 10",
                (run_type,),
            ).fetchall()
        streak = 0
        for (unstable,) in rows:
            if int(unstable) == 1:
                streak += 1
            else:
                break
        return streak

    def set_channel_paused(self, channel: str, reason: str, cooldown_hours: int = 12) -> None:
        now = datetime.now(UTC)
        cooldown = now.timestamp() + (cooldown_hours * 3600)
        cooldown_until = datetime.fromtimestamp(cooldown, UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO channel_status (channel, status, reason, paused_at_utc, resumed_at_utc, cooldown_until_utc)
                VALUES (?, 'PAUSED', ?, ?, NULL, ?)
                ON CONFLICT(channel) DO UPDATE SET
                    status='PAUSED', reason=excluded.reason, paused_at_utc=excluded.paused_at_utc,
                    resumed_at_utc=NULL, cooldown_until_utc=excluded.cooldown_until_utc
                """,
                (channel, reason, now.isoformat(), cooldown_until),
            )
            conn.commit()

    def set_channel_resumed(self, channel: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO channel_status (channel, status, reason, paused_at_utc, resumed_at_utc, cooldown_until_utc)
                VALUES (?, 'ACTIVE', '', NULL, ?, NULL)
                ON CONFLICT(channel) DO UPDATE SET
                    status='ACTIVE', reason='', resumed_at_utc=excluded.resumed_at_utc,
                    cooldown_until_utc=NULL
                """,
                (channel, now),
            )
            conn.commit()

    def is_channel_paused(self, channel: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT status, cooldown_until_utc FROM channel_status WHERE channel = ?",
                (channel,),
            ).fetchone()
        if not row:
            return False
        status, cooldown_until = row
        if status != "PAUSED":
            return False
        if not cooldown_until:
            return True
        paused = datetime.now(UTC) < datetime.fromisoformat(cooldown_until)
        if not paused:
            self.set_channel_resumed(channel)
        return paused

    def count_paused_channels(self, channels: list[str]) -> int:
        if not channels:
            return 0
        placeholders = ",".join("?" for _ in channels)
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM channel_status
                WHERE status='PAUSED'
                  AND channel IN ({placeholders})
                  AND (cooldown_until_utc IS NULL OR cooldown_until_utc > ?)
                """,
                tuple(channels) + (now,),
            ).fetchone()
        return int(row[0]) if row else 0

    def set_global_safe_mode(self, enabled: bool) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO flags (name, value, updated_at_utc)
                VALUES ('GLOBAL_SAFE_MODE', ?, ?)
                ON CONFLICT(name) DO UPDATE SET value=excluded.value, updated_at_utc=excluded.updated_at_utc
                """,
                ("1" if enabled else "0", now),
            )
            conn.commit()

    def global_safe_mode_enabled(self) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM flags WHERE name='GLOBAL_SAFE_MODE'").fetchone()
        return bool(row and row[0] == "1")

    def add_channel_metrics(
        self,
        channel: str,
        sent: int = 0,
        failed: int = 0,
        bounces: int = 0,
        complaints: int = 0,
        day_utc: str | None = None,
    ) -> None:
        day = day_utc or datetime.now(UTC).strftime("%Y-%m-%d")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO channel_metrics_daily (day_utc, channel, sent, failed, bounces, complaints)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(day_utc, channel) DO UPDATE SET
                    sent=sent + excluded.sent,
                    failed=failed + excluded.failed,
                    bounces=bounces + excluded.bounces,
                    complaints=complaints + excluded.complaints
                """,
                (day, channel, sent, failed, bounces, complaints),
            )
            conn.commit()

    def get_channel_metrics(self, channel: str, day_utc: str | None = None) -> ChannelMetrics:
        day = day_utc or datetime.now(UTC).strftime("%Y-%m-%d")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT sent, failed, bounces, complaints FROM channel_metrics_daily WHERE day_utc=? AND channel=?",
                (day, channel),
            ).fetchone()
        if not row:
            return ChannelMetrics(sent=0, failed=0, bounces=0, complaints=0)
        return ChannelMetrics(sent=int(row[0]), failed=int(row[1]), bounces=int(row[2]), complaints=int(row[3]))
