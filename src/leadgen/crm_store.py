from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from .time_utils import UTC


@dataclass
class Lead:
    id: int
    run_id: str
    business_name: str
    maps_url: str
    phone: str
    email: str
    website: str
    address: str
    stage: str
    channel_preferred: str
    opt_out: int


class CrmStore:
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
                CREATE TABLE IF NOT EXISTS leads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    business_name TEXT NOT NULL,
                    maps_url TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    email TEXT NOT NULL,
                    website TEXT NOT NULL,
                    address TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    channel_preferred TEXT NOT NULL,
                    opt_out INTEGER NOT NULL DEFAULT 0,
                    consent_accepted INTEGER NOT NULL DEFAULT 0,
                    preview_url TEXT NOT NULL DEFAULT '',
                    payment_url TEXT NOT NULL DEFAULT '',
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    UNIQUE(maps_url)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS touches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    intent TEXT NOT NULL,
                    template_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    provider_message_id TEXT NOT NULL,
                    body TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    body TEXT NOT NULL,
                    classification TEXT NOT NULL,
                    confidence REAL NOT NULL,
                    timestamp_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS opt_outs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_hash TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    UNIQUE(contact_hash, channel)
                )
                """
            )
            conn.commit()

    def upsert_lead_from_row(self, run_id: str, row: dict) -> int:
        now = datetime.now(UTC).isoformat()
        name = str(row.get("name", "")).strip()
        phone = str(row.get("phone", "")).strip()
        email = str(row.get("website_emails", "")).split(",")[0].strip()
        website = str(row.get("website", "")).strip()
        maps_url = str(row.get("maps_url", "")).strip()
        address = str(row.get("address", "")).strip()

        preferred = "EMAIL" if email else ("WHATSAPP" if phone else "NONE")
        stage = "NEW"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO leads (
                    run_id, business_name, maps_url, phone, email, website, address,
                    stage, channel_preferred, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(maps_url) DO UPDATE SET
                    run_id=excluded.run_id,
                    business_name=excluded.business_name,
                    phone=excluded.phone,
                    email=excluded.email,
                    website=excluded.website,
                    address=excluded.address,
                    channel_preferred=excluded.channel_preferred,
                    updated_at_utc=excluded.updated_at_utc
                """,
                (run_id, name, maps_url, phone, email, website, address, stage, preferred, now, now),
            )
            row_db = conn.execute("SELECT id FROM leads WHERE maps_url = ?", (maps_url,)).fetchone()
            conn.commit()
        if not row_db:
            raise RuntimeError("failed to upsert lead")
        return int(row_db[0])

    def count_leads(self) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT COUNT(*) FROM leads").fetchone()
        return int(row[0]) if row else 0

    def list_leads_for_initial_contact(self, limit: int = 100) -> list[Lead]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, business_name, maps_url, phone, email, website, address, stage, channel_preferred, opt_out
                FROM leads
                WHERE stage IN ('NEW', 'QUALIFIED') AND opt_out = 0 AND channel_preferred IN ('EMAIL', 'WHATSAPP')
                ORDER BY id ASC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [Lead(*self._normalize_lead_row(row)) for row in rows]

    def list_leads_for_offer(self, limit: int = 100) -> list[Lead]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, business_name, maps_url, phone, email, website, address, stage, channel_preferred, opt_out
                FROM leads
                WHERE stage = 'CONSENTED' AND opt_out = 0
                ORDER BY id ASC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [Lead(*self._normalize_lead_row(row)) for row in rows]

    def list_leads_waiting_reply(self, limit: int = 100) -> list[Lead]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, business_name, maps_url, phone, email, website, address, stage, channel_preferred, opt_out
                FROM leads
                WHERE stage = 'WAITING_REPLY' AND opt_out = 0
                ORDER BY id ASC LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [Lead(*self._normalize_lead_row(row)) for row in rows]

    def update_stage(self, lead_id: int, stage: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute("UPDATE leads SET stage=?, updated_at_utc=? WHERE id=?", (stage, now, lead_id))
            conn.commit()

    def set_consent(self, lead_id: int, accepted: bool) -> None:
        now = datetime.now(UTC).isoformat()
        stage = "CONSENTED" if accepted else "WAITING_REPLY"
        with self._connect() as conn:
            conn.execute(
                "UPDATE leads SET consent_accepted=?, stage=?, updated_at_utc=? WHERE id=?",
                (1 if accepted else 0, stage, now, lead_id),
            )
            conn.commit()

    def set_preview_and_payment(self, lead_id: int, preview_url: str, payment_url: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE leads SET preview_url=?, payment_url=?, stage='DEMO_PUBLISHED', updated_at_utc=? WHERE id=?",
                (preview_url, payment_url, now, lead_id),
            )
            conn.commit()

    def get_contact(self, lead_id: int) -> tuple[str, str]:
        with self._connect() as conn:
            row = conn.execute("SELECT email, phone FROM leads WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return "", ""
        return str(row[0] or ""), str(row[1] or "")

    def get_lead_id_by_email(self, email: str) -> int | None:
        if not email:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id FROM leads WHERE lower(email)=lower(?) ORDER BY id DESC LIMIT 1",
                (email.strip(),),
            ).fetchone()
        if not row:
            return None
        return int(row[0])

    def get_preview_and_payment(self, lead_id: int) -> tuple[str, str]:
        with self._connect() as conn:
            row = conn.execute("SELECT preview_url, payment_url FROM leads WHERE id=?", (lead_id,)).fetchone()
        if not row:
            return "", ""
        return str(row[0] or ""), str(row[1] or "")

    def save_touch(
        self,
        lead_id: int,
        channel: str,
        intent: str,
        template_id: str,
        status: str,
        provider_message_id: str,
        body: str,
    ) -> None:
        ts = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO touches (lead_id, channel, intent, template_id, status, provider_message_id, body, timestamp_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (lead_id, channel, intent, template_id, status, provider_message_id, body, ts),
            )
            conn.commit()

    def count_touches(self, lead_id: int, intent: str = "CONSENT_REQUEST") -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM touches WHERE lead_id=? AND intent=?",
                (lead_id, intent),
            ).fetchone()
        return int(row[0]) if row else 0

    def save_reply(self, lead_id: int, channel: str, body: str, classification: str, confidence: float) -> None:
        ts = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO replies (lead_id, channel, body, classification, confidence, timestamp_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (lead_id, channel, body, classification, confidence, ts),
            )
            conn.commit()

    def register_opt_out(self, contact: str, channel: str, reason: str) -> None:
        ts = datetime.now(UTC).isoformat()
        contact_hash = hashlib.sha256(contact.encode("utf-8")).hexdigest()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO opt_outs (contact_hash, channel, reason, timestamp_utc) VALUES (?, ?, ?, ?)",
                (contact_hash, channel, reason, ts),
            )
            conn.execute(
                "UPDATE leads SET opt_out=1, stage='UNSUBSCRIBED', updated_at_utc=? WHERE email=? OR phone=?",
                (ts, contact, contact),
            )
            conn.commit()

    def is_opted_out(self, contact: str, channel: str) -> bool:
        contact_hash = hashlib.sha256(contact.encode("utf-8")).hexdigest()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM opt_outs WHERE contact_hash=? AND channel=?",
                (contact_hash, channel),
            ).fetchone()
        return row is not None

    @staticmethod
    def _normalize_lead_row(row: tuple) -> tuple:
        return (
            int(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            str(row[5]),
            str(row[6]),
            str(row[7]),
            str(row[8]),
            str(row[9]),
            int(row[10]),
        )
