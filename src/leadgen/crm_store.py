from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .time_utils import UTC


def _price_base_full() -> int:
    return int(os.getenv("LEADGEN_PRICE_BASE_FULL", "200") or "200")


def _price_base_simple() -> int:
    return int(os.getenv("LEADGEN_PRICE_BASE_SIMPLE", "100") or "100")


def _price_step() -> int:
    return int(os.getenv("LEADGEN_PRICE_STEP", "100") or "100")


def _price_for_level(level: int) -> tuple[int, int]:
    base_full = _price_base_full()
    base_simple = _price_base_simple()
    step = _price_step()
    return base_full + (level * step), base_simple + (level * step)


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


def _infer_country_code(phone: str, address: str) -> str:
    digits = "".join(ch for ch in (phone or "") if ch.isdigit())
    addr = (address or "").lower()
    if digits.startswith("55") or any(token in addr for token in [" brasil", "brazil", " sao paulo", " rio de janeiro", " belo horizonte", " fortaleza", " recife"]):
        return "BR"
    if digits.startswith("351") or any(token in addr for token in [" portugal", " lisboa", " lisbon", " porto"]):
        return "PT"
    if digits.startswith("44") or any(token in addr for token in [" united kingdom", " london", " manchester", " england"]):
        return "UK"
    if digits.startswith("1") or any(token in addr for token in [" united states", " usa", " miami", " new york", " florida"]):
        return "US"
    return ""


@dataclass
class PricingState:
    price_level: int
    price_full: int
    price_simple: int
    baseline_conversion: float | None
    offers_in_window: int
    sales_in_window: int
    updated_at_utc: str


@dataclass
class ReplyReviewItem:
    id: int
    lead_id: int
    channel: str
    inbound_text: str
    status: str
    intent_final: str
    draft_reply: str
    confidence: float
    created_at_utc: str
    updated_at_utc: str


class CrmStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC)

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
                    audience TEXT NOT NULL DEFAULT '',
                    country_code TEXT NOT NULL DEFAULT '',
                    opt_out INTEGER NOT NULL DEFAULT 0,
                    consent_accepted INTEGER NOT NULL DEFAULT 0,
                    preview_url TEXT NOT NULL DEFAULT '',
                    payment_url TEXT NOT NULL DEFAULT '',
                    sale_amount REAL NOT NULL DEFAULT 0,
                    accepted_plan TEXT NOT NULL DEFAULT '',
                    won_at_utc TEXT NOT NULL DEFAULT '',
                    lost_at_utc TEXT NOT NULL DEFAULT '',
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS contact_send_guard (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    contact_hash TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    intent TEXT NOT NULL,
                    first_lead_id INTEGER NOT NULL,
                    last_lead_id INTEGER NOT NULL,
                    first_sent_at_utc TEXT NOT NULL,
                    last_sent_at_utc TEXT NOT NULL,
                    send_count INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(contact_hash, channel, intent)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_state (
                    id INTEGER PRIMARY KEY,
                    price_level INTEGER NOT NULL,
                    price_full INTEGER NOT NULL,
                    price_simple INTEGER NOT NULL,
                    baseline_conversion REAL,
                    offers_in_window INTEGER NOT NULL DEFAULT 0,
                    sales_in_window INTEGER NOT NULL DEFAULT 0,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pricing_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    from_level INTEGER NOT NULL,
                    to_level INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS offer_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL,
                    run_id TEXT NOT NULL,
                    price_level INTEGER NOT NULL,
                    price_full INTEGER NOT NULL,
                    price_simple INTEGER NOT NULL,
                    offered_at_utc TEXT NOT NULL,
                    converted INTEGER NOT NULL DEFAULT 0,
                    converted_at_utc TEXT NOT NULL DEFAULT ''
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reply_review_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL,
                    channel TEXT NOT NULL,
                    inbound_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    intent_final TEXT NOT NULL DEFAULT '',
                    draft_reply TEXT NOT NULL DEFAULT '',
                    confidence REAL NOT NULL DEFAULT 0,
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS domain_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id INTEGER NOT NULL UNIQUE,
                    domain_name TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    expires_at_utc TEXT NOT NULL DEFAULT '',
                    notes TEXT NOT NULL DEFAULT '',
                    created_at_utc TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS domain_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    domain_job_id INTEGER NOT NULL,
                    days_before INTEGER NOT NULL,
                    created_at_utc TEXT NOT NULL,
                    UNIQUE(domain_job_id, days_before)
                )
                """
            )
            self._migrate_schema(conn)
            conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        lead_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(leads)").fetchall()}
        migrations: list[tuple[str, str]] = [
            ("sale_amount", "ALTER TABLE leads ADD COLUMN sale_amount REAL NOT NULL DEFAULT 0"),
            ("accepted_plan", "ALTER TABLE leads ADD COLUMN accepted_plan TEXT NOT NULL DEFAULT ''"),
            ("won_at_utc", "ALTER TABLE leads ADD COLUMN won_at_utc TEXT NOT NULL DEFAULT ''"),
            ("lost_at_utc", "ALTER TABLE leads ADD COLUMN lost_at_utc TEXT NOT NULL DEFAULT ''"),
            ("audience", "ALTER TABLE leads ADD COLUMN audience TEXT NOT NULL DEFAULT ''"),
            ("country_code", "ALTER TABLE leads ADD COLUMN country_code TEXT NOT NULL DEFAULT ''"),
        ]
        for col, sql in migrations:
            if col not in lead_cols:
                conn.execute(sql)
        if "country_code" in {str(r[1]) for r in conn.execute("PRAGMA table_info(leads)").fetchall()}:
            rows = conn.execute("SELECT id, phone, address, country_code FROM leads").fetchall()
            for row in rows:
                existing = str(row[3] or "").strip().upper()
                if existing:
                    continue
                guessed = _infer_country_code(str(row[1] or ""), str(row[2] or ""))
                if guessed:
                    conn.execute("UPDATE leads SET country_code=? WHERE id=?", (guessed, int(row[0])))

    def upsert_lead_from_row(self, run_id: str, row: dict[str, Any], audience: str = "", country_code: str = "") -> int:
        now = self._now().isoformat()
        name = str(row.get("name", "")).strip()
        phone = str(row.get("phone", "")).strip()
        email = str(row.get("website_emails", "")).split(",")[0].strip()
        website = str(row.get("website", "")).strip()
        maps_url = str(row.get("maps_url", "")).strip()
        address = str(row.get("address", "")).strip()
        normalized_audience = str(audience or "").strip()
        normalized_country = str(country_code or "").strip().upper() or _infer_country_code(phone, address)

        preferred = "EMAIL" if email else ("WHATSAPP" if phone else "NONE")
        stage = "NEW"

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO leads (
                    run_id, business_name, maps_url, phone, email, website, address,
                    stage, channel_preferred, audience, country_code, created_at_utc, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(maps_url) DO UPDATE SET
                    run_id=excluded.run_id,
                    business_name=excluded.business_name,
                    phone=excluded.phone,
                    email=excluded.email,
                    website=excluded.website,
                    address=excluded.address,
                    channel_preferred=excluded.channel_preferred,
                    audience=CASE WHEN excluded.audience != '' THEN excluded.audience ELSE leads.audience END,
                    country_code=CASE WHEN excluded.country_code != '' THEN excluded.country_code ELSE leads.country_code END,
                    updated_at_utc=excluded.updated_at_utc
                """,
                (
                    run_id,
                    name,
                    maps_url,
                    phone,
                    email,
                    website,
                    address,
                    stage,
                    preferred,
                    normalized_audience,
                    normalized_country,
                    now,
                    now,
                ),
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

    def list_leads_for_initial_contact(self, limit: int = 100, run_id_prefix: str = "") -> list[Lead]:
        with self._connect() as conn:
            if run_id_prefix.strip():
                rows = conn.execute(
                    """
                    SELECT id, run_id, business_name, maps_url, phone, email, website, address, stage, channel_preferred, opt_out
                    FROM leads
                    WHERE stage IN ('NEW', 'QUALIFIED')
                      AND opt_out = 0
                      AND channel_preferred IN ('EMAIL', 'WHATSAPP')
                      AND run_id LIKE ?
                    ORDER BY CASE WHEN channel_preferred='EMAIL' THEN 0 ELSE 1 END, id ASC LIMIT ?
                    """,
                    (f"{run_id_prefix}%", limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, run_id, business_name, maps_url, phone, email, website, address, stage, channel_preferred, opt_out
                    FROM leads
                    WHERE stage IN ('NEW', 'QUALIFIED') AND opt_out = 0 AND channel_preferred IN ('EMAIL', 'WHATSAPP')
                    ORDER BY CASE WHEN channel_preferred='EMAIL' THEN 0 ELSE 1 END, id ASC LIMIT ?
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

    def list_leads_by_stage(self, stage: str, limit: int = 100) -> list[Lead]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, run_id, business_name, maps_url, phone, email, website, address, stage, channel_preferred, opt_out
                FROM leads
                WHERE stage = ? AND opt_out = 0
                ORDER BY id ASC LIMIT ?
                """,
                (stage, limit),
            ).fetchall()
        return [Lead(*self._normalize_lead_row(row)) for row in rows]

    def update_stage(self, lead_id: int, stage: str) -> None:
        now = self._now().isoformat()
        lost_at = now if stage == "LOST" else ""
        won_at = now if stage == "WON" else ""
        with self._connect() as conn:
            if stage == "LOST":
                conn.execute("UPDATE leads SET stage=?, lost_at_utc=?, updated_at_utc=? WHERE id=?", (stage, lost_at, now, lead_id))
            elif stage == "WON":
                conn.execute("UPDATE leads SET stage=?, won_at_utc=?, updated_at_utc=? WHERE id=?", (stage, won_at, now, lead_id))
            else:
                conn.execute("UPDATE leads SET stage=?, updated_at_utc=? WHERE id=?", (stage, now, lead_id))
            conn.commit()

    def set_consent(self, lead_id: int, accepted: bool) -> None:
        now = self._now().isoformat()
        stage = "CONSENTED" if accepted else "WAITING_REPLY"
        with self._connect() as conn:
            conn.execute(
                "UPDATE leads SET consent_accepted=?, stage=?, updated_at_utc=? WHERE id=?",
                (1 if accepted else 0, stage, now, lead_id),
            )
            conn.commit()

    def set_preview_and_payment(self, lead_id: int, preview_url: str, payment_url: str) -> None:
        now = self._now().isoformat()
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
        ts = self._now().isoformat()
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

    def get_first_touch_timestamp(self, lead_id: int, intent: str) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT timestamp_utc FROM touches WHERE lead_id=? AND intent=? ORDER BY id ASC LIMIT 1",
                (lead_id, intent),
            ).fetchone()
        return str(row[0]) if row and row[0] else ""

    def get_latest_touch(self, lead_id: int, intent: str) -> sqlite3.Row | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, template_id, status, timestamp_utc
                FROM touches
                WHERE lead_id=? AND intent=?
                ORDER BY id DESC LIMIT 1
                """,
                (lead_id, intent),
            ).fetchone()
        return row

    def has_offer_sent(self, lead_id: int) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM touches WHERE lead_id=? AND intent='OFFER' LIMIT 1",
                (lead_id,),
            ).fetchone()
        return row is not None

    def save_reply(self, lead_id: int, channel: str, body: str, classification: str, confidence: float) -> None:
        ts = self._now().isoformat()
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
        ts = self._now().isoformat()
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
    def _contact_hash(contact: str) -> str:
        return hashlib.sha256((contact or "").strip().lower().encode("utf-8")).hexdigest()

    def has_contact_been_sent(self, contact: str, channel: str, intent: str) -> bool:
        h = self._contact_hash(contact)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM contact_send_guard WHERE contact_hash=? AND channel=? AND intent=? LIMIT 1",
                (h, channel, intent),
            ).fetchone()
        return row is not None

    def mark_contact_sent(self, contact: str, channel: str, intent: str, lead_id: int) -> None:
        ts = self._now().isoformat()
        h = self._contact_hash(contact)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO contact_send_guard
                (contact_hash, channel, intent, first_lead_id, last_lead_id, first_sent_at_utc, last_sent_at_utc, send_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(contact_hash, channel, intent) DO UPDATE SET
                    last_lead_id=excluded.last_lead_id,
                    last_sent_at_utc=excluded.last_sent_at_utc,
                    send_count=contact_send_guard.send_count + 1
                """,
                (h, channel, intent, lead_id, lead_id, ts, ts),
            )
            conn.commit()

    def get_pricing_state(self) -> PricingState:
        with self._connect() as conn:
            state = self._get_or_init_pricing_state_conn(conn)
        return state

    def _get_or_init_pricing_state_conn(self, conn: sqlite3.Connection) -> PricingState:
        row = conn.execute(
            """
            SELECT price_level, price_full, price_simple, baseline_conversion, offers_in_window, sales_in_window, updated_at_utc
            FROM pricing_state
            WHERE id=1
            """
        ).fetchone()
        if row:
            baseline = float(row[3]) if row[3] is not None else None
            return PricingState(
                price_level=int(row[0]),
                price_full=int(row[1]),
                price_simple=int(row[2]),
                baseline_conversion=baseline,
                offers_in_window=int(row[4]),
                sales_in_window=int(row[5]),
                updated_at_utc=str(row[6]),
            )
        now = self._now().isoformat()
        base_full, base_simple = _price_for_level(0)
        conn.execute(
            """
            INSERT INTO pricing_state (id, price_level, price_full, price_simple, baseline_conversion, offers_in_window, sales_in_window, updated_at_utc)
            VALUES (1, 0, ?, ?, NULL, 0, 0, ?)
            """,
            (base_full, base_simple, now),
        )
        conn.commit()
        return PricingState(
            price_level=0,
            price_full=base_full,
            price_simple=base_simple,
            baseline_conversion=None,
            offers_in_window=0,
            sales_in_window=0,
            updated_at_utc=now,
        )

    def record_offer_snapshot(self, lead_id: int, run_id: str) -> dict[str, Any]:
        now = self._now().isoformat()
        events: list[dict[str, Any]] = []
        with self._connect() as conn:
            state = self._get_or_init_pricing_state_conn(conn)
            conn.execute(
                """
                INSERT INTO offer_snapshots
                (lead_id, run_id, price_level, price_full, price_simple, offered_at_utc, converted, converted_at_utc)
                VALUES (?, ?, ?, ?, ?, ?, 0, '')
                """,
                (lead_id, run_id, state.price_level, state.price_full, state.price_simple, now),
            )

            offers = state.offers_in_window + 1
            sales = state.sales_in_window
            level = state.price_level
            price_full = state.price_full
            price_simple = state.price_simple
            baseline = state.baseline_conversion
            window_closed = False
            window_conversion = (sales / offers) if offers else 0.0

            if offers >= 10:
                window_closed = True
                if level == 0 and baseline is None:
                    baseline = window_conversion
                    conn.execute(
                        """
                        INSERT INTO pricing_events (event_type, from_level, to_level, reason, run_id, timestamp_utc)
                        VALUES ('BASELINE_UPDATE', ?, ?, ?, ?, ?)
                        """,
                        (level, level, "baseline_initialized", run_id, now),
                    )
                    events.append(
                        {
                            "event": "baseline_conversion_updated",
                            "from_level": level,
                            "to_level": level,
                            "baseline_conversion": baseline,
                            "reason": "baseline_initialized",
                        }
                    )

                should_down = False
                reason = ""
                if sales == 0:
                    should_down = True
                    reason = "zero_sales_in_window"
                elif baseline is not None and window_conversion < baseline:
                    should_down = True
                    reason = "below_baseline"

                if should_down and level > 0:
                    new_level = level - 1
                    conn.execute(
                        """
                        INSERT INTO pricing_events (event_type, from_level, to_level, reason, run_id, timestamp_utc)
                        VALUES ('DOWN', ?, ?, ?, ?, ?)
                        """,
                        (level, new_level, reason, run_id, now),
                    )
                    events.append(
                        {
                            "event": "pricing_level_down",
                            "from_level": level,
                            "to_level": new_level,
                            "reason": reason,
                        }
                    )
                    level = new_level
                    price_full, price_simple = _price_for_level(new_level)

                offers = 0
                sales = 0

            conn.execute(
                """
                UPDATE pricing_state
                SET price_level=?, price_full=?, price_simple=?, baseline_conversion=?,
                    offers_in_window=?, sales_in_window=?, updated_at_utc=?
                WHERE id=1
                """,
                (level, price_full, price_simple, baseline, offers, sales, now),
            )
            conn.commit()

        return {
            "price_level": level,
            "price_full": price_full,
            "price_simple": price_simple,
            "baseline_conversion": baseline,
            "offers_in_window": offers,
            "sales_in_window": sales,
            "window_closed": window_closed,
            "window_conversion": window_conversion,
            "events": events,
        }

    def mark_sale(self, lead_id: int, run_id: str, reason: str, accepted_plan: str = "COMPLETO", sale_amount: float | None = None) -> dict[str, Any]:
        now_dt = self._now()
        now = now_dt.isoformat()
        chosen_plan = (accepted_plan or "COMPLETO").strip().upper()
        if chosen_plan not in {"COMPLETO", "SIMPLES"}:
            chosen_plan = "COMPLETO"

        with self._connect() as conn:
            state = self._get_or_init_pricing_state_conn(conn)
            snapshot = conn.execute(
                """
                SELECT id, price_level, price_full, price_simple
                FROM offer_snapshots
                WHERE lead_id=?
                ORDER BY id DESC LIMIT 1
                """,
                (lead_id,),
            ).fetchone()
            offer_full = int(snapshot[2]) if snapshot else state.price_full
            offer_simple = int(snapshot[3]) if snapshot else state.price_simple
            used_level = int(snapshot[1]) if snapshot else state.price_level
            snapshot_id = int(snapshot[0]) if snapshot else 0

            effective_amount = float(sale_amount if sale_amount is not None else (offer_simple if chosen_plan == "SIMPLES" else offer_full))
            if snapshot_id:
                conn.execute(
                    "UPDATE offer_snapshots SET converted=1, converted_at_utc=? WHERE id=?",
                    (now, snapshot_id),
                )

            conn.execute(
                """
                UPDATE leads
                SET stage='WON', sale_amount=?, accepted_plan=?, won_at_utc=?, updated_at_utc=?
                WHERE id=?
                """,
                (effective_amount, chosen_plan, now, now, lead_id),
            )

            new_level = state.price_level + 1
            new_full, new_simple = _price_for_level(new_level)
            conn.execute(
                """
                INSERT INTO pricing_events (event_type, from_level, to_level, reason, run_id, timestamp_utc)
                VALUES ('UP', ?, ?, ?, ?, ?)
                """,
                (state.price_level, new_level, reason, run_id, now),
            )
            conn.execute(
                """
                UPDATE pricing_state
                SET price_level=?, price_full=?, price_simple=?, baseline_conversion=?,
                    offers_in_window=0, sales_in_window=0, updated_at_utc=?
                WHERE id=1
                """,
                (new_level, new_full, new_simple, state.baseline_conversion, now),
            )

            expires = (now_dt + timedelta(days=365)).isoformat()
            conn.execute(
                """
                INSERT INTO domain_jobs (lead_id, domain_name, status, expires_at_utc, notes, created_at_utc, updated_at_utc)
                VALUES (?, '', 'DOMAIN_SELECTED', ?, 'Selecionar dominio com potencial local', ?, ?)
                ON CONFLICT(lead_id) DO NOTHING
                """,
                (lead_id, expires, now, now),
            )
            conn.commit()

        return {
            "lead_id": lead_id,
            "accepted_plan": chosen_plan,
            "sale_amount": effective_amount,
            "old_level": state.price_level,
            "new_level": new_level,
            "price_full": new_full,
            "price_simple": new_simple,
            "offer_level": used_level,
            "offer_price_full": offer_full,
            "offer_price_simple": offer_simple,
        }

    def enqueue_reply_review(self, lead_id: int, channel: str, inbound_text: str) -> int:
        now = self._now().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO reply_review_queue
                (lead_id, channel, inbound_text, status, intent_final, draft_reply, confidence, created_at_utc, updated_at_utc)
                VALUES (?, ?, ?, 'PENDING', '', '', 0, ?, ?)
                """,
                (lead_id, channel, inbound_text, now, now),
            )
            row = conn.execute("SELECT last_insert_rowid()").fetchone()
            conn.commit()
        if not row:
            raise RuntimeError("failed to enqueue reply")
        return int(row[0])

    def list_reply_review_queue(self, statuses: list[str] | None = None, limit: int = 100) -> list[ReplyReviewItem]:
        with self._connect() as conn:
            if statuses:
                placeholders = ",".join("?" for _ in statuses)
                rows = conn.execute(
                    f"""
                    SELECT id, lead_id, channel, inbound_text, status, intent_final, draft_reply, confidence, created_at_utc, updated_at_utc
                    FROM reply_review_queue
                    WHERE status IN ({placeholders})
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    tuple(statuses) + (limit,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, lead_id, channel, inbound_text, status, intent_final, draft_reply, confidence, created_at_utc, updated_at_utc
                    FROM reply_review_queue
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [
            ReplyReviewItem(
                id=int(r[0]),
                lead_id=int(r[1]),
                channel=str(r[2]),
                inbound_text=str(r[3]),
                status=str(r[4]),
                intent_final=str(r[5]),
                draft_reply=str(r[6]),
                confidence=float(r[7]),
                created_at_utc=str(r[8]),
                updated_at_utc=str(r[9]),
            )
            for r in rows
        ]

    def get_reply_review_item(self, queue_id: int) -> ReplyReviewItem | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, lead_id, channel, inbound_text, status, intent_final, draft_reply, confidence, created_at_utc, updated_at_utc
                FROM reply_review_queue WHERE id=?
                """,
                (queue_id,),
            ).fetchone()
        if not row:
            return None
        return ReplyReviewItem(
            id=int(row[0]),
            lead_id=int(row[1]),
            channel=str(row[2]),
            inbound_text=str(row[3]),
            status=str(row[4]),
            intent_final=str(row[5]),
            draft_reply=str(row[6]),
            confidence=float(row[7]),
            created_at_utc=str(row[8]),
            updated_at_utc=str(row[9]),
        )

    def set_reply_codex_decision(self, queue_id: int, intent_final: str, draft_reply: str, confidence: float, status: str) -> None:
        now = self._now().isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE reply_review_queue
                SET intent_final=?, draft_reply=?, confidence=?, status=?, updated_at_utc=?
                WHERE id=?
                """,
                (intent_final, draft_reply, confidence, status, now, queue_id),
            )
            conn.commit()

    def mark_reply_sent(self, queue_id: int) -> None:
        now = self._now().isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE reply_review_queue SET status='SENT', updated_at_utc=? WHERE id=?",
                (now, queue_id),
            )
            conn.commit()

    def pending_reply_counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*)
                FROM reply_review_queue
                GROUP BY status
                """
            ).fetchall()
        counts = {str(r[0]): int(r[1]) for r in rows}
        return {
            "pending": counts.get("PENDING", 0),
            "codex_done": counts.get("CODEX_DONE", 0),
            "review_required": counts.get("REVIEW_REQUIRED", 0),
            "sent": counts.get("SENT", 0),
        }

    def list_domain_jobs(self, limit: int = 200) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT id, lead_id, domain_name, status, expires_at_utc, notes, created_at_utc, updated_at_utc
                FROM domain_jobs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [
            {
                "id": int(r[0]),
                "lead_id": int(r[1]),
                "domain_name": str(r[2] or ""),
                "status": str(r[3] or ""),
                "expires_at_utc": str(r[4] or ""),
                "notes": str(r[5] or ""),
                "created_at_utc": str(r[6] or ""),
                "updated_at_utc": str(r[7] or ""),
            }
            for r in rows
        ]

    def update_domain_job(self, job_id: int, status: str, domain_name: str = "", expires_at_utc: str = "", notes: str = "") -> None:
        now = self._now().isoformat()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT domain_name, expires_at_utc, notes FROM domain_jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            if not row:
                return
            next_domain = domain_name if domain_name else str(row[0] or "")
            next_expires = expires_at_utc if expires_at_utc else str(row[1] or "")
            next_notes = notes if notes else str(row[2] or "")
            conn.execute(
                """
                UPDATE domain_jobs
                SET status=?, domain_name=?, expires_at_utc=?, notes=?, updated_at_utc=?
                WHERE id=?
                """,
                (status, next_domain, next_expires, next_notes, now, job_id),
            )
            conn.commit()

    def list_domain_alert_candidates(self, alert_days: list[int]) -> list[dict[str, Any]]:
        now = self._now()
        jobs = self.list_domain_jobs(limit=500)
        out: list[dict[str, Any]] = []
        with self._connect() as conn:
            for job in jobs:
                expires_raw = str(job.get("expires_at_utc") or "")
                if not expires_raw:
                    continue
                try:
                    expires = datetime.fromisoformat(expires_raw)
                except Exception:
                    continue
                days_left = (expires.date() - now.date()).days
                if days_left not in alert_days:
                    continue
                seen = conn.execute(
                    "SELECT 1 FROM domain_alerts WHERE domain_job_id=? AND days_before=?",
                    (int(job["id"]), days_left),
                ).fetchone()
                if seen:
                    continue
                out.append({"job_id": int(job["id"]), "lead_id": int(job["lead_id"]), "days_left": days_left, "domain_name": job["domain_name"]})
        return out

    def mark_domain_alert_sent(self, job_id: int, days_before: int) -> None:
        now = self._now().isoformat()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO domain_alerts (domain_job_id, days_before, created_at_utc) VALUES (?, ?, ?)",
                (job_id, days_before, now),
            )
            conn.commit()

    def close_expired_sequences(self, max_days: int = 7) -> list[int]:
        cutoff = (self._now() - timedelta(days=max_days)).isoformat()
        lost_ids: list[int] = []
        with self._connect() as conn:
            rows_wait = conn.execute(
                """
                SELECT l.id
                FROM leads l
                JOIN (
                    SELECT lead_id, MIN(timestamp_utc) AS first_touch
                    FROM touches
                    WHERE intent='CONSENT_REQUEST'
                    GROUP BY lead_id
                ) t ON t.lead_id = l.id
                WHERE l.stage='WAITING_REPLY' AND l.opt_out=0 AND t.first_touch <= ?
                """,
                (cutoff,),
            ).fetchall()
            rows_offer = conn.execute(
                """
                SELECT l.id
                FROM leads l
                JOIN (
                    SELECT lead_id, MIN(timestamp_utc) AS first_offer
                    FROM touches
                    WHERE intent='OFFER'
                    GROUP BY lead_id
                ) t ON t.lead_id = l.id
                WHERE l.stage='PAYMENT_SENT' AND l.opt_out=0 AND t.first_offer <= ?
                """,
                (cutoff,),
            ).fetchall()
            ids = sorted({int(r[0]) for r in rows_wait} | {int(r[0]) for r in rows_offer})
            now = self._now().isoformat()
            for lead_id in ids:
                conn.execute(
                    "UPDATE leads SET stage='LOST', lost_at_utc=?, updated_at_utc=? WHERE id=?",
                    (now, now, lead_id),
                )
            conn.commit()
            lost_ids.extend(ids)
        return lost_ids

    def get_lead_sale_context(self, lead_id: int) -> dict[str, Any]:
        with self._connect() as conn:
            lead = conn.execute(
                """
                SELECT id, stage, email, phone, business_name, sale_amount, accepted_plan
                FROM leads
                WHERE id=?
                """,
                (lead_id,),
            ).fetchone()
            if not lead:
                return {}
            latest_offer = conn.execute(
                """
                SELECT id, price_level, price_full, price_simple, offered_at_utc
                FROM offer_snapshots
                WHERE lead_id=?
                ORDER BY id DESC LIMIT 1
                """,
                (lead_id,),
            ).fetchone()
        return {
            "id": int(lead["id"]),
            "stage": str(lead["stage"]),
            "email": str(lead["email"] or ""),
            "phone": str(lead["phone"] or ""),
            "business_name": str(lead["business_name"] or ""),
            "sale_amount": float(lead["sale_amount"] or 0),
            "accepted_plan": str(lead["accepted_plan"] or ""),
            "latest_offer": {
                "id": int(latest_offer["id"]),
                "price_level": int(latest_offer["price_level"]),
                "price_full": int(latest_offer["price_full"]),
                "price_simple": int(latest_offer["price_simple"]),
                "offered_at_utc": str(latest_offer["offered_at_utc"]),
            }
            if latest_offer
            else None,
        }

    @staticmethod
    def _normalize_lead_row(row: sqlite3.Row) -> tuple[int, str, str, str, str, str, str, str, str, str, int]:
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
