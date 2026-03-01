from __future__ import annotations

import html as html_lib
import json
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, unquote_plus

from .config import get_config
from .crm_store import CrmStore


def _parse_utc(value: str) -> datetime | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_country_filter(value: str) -> str:
    raw = (value or "ALL").strip().upper()
    return raw if raw in {"ALL", "BR", "NON_BR", "PT", "UK", "US", "ES"} else "ALL"


def _normalize_audience_filter(value: str) -> str:
    raw = (value or "ALL").strip()
    if not raw:
        return "ALL"
    if raw.lower() == "all":
        return "ALL"
    return raw[:120]


def _normalize_approach_filter(value: str) -> str:
    raw = (value or "ALL").strip().upper()
    return raw if raw in {"ALL", "LEGACY", "V2"} else "ALL"


def _lead_filter_clauses(
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
    alias: str = "",
) -> tuple[list[str], list[Any]]:
    prefix = f"{alias}." if alias else ""
    clauses: list[str] = []
    params: list[Any] = []
    country = _normalize_country_filter(country_filter)
    audience = _normalize_audience_filter(audience_filter)
    approach = _normalize_approach_filter(approach_filter)
    if country == "NON_BR":
        clauses.append(f"COALESCE({prefix}country_code, '') != 'BR'")
    elif country != "ALL":
        clauses.append(f"COALESCE({prefix}country_code, '') = ?")
        params.append(country)
    if audience != "ALL":
        clauses.append(f"lower(COALESCE({prefix}audience, '')) = lower(?)")
        params.append(audience)
    if approach == "LEGACY":
        clauses.append(f"COALESCE({prefix}approach_version, 'v1_legacy') != 'v2_identity_probe'")
    elif approach == "V2":
        clauses.append(f"COALESCE({prefix}approach_version, 'v1_legacy') = 'v2_identity_probe'")
    return clauses, params


def _read_last_events(path: Path, max_lines: int = 200) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()[-max_lines:]
    out: list[dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def _db_counts(db_path: Path, country_filter: str = "ALL", audience_filter: str = "ALL", approach_filter: str = "ALL") -> dict[str, Any]:
    if not db_path.exists():
        return {"leads_total": 0, "stage_counts": {}, "touches_total": 0, "replies_total": 0}
    with sqlite3.connect(db_path) as conn:
        clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        leads_total = int(conn.execute(f"SELECT COUNT(*) FROM leads{where_sql}", params).fetchone()[0])
        rows = conn.execute(f"SELECT stage, COUNT(*) FROM leads{where_sql} GROUP BY stage", params).fetchall()
        if clauses:
            touches_total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM touches t JOIN leads l ON l.id = t.lead_id WHERE {' AND '.join(_lead_filter_clauses(country_filter, audience_filter, approach_filter, 'l')[0])}",
                    _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")[1],
                ).fetchone()[0]
            )
            replies_total = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM replies r JOIN leads l ON l.id = r.lead_id WHERE {' AND '.join(_lead_filter_clauses(country_filter, audience_filter, approach_filter, 'l')[0])}",
                    _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")[1],
                ).fetchone()[0]
            )
        else:
            touches_total = int(conn.execute("SELECT COUNT(*) FROM touches").fetchone()[0])
            replies_total = int(conn.execute("SELECT COUNT(*) FROM replies").fetchone()[0])
    return {
        "leads_total": leads_total,
        "stage_counts": {str(r[0]): int(r[1]) for r in rows},
        "touches_total": touches_total,
        "replies_total": replies_total,
    }


def _derive_country(phone: str, address: str, country_code: str = "") -> str:
    normalized = (country_code or "").strip().upper()
    if normalized in {"BR", "PT", "UK", "US", "ES"}:
        return normalized
    if normalized == "NON_BR":
        return "OTHER"
    phone_s = (phone or "").strip()
    address_l = (address or "").lower()
    if phone_s.startswith("+55") or "brasil" in address_l or "brazil" in address_l:
        return "BR"
    if phone_s.startswith("+351") or "portugal" in address_l or "lisbon" in address_l or "lisboa" in address_l or "porto" in address_l:
        return "PT"
    if phone_s.startswith("+44") or "london" in address_l or "united kingdom" in address_l or "uk" in address_l:
        return "UK"
    if phone_s.startswith("+1") or "united states" in address_l or "usa" in address_l or "new york" in address_l or "miami" in address_l:
        return "US"
    if phone_s.startswith("+34") or "spain" in address_l or "españa" in address_l or "espana" in address_l or "madrid" in address_l or "barcelona" in address_l:
        return "ES"
    if not phone_s and not address_l:
        return "UNKNOWN"
    return "OTHER"


def _country_channel_snapshot(db_path: Path, country_filter: str = "ALL", audience_filter: str = "ALL", approach_filter: str = "ALL") -> dict[str, Any]:
    defaults = {"by_country": [], "approaches_by_channel": [], "approaches_by_country_channel": []}
    if not db_path.exists():
        return defaults
    try:
        with sqlite3.connect(db_path) as conn:
            lead_clauses, lead_params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
            lead_where = f" WHERE {' AND '.join(lead_clauses)}" if lead_clauses else ""
            lead_rows = conn.execute(f"SELECT phone, address, country_code FROM leads{lead_where}", lead_params).fetchall()
            by_country_counter: Counter[str] = Counter()
            for row in lead_rows:
                by_country_counter[_derive_country(str(row[0] or ""), str(row[1] or ""), str(row[2] or ""))] += 1

            touch_clauses, touch_params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            touch_where = f" WHERE {' AND '.join(touch_clauses)}" if touch_clauses else ""
            channel_rows = conn.execute(
                f"""
                SELECT channel, COUNT(*)
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                {touch_where}
                GROUP BY channel
                ORDER BY COUNT(*) DESC
                """,
                touch_params,
            ).fetchall()

            country_channel_rows = conn.execute(
                f"""
                SELECT l.country_code, l.phone, l.address, t.channel, COUNT(*)
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                {touch_where}
                GROUP BY l.country_code, l.phone, l.address, t.channel
                """,
                touch_params,
            ).fetchall()
    except sqlite3.Error:
        return defaults

    country_channel_counter: Counter[tuple[str, str]] = Counter()
    for country_code, phone, address, channel, count in country_channel_rows:
        country = _derive_country(str(phone or ""), str(address or ""), str(country_code or ""))
        country_channel_counter[(country, str(channel or "UNKNOWN"))] += int(count or 0)

    return {
        "by_country": [
            {"country": country, "leads": count}
            for country, count in sorted(by_country_counter.items(), key=lambda it: (-it[1], it[0]))
        ],
        "approaches_by_channel": [
            {"channel": str(r[0] or "UNKNOWN"), "touches": int(r[1])}
            for r in channel_rows
        ],
        "approaches_by_country_channel": [
            {"country": country, "channel": channel, "touches": touches}
            for (country, channel), touches in sorted(country_channel_counter.items(), key=lambda it: (-it[1], it[0][0], it[0][1]))
        ],
    }


def _audience_options_snapshot(db_path: Path, country_filter: str = "ALL", approach_filter: str = "ALL") -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, "ALL", approach_filter)
            if clauses:
                clauses.append("trim(COALESCE(audience, '')) != ''")
                sql = f" WHERE {' AND '.join(clauses)}"
            else:
                sql = " WHERE trim(COALESCE(audience, '')) != ''"
            rows = conn.execute(
                f"""
                SELECT trim(audience) AS audience_value, COUNT(*)
                FROM leads
                {sql}
                GROUP BY lower(trim(audience))
                ORDER BY COUNT(*) DESC, lower(trim(audience)) ASC
                LIMIT 12
                """,
                params,
            ).fetchall()
    except sqlite3.Error:
        return []
    return [{"audience": str(r[0] or "").strip(), "count": int(r[1])} for r in rows if str(r[0] or "").strip()]


def _throughput_snapshot(
    db_path: Path,
    events: list[dict[str, Any]],
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {
        "touches_1h_total": 0,
        "touches_24h_total": 0,
        "touches_1h_by_channel": [],
        "touches_24h_by_channel": [],
        "new_leads_24h": 0,
        "replies_24h": 0,
        "offers_24h": 0,
        "last_event_utc": "",
        "last_event_age_min": None,
    }
    now = datetime.now(timezone.utc)
    last_event_utc = ""
    last_event_age_min: int | None = None
    for event in reversed(events):
        ts = _parse_utc(str(event.get("timestamp_utc", "")))
        if ts:
            last_event_utc = ts.isoformat()
            last_event_age_min = max(0, int((now - ts).total_seconds() // 60))
            break

    if not db_path.exists():
        defaults["last_event_utc"] = last_event_utc
        defaults["last_event_age_min"] = last_event_age_min
        return defaults

    since_1h = (now - timedelta(hours=1)).isoformat()
    since_24h = (now - timedelta(hours=24)).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            lead_clauses, lead_params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            base_1h = [*lead_clauses, "t.timestamp_utc >= ?"]
            base_24h = [*lead_clauses, "t.timestamp_utc >= ?"]
            lead_self_clauses, lead_self_params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
            rows_1h = conn.execute(
                f"""
                SELECT channel, COUNT(*)
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE {' AND '.join(base_1h)}
                GROUP BY channel
                ORDER BY COUNT(*) DESC
                """,
                [*lead_params, since_1h],
            ).fetchall()
            rows_24h = conn.execute(
                f"""
                SELECT channel, COUNT(*)
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE {' AND '.join(base_24h)}
                GROUP BY channel
                ORDER BY COUNT(*) DESC
                """,
                [*lead_params, since_24h],
            ).fetchall()
            new_leads_24h = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM leads WHERE {' AND '.join([*lead_self_clauses, 'created_at_utc >= ?'])}",
                    [*lead_self_params, since_24h],
                ).fetchone()[0]
            )
            replies_24h = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM replies r JOIN leads l ON l.id = r.lead_id WHERE {' AND '.join([*lead_clauses, 'r.timestamp_utc >= ?'])}",
                    [*lead_params, since_24h],
                ).fetchone()[0]
            )
            offers_24h = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM offer_snapshots o JOIN leads l ON l.id = o.lead_id WHERE {' AND '.join([*lead_clauses, 'o.offered_at_utc >= ?'])}",
                    [*lead_params, since_24h],
                ).fetchone()[0]
            )
    except sqlite3.Error:
        defaults["last_event_utc"] = last_event_utc
        defaults["last_event_age_min"] = last_event_age_min
        return defaults

    touches_1h = [{"channel": str(r[0] or "UNKNOWN"), "count": int(r[1])} for r in rows_1h]
    touches_24h = [{"channel": str(r[0] or "UNKNOWN"), "count": int(r[1])} for r in rows_24h]
    return {
        "touches_1h_total": sum(int(r["count"]) for r in touches_1h),
        "touches_24h_total": sum(int(r["count"]) for r in touches_24h),
        "touches_1h_by_channel": touches_1h,
        "touches_24h_by_channel": touches_24h,
        "new_leads_24h": new_leads_24h,
        "replies_24h": replies_24h,
        "offers_24h": offers_24h,
        "last_event_utc": last_event_utc,
        "last_event_age_min": last_event_age_min,
    }


def _ops_snapshot(ops_db: Path) -> dict[str, Any]:
    if not ops_db.exists():
        return {"global_safe_mode": False, "channels": [], "metrics": []}
    with sqlite3.connect(ops_db) as conn:
        flag = conn.execute("SELECT value FROM flags WHERE name='GLOBAL_SAFE_MODE'").fetchone()
        channels = conn.execute(
            "SELECT channel, status, reason, cooldown_until_utc FROM channel_status ORDER BY channel"
        ).fetchall()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        metrics = conn.execute(
            "SELECT channel, sent, failed, bounces, complaints FROM channel_metrics_daily WHERE day_utc=?",
            (today,),
        ).fetchall()
    return {
        "global_safe_mode": bool(flag and flag[0] == "1"),
        "channels": [
            {
                "channel": str(c[0]),
                "status": str(c[1]),
                "reason": str(c[2] or ""),
                "cooldown_until_utc": str(c[3] or ""),
            }
            for c in channels
        ],
        "metrics": [
            {
                "channel": str(m[0]),
                "sent": int(m[1]),
                "failed": int(m[2]),
                "bounces": int(m[3]),
                "complaints": int(m[4]),
            }
            for m in metrics
        ],
    }


def _pricing_snapshot(db_path: Path) -> dict[str, Any]:
    defaults = {
        "price_level": 0,
        "price_full": 200,
        "price_simple": 100,
        "baseline_conversion": None,
        "offers_in_window": 0,
        "sales_in_window": 0,
        "updated_at_utc": "",
    }
    if not db_path.exists():
        return defaults
    try:
        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT price_level, price_full, price_simple, baseline_conversion, offers_in_window, sales_in_window, updated_at_utc
                FROM pricing_state WHERE id=1
                """
            ).fetchone()
    except sqlite3.Error:
        return defaults
    if not row:
        return defaults
    baseline = float(row[3]) if row[3] is not None else None
    return {
        "price_level": int(row[0]),
        "price_full": int(row[1]),
        "price_simple": int(row[2]),
        "baseline_conversion": baseline,
        "offers_in_window": int(row[4]),
        "sales_in_window": int(row[5]),
        "updated_at_utc": str(row[6] or ""),
    }


def _funnel_7d(db_path: Path, country_filter: str = "ALL", audience_filter: str = "ALL", approach_filter: str = "ALL") -> dict[str, Any]:
    defaults = {
        "leads_7d": 0,
        "consented_7d": 0,
        "offers_7d": 0,
        "won_7d": 0,
        "lost_7d": 0,
        "conversion_7d": 0.0,
        "avg_days_to_win_7d": 0.0,
        "revenue_estimated_7d": 0.0,
    }
    if not db_path.exists():
        return defaults
    since = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
            leads_7d_where = " AND ".join([*clauses, "created_at_utc >= ?"])
            consented_where = " AND ".join([*clauses, "consent_accepted=1", "updated_at_utc >= ?"])
            won_where = " AND ".join([*clauses, "stage='WON'", "updated_at_utc >= ?"])
            lost_where = " AND ".join([*clauses, "stage='LOST'", "updated_at_utc >= ?"])
            won_time_where = " AND ".join([*clauses, "stage='WON'", "won_at_utc != ''", "won_at_utc >= ?"])
            leads_7d = int(conn.execute(f"SELECT COUNT(*) FROM leads WHERE {leads_7d_where}", [*params, since]).fetchone()[0])
            consented_7d = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM leads WHERE {consented_where}",
                    [*params, since],
                ).fetchone()[0]
            )
            lead_clauses_alias, lead_params_alias = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            lead_where_alias = " AND ".join([*lead_clauses_alias, "o.offered_at_utc >= ?"])
            offers_7d = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM offer_snapshots o JOIN leads l ON l.id = o.lead_id WHERE {lead_where_alias}",
                    [*lead_params_alias, since],
                ).fetchone()[0]
            )
            won_7d = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM leads WHERE {won_where}",
                    [*params, since],
                ).fetchone()[0]
            )
            lost_7d = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM leads WHERE {lost_where}",
                    [*params, since],
                ).fetchone()[0]
            )
            avg_days_row = conn.execute(
                f"""
                SELECT AVG(julianday(won_at_utc) - julianday(created_at_utc))
                FROM leads
                WHERE {won_time_where}
                """,
                [*params, since],
            ).fetchone()
            revenue_row = conn.execute(
                f"""
                SELECT COALESCE(SUM(sale_amount), 0)
                FROM leads
                WHERE {won_time_where}
                """,
                [*params, since],
            ).fetchone()
    except sqlite3.Error:
        return defaults
    conversion = (won_7d / offers_7d) if offers_7d else 0.0
    return {
        "leads_7d": leads_7d,
        "consented_7d": consented_7d,
        "offers_7d": offers_7d,
        "won_7d": won_7d,
        "lost_7d": lost_7d,
        "conversion_7d": conversion,
        "avg_days_to_win_7d": float(avg_days_row[0] or 0.0),
        "revenue_estimated_7d": float(revenue_row[0] or 0.0),
    }


def _domain_ops_snapshot(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"total_jobs": 0, "in_progress": 0, "by_status": [], "next_expiring": []}
    now = datetime.now(timezone.utc)
    try:
        with sqlite3.connect(db_path) as conn:
            total_jobs = int(conn.execute("SELECT COUNT(*) FROM domain_jobs").fetchone()[0])
            by_status_rows = conn.execute(
                "SELECT status, COUNT(*) FROM domain_jobs GROUP BY status ORDER BY COUNT(*) DESC"
            ).fetchall()
            expiring_rows = conn.execute(
                """
                SELECT id, lead_id, domain_name, status, expires_at_utc
                FROM domain_jobs
                WHERE expires_at_utc != ''
                ORDER BY expires_at_utc ASC
                LIMIT 10
                """
            ).fetchall()
    except sqlite3.Error:
        return {"total_jobs": 0, "in_progress": 0, "by_status": [], "next_expiring": []}
    by_status = [{"status": str(r[0]), "count": int(r[1])} for r in by_status_rows]
    in_progress_status = {"DOMAIN_SELECTED", "DOMAIN_PURCHASED", "DNS_POINTED", "SSL_OK"}
    in_progress = sum(item["count"] for item in by_status if item["status"] in in_progress_status)
    next_expiring: list[dict[str, Any]] = []
    for r in expiring_rows:
        expires_raw = str(r[4] or "")
        try:
            expires_at = datetime.fromisoformat(expires_raw)
            days_left = (expires_at.date() - now.date()).days
        except Exception:
            days_left = None
        next_expiring.append(
            {
                "id": int(r[0]),
                "lead_id": int(r[1]),
                "domain_name": str(r[2] or ""),
                "status": str(r[3] or ""),
                "expires_at_utc": expires_raw,
                "days_left": days_left,
            }
        )
    return {
        "total_jobs": total_jobs,
        "in_progress": in_progress,
        "by_status": by_status,
        "next_expiring": next_expiring,
    }


def _reply_queue_snapshot(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"counts": {"pending": 0, "codex_done": 0, "review_required": 0, "sent": 0}, "top_pending": []}
    try:
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*)
                FROM reply_review_queue
                GROUP BY status
                """
            ).fetchall()
            pending = conn.execute(
                """
                SELECT id, lead_id, inbound_text, created_at_utc
                FROM reply_review_queue
                WHERE status IN ('PENDING', 'REVIEW_REQUIRED')
                ORDER BY id ASC
                LIMIT 8
                """
            ).fetchall()
    except sqlite3.Error:
        return {"counts": {"pending": 0, "codex_done": 0, "review_required": 0, "sent": 0}, "top_pending": []}
    counts = {str(r[0]): int(r[1]) for r in rows}
    return {
        "counts": {
            "pending": counts.get("PENDING", 0),
            "codex_done": counts.get("CODEX_DONE", 0),
            "review_required": counts.get("REVIEW_REQUIRED", 0),
            "sent": counts.get("SENT", 0),
        },
        "top_pending": [
            {
                "id": int(r[0]),
                "lead_id": int(r[1]),
                "inbound_text": str(r[2] or "")[:220],
                "created_at_utc": str(r[3] or ""),
            }
            for r in pending
        ],
    }


def _template_performance_snapshot(
    db_path: Path,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {"rows": [], "ab_v2_handoff": []}
    if not db_path.exists():
        return defaults
    try:
        with sqlite3.connect(db_path) as conn:
            lead_clauses, lead_params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            where_clause = " AND ".join([*lead_clauses, "t.channel='EMAIL'", "t.intent IN ('IDENTITY_CHECK','CONSENT_REQUEST','OFFER')"])
            rows = conn.execute(
                f"""
                SELECT
                  t.template_id,
                  COUNT(*) AS sent_count,
                  COUNT(DISTINCT t.lead_id) AS unique_leads,
                  COUNT(DISTINCT CASE
                    WHEN EXISTS (
                      SELECT 1 FROM replies r
                      WHERE r.lead_id = t.lead_id
                        AND r.channel='EMAIL'
                        AND r.timestamp_utc >= t.timestamp_utc
                    ) THEN t.lead_id
                    ELSE NULL
                  END) AS replied_leads
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE {where_clause}
                GROUP BY t.template_id
                ORDER BY sent_count DESC, t.template_id ASC
                LIMIT 24
                """,
                lead_params,
            ).fetchall()
    except sqlite3.Error:
        return defaults
    out_rows: list[dict[str, Any]] = []
    for row in rows:
        template_id = str(row[0] or "")
        sent_count = int(row[1] or 0)
        unique_leads = int(row[2] or 0)
        replied_leads = int(row[3] or 0)
        reply_rate = (replied_leads / unique_leads) if unique_leads else 0.0
        out_rows.append(
            {
                "template_id": template_id,
                "sent_count": sent_count,
                "unique_leads": unique_leads,
                "replied_leads": replied_leads,
                "reply_rate": reply_rate,
            }
        )
    ab_rows = [r for r in out_rows if r["template_id"] in {"email_v2_handoff_a", "email_v2_handoff_b"}]
    return {"rows": out_rows, "ab_v2_handoff": ab_rows}


def _compute_event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter()
    for e in events:
        counter[str(e.get("event_type", "unknown"))] += 1
    return {
        "contact_delivered": counter.get("contact_delivered", 0),
        "contact_failed": counter.get("contact_failed", 0),
        "lead_qualified": counter.get("lead_qualified", 0),
        "offer_sent": counter.get("offer_sent", 0),
        "sale_marked": counter.get("sale_marked", 0),
        "pricing_level_up": counter.get("pricing_level_up", 0),
        "pricing_level_down": counter.get("pricing_level_down", 0),
        "reply_queued_for_codex": counter.get("reply_queued_for_codex", 0),
        "reply_sent": counter.get("reply_sent", 0),
        "domain_job_created": counter.get("domain_job_created", 0),
        "ab_variant_assigned": counter.get("ab_variant_assigned", 0),
    }


def build_snapshot(country_filter: str = "ALL", audience_filter: str = "ALL", approach_filter: str = "ALL") -> dict[str, Any]:
    cfg = get_config()
    CrmStore(cfg.state_db)
    events = _read_last_events(cfg.log_dir / "events.jsonl")
    country = _normalize_country_filter(country_filter)
    audience = _normalize_audience_filter(audience_filter)
    approach = _normalize_approach_filter(approach_filter)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db": _db_counts(cfg.state_db, country, audience, approach),
        "ops": _ops_snapshot(cfg.ops_state_db),
        "pricing": _pricing_snapshot(cfg.state_db),
        "funnel_7d": _funnel_7d(cfg.state_db, country, audience, approach),
        "geo_channels": _country_channel_snapshot(cfg.state_db, country, audience, approach),
        "throughput": _throughput_snapshot(cfg.state_db, events, country, audience, approach),
        "filters": {
            "country": country,
            "audience": audience,
            "approach": approach,
            "audience_options": _audience_options_snapshot(cfg.state_db, country, approach),
        },
        "domain_ops": _domain_ops_snapshot(cfg.state_db),
        "reply_queue": _reply_queue_snapshot(cfg.state_db),
        "template_performance": _template_performance_snapshot(cfg.state_db, country, audience, approach),
        "events_summary": _compute_event_summary(events),
        "events": events[-50:],
    }


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        path, _, query_raw = self.path.partition("?")
        query: dict[str, str] = {}
        if query_raw:
            for part in query_raw.split("&"):
                if "=" not in part:
                    continue
                k, v = part.split("=", 1)
                query[k] = unquote_plus(v)
        country = query.get("country", "ALL")
        audience = query.get("audience", "ALL")
        approach = query.get("approach", "")
        if path.startswith("/api/status2"):
            self._json(200, build_snapshot(country_filter=country, audience_filter=audience, approach_filter=approach or "V2"))
            return
        if path.startswith("/api/status"):
            self._json(200, build_snapshot(country_filter=country, audience_filter=audience, approach_filter=approach or "LEGACY"))
            return
        if path.startswith("/health"):
            self._json(200, {"status": "ok"})
            return
        if path in {"/", "/dashboard"}:
            self._html(200, self._render_dashboard(country_filter=country, audience_filter=audience, approach_filter=approach or "LEGACY"))
            return
        if path == "/dashboard2":
            self._html(200, self._render_dashboard(country_filter=country, audience_filter=audience, approach_filter=approach or "V2"))
            return
        self._json(404, {"ok": False, "error": "not_found"})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _json(self, code: int, payload: dict[str, Any]) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _html(self, code: int, html: str) -> None:
        raw = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _render_dashboard(self, country_filter: str = "ALL", audience_filter: str = "ALL", approach_filter: str = "ALL") -> str:
        return render_dashboard_html(country_filter=country_filter, audience_filter=audience_filter, approach_filter=approach_filter)


def render_dashboard_html(country_filter: str = "ALL", audience_filter: str = "ALL", approach_filter: str = "ALL") -> str:
    snap = build_snapshot(country_filter=country_filter, audience_filter=audience_filter, approach_filter=approach_filter)
    pricing = snap["pricing"]
    funnel = snap["funnel_7d"]
    queue = snap["reply_queue"]
    domains = snap["domain_ops"]
    ops = snap["ops"]
    geo = snap["geo_channels"]
    throughput = snap["throughput"]
    filters = snap["filters"]
    es = snap["events_summary"]
    template_perf = snap["template_performance"]
    progress_pct = min(100, int((pricing["offers_in_window"] / 10) * 100))
    safe_mode = "ATIVO" if ops["global_safe_mode"] else "NORMAL"
    safe_class = "is-bad" if ops["global_safe_mode"] else "is-ok"
    baseline_txt = f"{pricing['baseline_conversion'] * 100:.1f}%" if pricing["baseline_conversion"] is not None else "n/a"
    conv_txt = f"{funnel['conversion_7d'] * 100:.1f}%"
    event_age = throughput.get("last_event_age_min")
    if event_age is None:
        activity_txt = "Sem atividade recente"
        activity_class = "is-warn"
    elif event_age <= 3:
        activity_txt = "Agora"
        activity_class = "is-ok"
    elif event_age <= 20:
        activity_txt = f"{event_age} min atras"
        activity_class = "is-ok"
    elif event_age <= 60:
        activity_txt = f"{event_age} min atras"
        activity_class = "is-warn"
    else:
        activity_txt = f"{event_age} min atras"
        activity_class = "is-bad"

    lead_total_geo = max(1, sum(int(it["leads"]) for it in geo["by_country"]))
    max_channel_touches = max([int(it["touches"]) for it in geo["approaches_by_channel"]] or [1])
    max_country_channel_touches = max([int(it["touches"]) for it in geo["approaches_by_country_channel"]] or [1])
    ch_1h = {str(it["channel"]): int(it["count"]) for it in throughput["touches_1h_by_channel"]}
    ch_24h = {str(it["channel"]): int(it["count"]) for it in throughput["touches_24h_by_channel"]}
    pace_channels = sorted(set(ch_1h.keys()) | set(ch_24h.keys()))
    pace_max_1h = max(1, max([ch_1h.get(ch, 0) for ch in pace_channels] or [0]))
    pace_max_24h = max(1, max([ch_24h.get(ch, 0) for ch in pace_channels] or [0]))
    selected_country = filters["country"]
    selected_audience = filters["audience"]
    selected_approach = filters["approach"]
    queue_backlog = queue["counts"]["pending"] + queue["counts"]["review_required"]
    queue_label = "Atenção" if queue_backlog >= 10 else ("Fila ativa" if queue_backlog else "Sem fila")
    queue_class = "is-bad" if queue_backlog >= 10 else ("is-warn" if queue_backlog else "is-ok")

    current_scope = {
        "ALL": "Geral",
        "BR": "Brasil",
        "NON_BR": "Fora do Brasil",
        "PT": "Portugal",
        "UK": "Reino Unido",
        "US": "USA",
        "ES": "Espanha",
    }.get(selected_country, "Geral")
    dashboard_title = "LeadGenerator 2"
    dashboard_badge = "Fluxo 2"
    if selected_approach == "LEGACY":
        dashboard_title = "LeadGenerator 1"
        dashboard_badge = "Fluxo 1"
    scope_suffix = "" if selected_audience == "ALL" else f" | Nicho: {selected_audience}"

    def _safe(value: Any) -> str:
        return html_lib.escape(str(value or ""))

    def _status_badge(status: str) -> str:
        norm = (status or "").upper()
        if norm in {"ACTIVE", "OK", "ENABLED"}:
            return "is-ok"
        if norm in {"PAUSED", "STOPPED"}:
            return "is-warn"
        if norm in {"ERROR", "FAILED", "DISABLED", "BLOCKED"}:
            return "is-bad"
        return "is-neutral"

    dashboard_nav = (
        f"<a class='filter-pill {'is-active' if selected_approach == 'LEGACY' else ''}' "
        f"href='/dashboard?country={quote_plus(selected_country)}&audience={quote_plus(selected_audience)}'>Dashboard 1</a>"
        f"<a class='filter-pill {'is-active' if selected_approach == 'V2' else ''}' "
        f"href='/dashboard2?country={quote_plus(selected_country)}&audience={quote_plus(selected_audience)}'>Dashboard 2</a>"
    )

    stage_funnel = [
        ("Leads", funnel["leads_7d"]),
        ("Consentidos", funnel["consented_7d"]),
        ("Ofertas", funnel["offers_7d"]),
        ("Vendas", funnel["won_7d"]),
        ("Perdidos", funnel["lost_7d"]),
    ]
    stage_base = max(1, stage_funnel[0][1])
    funnel_rows = ""
    for idx, (label, value) in enumerate(stage_funnel):
        width = max(4, int((value / stage_base) * 100)) if stage_base else 4
        next_value = stage_funnel[idx + 1][1] if idx + 1 < len(stage_funnel) else None
        drop_info = ""
        if next_value is not None:
            drop_abs = max(0, value - next_value)
            drop_pct = (drop_abs / value * 100) if value else 0
            drop_info = f"<span class='funnel-drop'>Queda: {drop_abs} ({drop_pct:.0f}%)</span>"
        funnel_rows += (
            f"<div class='funnel-row'>"
            f"<div class='funnel-head'><span>{_safe(label)}</span><b>{value}</b>{drop_info}</div>"
            f"<div class='meter meter-funnel'><i style='width:{width}%'></i></div>"
            f"</div>"
        )

    pending_rows = "".join(
        (
            f"<tr>"
            f"<td>#{it['id']}</td>"
            f"<td>Lead {it['lead_id']}</td>"
            f"<td>{_safe(it['created_at_utc'])}</td>"
            f"<td><code>{_safe((it['inbound_text'] or '')[:220])}</code></td>"
            f"</tr>"
        )
        for it in queue["top_pending"]
    ) or "<tr><td colspan='4'>Sem pendencias.</td></tr>"

    domain_rows = "".join(
        (
            f"<tr>"
            f"<td>{d['id']}</td>"
            f"<td>{_safe(d['domain_name'] or '-')}</td>"
            f"<td><span class='status-chip {_status_badge(str(d['status']))}'>{_safe(d['status'])}</span></td>"
            f"<td>{d['days_left'] if d['days_left'] is not None else '-'}</td>"
            f"</tr>"
        )
        for d in domains["next_expiring"][:8]
    ) or "<tr><td colspan='4'>Sem dominios com expiracao registrada.</td></tr>"

    channel_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(c['channel'])}</b></td>"
            f"<td><span class='status-chip {_status_badge(str(c['status']))}'>{_safe(c['status'])}</span></td>"
            f"<td>{_safe(c['reason'] or '-')}</td>"
            f"</tr>"
        )
        for c in ops["channels"]
    ) or "<tr><td colspan='3'>Sem canais registrados.</td></tr>"

    country_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(it['country'])}</b></td>"
            f"<td>{it['leads']}</td>"
            f"<td>{(int(it['leads']) / lead_total_geo) * 100:.1f}%</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((int(it['leads']) / lead_total_geo) * 100))}%'></i></div></td>"
            f"</tr>"
        )
        for it in geo["by_country"]
    ) or "<tr><td colspan='4'>Sem dados por pais.</td></tr>"

    approach_channel_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(it['channel'])}</b></td>"
            f"<td>{it['touches']}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((int(it['touches']) / max_channel_touches) * 100))}%'></i></div></td>"
            f"</tr>"
        )
        for it in geo["approaches_by_channel"]
    ) or "<tr><td colspan='3'>Sem abordagens registradas.</td></tr>"

    approach_country_channel_rows = "".join(
        (
            f"<tr>"
            f"<td>{_safe(it['country'])}</td>"
            f"<td>{_safe(it['channel'])}</td>"
            f"<td>{it['touches']}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((int(it['touches']) / max_country_channel_touches) * 100))}%'></i></div></td>"
            f"</tr>"
        )
        for it in geo["approaches_by_country_channel"][:20]
    ) or "<tr><td colspan='4'>Sem cruzamento pais/canal.</td></tr>"

    pace_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(ch)}</b></td>"
            f"<td>{ch_1h.get(ch, 0)}</td>"
            f"<td>{ch_24h.get(ch, 0)}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((ch_24h.get(ch, 0) / pace_max_24h) * 100))}%'></i></div></td>"
            f"<td><div class='meter meter-cool'><i style='width:{max(4, int((ch_1h.get(ch, 0) / pace_max_1h) * 100))}%'></i></div></td>"
            f"</tr>"
        )
        for ch in pace_channels
    ) or "<tr><td colspan='5'>Sem ritmo por canal ainda.</td></tr>"

    max_reply_rate = max([float(it["reply_rate"]) for it in template_perf["rows"]] or [0.01])
    template_rows = "".join(
        (
            f"<tr class='{'is-row-highlight' if float(it['reply_rate']) >= 0.08 else ''}'>"
            f"<td><b>{_safe(it['template_id'])}</b></td>"
            f"<td>{it['sent_count']}</td>"
            f"<td>{it['unique_leads']}</td>"
            f"<td>{it['replied_leads']}</td>"
            f"<td>{it['reply_rate'] * 100:.1f}%</td>"
            f"<td><div class='meter meter-spark'><i style='width:{max(4, int((float(it['reply_rate']) / max_reply_rate) * 100))}%'></i></div></td>"
            f"</tr>"
        )
        for it in template_perf["rows"][:12]
    ) or "<tr><td colspan='6'>Sem dados de template.</td></tr>"

    ab_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(it['template_id'])}</b></td>"
            f"<td>{it['sent_count']}</td>"
            f"<td>{it['replied_leads']}</td>"
            f"<td>{it['reply_rate'] * 100:.1f}%</td>"
            f"</tr>"
        )
        for it in template_perf["ab_v2_handoff"]
    ) or "<tr><td colspan='4'>Sem dados A/B ainda.</td></tr>"

    country_choices = [
        ("ALL", "Geral"),
        ("BR", "Brasil"),
        ("NON_BR", "Fora do BR"),
        ("PT", "Portugal"),
        ("UK", "Reino Unido"),
        ("US", "USA"),
        ("ES", "Espanha"),
    ]
    country_pills = "".join(
        (
            f"<a class='filter-pill {'is-active' if selected_country == value else ''}' "
            f"href='/{'dashboard2' if selected_approach == 'V2' else 'dashboard'}?country={quote_plus(value)}&audience={quote_plus(selected_audience)}'>{label}</a>"
        )
        for value, label in country_choices
    )
    audience_pills = (
        f"<a class='filter-pill {'is-active' if selected_audience == 'ALL' else ''}' "
        f"href='/{'dashboard2' if selected_approach == 'V2' else 'dashboard'}?country={quote_plus(selected_country)}&audience=ALL'>Todos os nichos</a>"
    )
    audience_pills += "".join(
        (
            f"<a class='filter-pill {'is-active' if selected_audience == item['audience'] else ''}' "
            f"href='/{'dashboard2' if selected_approach == 'V2' else 'dashboard'}?country={quote_plus(selected_country)}&audience={quote_plus(item['audience'])}'>"
            f"{_safe(item['audience'])} <span>{item['count']}</span></a>"
        )
        for item in filters["audience_options"]
    )

    alerts: list[tuple[str, str, str]] = []
    if ops["global_safe_mode"]:
        alerts.append(("critical", "GLOBAL SAFE MODE", "Dois canais pausados no mesmo dia. Novos envios devem permanecer bloqueados."))
    paused_channels = [str(c["channel"]) for c in ops["channels"] if str(c.get("status", "")).upper() != "ACTIVE"]
    if paused_channels:
        alerts.append(("warning", "Canal em pausa", f"Canais com restricao: {', '.join(paused_channels[:4])}."))
    fail_rate = (es["contact_failed"] / max(1, es["contact_failed"] + es["contact_delivered"])) * 100
    if fail_rate >= 20:
        alerts.append(("warning", "Falhas de entrega elevadas", f"Falha atual: {fail_rate:.1f}% (ultimos eventos)."))
    if queue_backlog >= 10:
        alerts.append(("warning", "Fila Codex acumulada", f"{queue_backlog} respostas aguardando revisao."))
    if throughput["touches_1h_total"] == 0 and throughput["touches_24h_total"] > 0:
        alerts.append(("info", "Ritmo momentaneamente baixo", "Ultima hora sem disparos, mas houve atividade nas ultimas 24h."))
    if not alerts:
        alerts.append(("ok", "Operacao estavel", "Sem alertas criticos no momento."))

    alert_rows = "".join(
        (
            f"<div class='alert-card alert-{_safe(level)}'>"
            f"<div class='alert-title'>{_safe(title)}</div>"
            f"<div class='alert-body'>{_safe(body)}</div>"
            f"</div>"
        )
        for level, title, body in alerts
    )

    event_feed = ""
    for e in reversed(snap["events"][-20:]):
        ts = _safe(e.get("timestamp_utc", ""))
        ev = str(e.get("event_type", ""))
        payload_txt = json.dumps(e.get("payload", {}), ensure_ascii=False)
        if len(payload_txt) > 260:
            payload_txt = payload_txt[:260] + "..."
        severity = "neutral"
        low_ev = ev.lower()
        if "failed" in low_ev or "error" in low_ev or "paused" in low_ev or "down" in low_ev:
            severity = "bad"
        elif "sent" in low_ev or "sale" in low_ev or "up" in low_ev:
            severity = "ok"
        elif "queued" in low_ev or "review" in low_ev:
            severity = "warn"
        event_feed += (
            f"<article class='event-card sev-{severity}'>"
            f"<div class='event-head'><span class='event-type'>{_safe(ev)}</span><span>{ts}</span></div>"
            f"<code>{_safe(payload_txt)}</code>"
            f"</article>"
        )
    if not event_feed:
        event_feed = "<div class='muted'>Sem eventos ainda.</div>"

    return f"""<!doctype html>
<html lang='pt-BR'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <meta http-equiv='refresh' content='10'/>
  <title>LeadGenerator - Dashboard Comercial</title>
  <style>
    :root {{
      --bg:#f4f7fb;
      --bg-soft:#e8eef7;
      --card:#ffffff;
      --card-soft:#f7fafc;
      --text:#111827;
      --muted:#4b5563;
      --line:#dbe3ee;
      --brand:#0f766e;
      --brand-2:#0f4c81;
      --accent:#1d4ed8;
      --ok:#15803d;
      --warn:#b45309;
      --bad:#b91c1c;
      --neutral:#334155;
      --shadow:0 10px 28px rgba(15, 23, 42, 0.09);
    }}
    html[data-theme='dark'] {{
      --bg:#0b1220;
      --bg-soft:#111a2d;
      --card:#121c2f;
      --card-soft:#0f172a;
      --text:#e5e7eb;
      --muted:#9ca3af;
      --line:#23304a;
      --brand:#2dd4bf;
      --brand-2:#38bdf8;
      --accent:#60a5fa;
      --ok:#22c55e;
      --warn:#f59e0b;
      --bad:#ef4444;
      --neutral:#94a3b8;
      --shadow:0 10px 30px rgba(2, 6, 23, 0.5);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--text);
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      background:
        radial-gradient(1200px 500px at 110% -20%, rgba(15, 118, 110, .16), transparent 72%),
        radial-gradient(1000px 420px at -15% -20%, rgba(15, 76, 129, .18), transparent 78%),
        linear-gradient(180deg, var(--bg-soft), var(--bg));
    }}
    .page {{ max-width: 1420px; margin: 0 auto; padding: 14px 16px 28px; }}
    .topbar {{
      position: sticky;
      top: 0;
      z-index: 11;
      margin-bottom: 10px;
      backdrop-filter: blur(8px);
      background: color-mix(in srgb, var(--bg-soft) 70%, transparent);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 9px 12px;
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }}
    .chip-row {{ display: flex; gap: 7px; flex-wrap: wrap; align-items: center; }}
    .chip {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 12px;
      font-weight: 700;
      background: var(--card);
      color: var(--muted);
      transition: .2s ease;
    }}
    .chip b {{ color: var(--text); }}
    .chip.is-ok {{ border-color: color-mix(in srgb, var(--ok) 42%, var(--line)); color: var(--ok); }}
    .chip.is-warn {{ border-color: color-mix(in srgb, var(--warn) 42%, var(--line)); color: var(--warn); }}
    .chip.is-bad {{ border-color: color-mix(in srgb, var(--bad) 42%, var(--line)); color: var(--bad); }}
    .chip.is-neutral {{ border-color: var(--line); color: var(--neutral); }}
    .theme-toggle {{
      border: 1px solid var(--line);
      background: var(--card);
      color: var(--text);
      border-radius: 10px;
      padding: 7px 10px;
      font-size: 12px;
      cursor: pointer;
      font-weight: 700;
    }}
    .hero {{
      border: 1px solid var(--line);
      background: linear-gradient(130deg, var(--card) 0%, color-mix(in srgb, var(--accent) 8%, var(--card)) 45%, color-mix(in srgb, var(--brand) 14%, var(--card)) 100%);
      border-radius: 16px;
      box-shadow: var(--shadow);
      padding: 16px;
      margin-bottom: 10px;
    }}
    .hero-head {{
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }}
    .hero h1 {{ margin: 0; font-size: 27px; letter-spacing: 0.2px; }}
    .hero .subtitle {{
      margin-top: 4px;
      color: var(--muted);
      font-size: 13px;
    }}
    .filters {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 10px;
    }}
    .filter-pill {{
      text-decoration: none;
      border-radius: 999px;
      padding: 6px 11px;
      border: 1px solid var(--line);
      background: var(--card);
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      display: inline-flex;
      gap: 6px;
      align-items: center;
      transition: .18s ease;
    }}
    .filter-pill:hover {{
      transform: translateY(-1px);
      border-color: color-mix(in srgb, var(--accent) 50%, var(--line));
      color: var(--text);
    }}
    .filter-pill span {{ color: var(--accent); }}
    .filter-pill.is-active {{
      background: linear-gradient(120deg, color-mix(in srgb, var(--accent) 15%, var(--card)), color-mix(in srgb, var(--brand) 16%, var(--card)));
      border-color: color-mix(in srgb, var(--accent) 40%, var(--line));
      color: var(--text);
    }}
    .alerts {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 10px;
      margin-bottom: 10px;
    }}
    .alert-card {{
      border: 1px solid var(--line);
      border-radius: 12px;
      background: var(--card);
      padding: 10px 12px;
      box-shadow: var(--shadow);
    }}
    .alert-card.alert-critical {{
      border-color: color-mix(in srgb, var(--bad) 45%, var(--line));
      background: color-mix(in srgb, var(--bad) 8%, var(--card));
    }}
    .alert-card.alert-warning {{
      border-color: color-mix(in srgb, var(--warn) 45%, var(--line));
      background: color-mix(in srgb, var(--warn) 8%, var(--card));
    }}
    .alert-card.alert-info {{
      border-color: color-mix(in srgb, var(--accent) 45%, var(--line));
      background: color-mix(in srgb, var(--accent) 8%, var(--card));
    }}
    .alert-card.alert-ok {{
      border-color: color-mix(in srgb, var(--ok) 45%, var(--line));
      background: color-mix(in srgb, var(--ok) 8%, var(--card));
    }}
    .alert-title {{ font-size: 12px; font-weight: 800; letter-spacing: 0.2px; text-transform: uppercase; }}
    .alert-body {{ margin-top: 4px; font-size: 13px; color: var(--muted); line-height: 1.35; }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(215px, 1fr));
      gap: 10px;
      margin-bottom: 10px;
    }}
    .card {{
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--card);
      box-shadow: var(--shadow);
      padding: 12px;
    }}
    .card-title {{
      margin: 0 0 8px;
      font-size: 15px;
      letter-spacing: 0.2px;
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.55px;
      font-weight: 700;
    }}
    .kpi-value {{
      font-size: 30px;
      font-weight: 800;
      line-height: 1.05;
      margin-top: 3px;
      letter-spacing: -0.2px;
    }}
    .kpi-foot {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
    }}
    .pill-row {{ margin-top: 6px; display: flex; gap: 7px; flex-wrap: wrap; }}
    .pill {{
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 11px;
      color: var(--muted);
      background: var(--card-soft);
      font-weight: 700;
    }}
    .progress {{
      margin-top: 8px;
      width: 100%;
      height: 10px;
      border-radius: 999px;
      overflow: hidden;
      background: color-mix(in srgb, var(--line) 70%, var(--card-soft));
    }}
    .progress > i {{
      display: block;
      height: 100%;
      width: {progress_pct}%;
      background: linear-gradient(90deg, var(--brand), var(--accent), var(--brand-2));
      transition: width .25s ease;
    }}
    .main-layout {{
      display: grid;
      gap: 10px;
      grid-template-columns: 2fr 1fr;
      margin-bottom: 10px;
    }}
    .sub-grid {{
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      margin-bottom: 10px;
    }}
    .funnel-stack {{ display: grid; gap: 8px; }}
    .funnel-row {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 8px;
      background: var(--card-soft);
    }}
    .funnel-head {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 6px;
      font-size: 13px;
    }}
    .funnel-head b {{ font-size: 16px; }}
    .funnel-drop {{
      margin-left: auto;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
    }}
    .meter {{
      width: 100%;
      height: 8px;
      border-radius: 999px;
      overflow: hidden;
      background: color-mix(in srgb, var(--line) 75%, var(--card-soft));
      min-width: 110px;
    }}
    .meter > i {{
      display: block;
      height: 100%;
      background: linear-gradient(90deg, var(--brand), var(--accent));
    }}
    .meter-funnel > i {{
      background: linear-gradient(90deg, color-mix(in srgb, var(--brand) 80%, white), color-mix(in srgb, var(--accent) 70%, white));
    }}
    .meter-cool > i {{
      background: linear-gradient(90deg, #0ea5e9, #6366f1);
    }}
    .meter-spark > i {{
      background: linear-gradient(90deg, #22c55e, #06b6d4);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      text-align: left;
      padding: 7px 6px;
      vertical-align: middle;
    }}
    th {{
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      position: sticky;
      top: 0;
      background: var(--card);
      z-index: 1;
    }}
    .scroll-table {{
      max-height: 360px;
      overflow: auto;
      border-radius: 10px;
      border: 1px solid var(--line);
    }}
    .scroll-table table th:first-child {{ padding-left: 10px; }}
    .scroll-table table td:first-child {{ padding-left: 10px; }}
    .status-chip {{
      border-radius: 999px;
      padding: 3px 8px;
      border: 1px solid var(--line);
      font-size: 11px;
      font-weight: 800;
      letter-spacing: 0.3px;
      text-transform: uppercase;
    }}
    .status-chip.is-ok {{ color: var(--ok); border-color: color-mix(in srgb, var(--ok) 42%, var(--line)); }}
    .status-chip.is-warn {{ color: var(--warn); border-color: color-mix(in srgb, var(--warn) 42%, var(--line)); }}
    .status-chip.is-bad {{ color: var(--bad); border-color: color-mix(in srgb, var(--bad) 42%, var(--line)); }}
    .status-chip.is-neutral {{ color: var(--neutral); }}
    .is-row-highlight {{
      background: color-mix(in srgb, var(--ok) 8%, var(--card));
    }}
    .aside-stack {{
      display: grid;
      gap: 10px;
      align-content: start;
    }}
    .event-feed {{
      display: grid;
      gap: 8px;
      max-height: 500px;
      overflow: auto;
      padding-right: 2px;
    }}
    .event-card {{
      border: 1px solid var(--line);
      border-radius: 10px;
      background: var(--card-soft);
      padding: 8px;
    }}
    .event-card.sev-ok {{ border-left: 4px solid var(--ok); }}
    .event-card.sev-warn {{ border-left: 4px solid var(--warn); }}
    .event-card.sev-bad {{ border-left: 4px solid var(--bad); }}
    .event-card.sev-neutral {{ border-left: 4px solid var(--neutral); }}
    .event-head {{
      display: flex;
      justify-content: space-between;
      gap: 6px;
      font-size: 11px;
      color: var(--muted);
      margin-bottom: 6px;
    }}
    .event-type {{
      font-weight: 800;
      color: var(--text);
      font-size: 12px;
    }}
    .event-card code {{
      white-space: pre-wrap;
      word-break: break-word;
      display: block;
      color: var(--muted);
      font-size: 11px;
    }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    .legend {{
      display: flex;
      gap: 7px;
      flex-wrap: wrap;
      margin-top: 6px;
    }}
    .legend .chip {{ font-weight: 700; }}
    @media (max-width: 1180px) {{
      .main-layout {{ grid-template-columns: 1fr; }}
    }}
    @media (max-width: 920px) {{
      .kpi-grid {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }}
      .sub-grid {{ grid-template-columns: 1fr; }}
      .kpi-value {{ font-size: 24px; }}
      .hero h1 {{ font-size: 23px; }}
    }}
  </style>
</head>
<body>
  <div class='page'>
    <header class='topbar'>
      <div class='chip-row'>
        <span class='chip {safe_class}'>Sistema: <b>{safe_mode}</b></span>
        <span class='chip {activity_class}'>Ultima atividade: <b>{activity_txt}</b></span>
        <span class='chip is-neutral'>Escopo: <b>{_safe(current_scope)}</b></span>
        <span class='chip is-neutral'>Fluxo: <b>{_safe(dashboard_badge)}</b></span>
        <span class='chip {queue_class}'>Fila Codex: <b>{queue_backlog}</b></span>
      </div>
      <button class='theme-toggle' id='themeToggle' type='button'>Alternar tema</button>
    </header>

    <section class='hero'>
      <div class='hero-head'>
        <div>
          <h1>{_safe(dashboard_title)}</h1>
          <div class='subtitle'>Atualizado em {_safe(snap['generated_at_utc'])} (UTC){_safe(scope_suffix)}. Refresco automatico a cada 10 segundos.</div>
        </div>
        <div class='chip-row'>
          <span class='chip is-neutral'>Abordagens 1h: <b>{throughput['touches_1h_total']}</b></span>
          <span class='chip is-neutral'>Abordagens 24h: <b>{throughput['touches_24h_total']}</b></span>
          <span class='chip is-neutral'>Leads 24h: <b>{throughput['new_leads_24h']}</b></span>
          <span class='chip is-neutral'>Respostas 24h: <b>{throughput['replies_24h']}</b></span>
        </div>
      </div>
      <div class='filters'>{dashboard_nav}</div>
      <div class='filters'>{country_pills}</div>
      <div class='filters'>{audience_pills}</div>
    </section>

    <section class='alerts'>{alert_rows}</section>

    <section class='kpi-grid'>
      <article class='card'>
        <div class='kpi-label'>Preco atual</div>
        <div class='kpi-value'>R$ {pricing['price_full']} / R$ {pricing['price_simple']}</div>
        <div class='pill-row'>
          <span class='pill'>Nivel {pricing['price_level']}</span>
          <span class='pill'>Baseline {baseline_txt}</span>
        </div>
      </article>
      <article class='card'>
        <div class='kpi-label'>Conversao 7d</div>
        <div class='kpi-value'>{conv_txt}</div>
        <div class='kpi-foot'>Vendas: {funnel['won_7d']} | Ofertas: {funnel['offers_7d']}</div>
      </article>
      <article class='card'>
        <div class='kpi-label'>Bloco de preco</div>
        <div class='kpi-value'>{pricing['offers_in_window']}/10</div>
        <div class='progress'><i></i></div>
        <div class='kpi-foot'>Progresso para decisao do proximo degrau.</div>
      </article>
      <article class='card'>
        <div class='kpi-label'>Vendas em 7 dias</div>
        <div class='kpi-value'>{funnel['won_7d']}</div>
        <div class='kpi-foot'>Tempo medio ate venda: {funnel['avg_days_to_win_7d']:.1f} dias</div>
      </article>
      <article class='card'>
        <div class='kpi-label'>Receita estimada 7d</div>
        <div class='kpi-value'>R$ {funnel['revenue_estimated_7d']:.0f}</div>
        <div class='kpi-foot'>Somente oportunidades marcadas como WON.</div>
      </article>
      <article class='card'>
        <div class='kpi-label'>Revisao Codex</div>
        <div class='kpi-value'>{queue_backlog}</div>
        <div class='pill-row'>
          <span class='pill'>Pendente: {queue['counts']['pending']}</span>
          <span class='pill'>Revisao: {queue['counts']['review_required']}</span>
        </div>
        <div class='kpi-foot'>{queue_label}</div>
      </article>
    </section>

    <section class='main-layout'>
      <div>
        <article class='card' style='margin-bottom:10px;'>
          <h2 class='card-title'>Funil Comercial (7 dias)</h2>
          <div class='funnel-stack'>{funnel_rows}</div>
          <div class='legend'>
            <span class='chip is-neutral'>Leads: {funnel['leads_7d']}</span>
            <span class='chip is-neutral'>Consentidos: {funnel['consented_7d']}</span>
            <span class='chip is-neutral'>Ofertas: {funnel['offers_7d']}</span>
            <span class='chip is-ok'>Vendas: {funnel['won_7d']}</span>
            <span class='chip is-warn'>Perdidos: {funnel['lost_7d']}</span>
          </div>
        </article>

        <article class='card' style='margin-bottom:10px;'>
          <h2 class='card-title'>Ritmo por Canal (1h x 24h)</h2>
          <div class='scroll-table'>
            <table>
              <thead><tr><th>Canal</th><th>1h</th><th>24h</th><th>Volume 24h</th><th>Intensidade 1h</th></tr></thead>
              <tbody>{pace_rows}</tbody>
            </table>
          </div>
        </article>

        <article class='card'>
          <h2 class='card-title'>Performance por Template (Email)</h2>
          <div class='scroll-table'>
            <table>
              <thead><tr><th>Template</th><th>Envios</th><th>Leads</th><th>Respostas</th><th>Taxa</th><th>Tendencia</th></tr></thead>
              <tbody>{template_rows}</tbody>
            </table>
          </div>
        </article>
      </div>

      <aside class='aside-stack'>
        <article class='card'>
          <h2 class='card-title'>Saude Operacional</h2>
          <div class='scroll-table'>
            <table>
              <thead><tr><th>Canal</th><th>Status</th><th>Motivo</th></tr></thead>
              <tbody>{channel_rows}</tbody>
            </table>
          </div>
          <div class='muted' style='margin-top:8px;'>
            Entregues: {es['contact_delivered']} | Falhas: {es['contact_failed']} | Ofertas: {es['offer_sent']} | Sales: {es['sale_marked']}
          </div>
        </article>

        <article class='card'>
          <h2 class='card-title'>Revisao Codex pendente</h2>
          <div class='scroll-table'>
            <table>
              <thead><tr><th>ID</th><th>Lead</th><th>Recebido</th><th>Mensagem</th></tr></thead>
              <tbody>{pending_rows}</tbody>
            </table>
          </div>
        </article>

        <article class='card'>
          <h2 class='card-title'>Dominios em implantacao</h2>
          <div class='muted'>Total jobs: {domains['total_jobs']} | Em andamento: {domains['in_progress']}</div>
          <div class='scroll-table' style='margin-top:8px; max-height:280px;'>
            <table>
              <thead><tr><th>Job</th><th>Dominio</th><th>Status</th><th>Dias</th></tr></thead>
              <tbody>{domain_rows}</tbody>
            </table>
          </div>
        </article>

        <article class='card'>
          <h2 class='card-title'>Timeline recente</h2>
          <div class='event-feed'>{event_feed}</div>
        </article>
      </aside>
    </section>

    <section class='sub-grid'>
      <article class='card'>
        <h2 class='card-title'>Leads por Pais</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Pais</th><th>Leads</th><th>%</th><th>Participacao</th></tr></thead>
            <tbody>{country_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Abordagens por Canal</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Canal</th><th>Abordagens</th><th>Volume relativo</th></tr></thead>
            <tbody>{approach_channel_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>A/B Fluxo 2 (Handoff)</h2>
        <div class='muted'>Variantes atribuídas: {es['ab_variant_assigned']}</div>
        <div class='scroll-table' style='margin-top:8px; max-height:240px;'>
          <table>
            <thead><tr><th>Variante</th><th>Envios</th><th>Respostas</th><th>Taxa</th></tr></thead>
            <tbody>{ab_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Abordagens por Pais x Canal</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Pais</th><th>Canal</th><th>Abordagens</th><th>Forca</th></tr></thead>
            <tbody>{approach_country_channel_rows}</tbody>
          </table>
        </div>
      </article>
    </section>
  </div>
  <script>
    (function() {{
      var root = document.documentElement;
      var btn = document.getElementById('themeToggle');
      var stored = localStorage.getItem('leadgen_theme');
      if (!stored) {{
        stored = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
      }}
      root.setAttribute('data-theme', stored);
      btn.textContent = stored === 'dark' ? 'Tema claro' : 'Tema escuro';
      btn.addEventListener('click', function() {{
        var current = root.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
        var next = current === 'dark' ? 'light' : 'dark';
        root.setAttribute('data-theme', next);
        localStorage.setItem('leadgen_theme', next);
        btn.textContent = next === 'dark' ? 'Tema claro' : 'Tema escuro';
      }});
    }})();
  </script>
</body>
</html>"""


def run_dashboard(host: str = "0.0.0.0", port: int = 8789) -> None:
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.serve_forever()
