from __future__ import annotations

import html as html_lib
import json
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import monotonic
from typing import Any
from urllib.parse import quote_plus, unquote_plus

from .config import get_config
from .crm_store import CrmStore


_SNAPSHOT_CACHE_TTL_SECONDS = 4.0
_HTML_CACHE_TTL_SECONDS = 4.0
_SNAPSHOT_CACHE: dict[tuple[str, str, str, tuple[tuple[str, int, int], ...]], tuple[float, dict[str, Any]]] = {}
_HTML_CACHE: dict[tuple[str, str, str, tuple[tuple[str, int, int], ...]], tuple[float, str]] = {}


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


def _path_signature(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return (0, 0)
    return (int(stat.st_mtime_ns), int(stat.st_size))


def _snapshot_signature(cfg: Any) -> tuple[tuple[str, int, int], ...]:
    return (
        ("state_db", *_path_signature(cfg.state_db)),
        ("ops_state_db", *_path_signature(cfg.ops_state_db)),
        ("events_jsonl", *_path_signature(cfg.log_dir / "events.jsonl")),
    )


def _events_in_window(events: list[dict[str, Any]], *, hours: int | None = None, days: int | None = None) -> list[dict[str, Any]]:
    if hours is None and days is None:
        return events
    delta = timedelta(hours=hours or 0, days=days or 0)
    since = datetime.now(timezone.utc) - delta
    out: list[dict[str, Any]] = []
    for event in events:
        ts = _parse_utc(str(event.get("timestamp_utc", "")))
        if ts and ts >= since:
            out.append(event)
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


def _reply_stage_snapshot(
    db_path: Path,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {
        "stages": [
            {"order": 1, "template_id": "email_identity_v1", "label": "1º email", "sent_count": 0, "replied_count": 0, "eligible_count": 0, "skipped_count": 0, "skip_reasons_top": [], "sample_message": "", "details": [], "reply_examples": []},
            {"order": 2, "template_id": "email_followup_1", "label": "2º email", "sent_count": 0, "replied_count": 0, "eligible_count": 0, "skipped_count": 0, "skip_reasons_top": [], "sample_message": "", "details": [], "reply_examples": []},
            {"order": 3, "template_id": "email_followup_2", "label": "3º email", "sent_count": 0, "replied_count": 0, "eligible_count": 0, "skipped_count": 0, "skip_reasons_top": [], "sample_message": "", "details": [], "reply_examples": []},
        ]
    }
    if not db_path.exists():
        return defaults
    templates = [("email_identity_v1", 1, "1º email"), ("email_followup_1", 2, "2º email"), ("email_followup_2", 3, "3º email")]
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            lead_where = f" AND {' AND '.join(clauses)}" if clauses else ""
            placeholders = ",".join(["?"] * len(templates))
            template_ids = [tpl for tpl, _, _ in templates]

            sent_rows = conn.execute(
                f"""
                SELECT t.template_id, COUNT(*)
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE t.channel='EMAIL'
                  AND t.template_id IN ({placeholders})
                  {lead_where}
                GROUP BY t.template_id
                """,
                [*template_ids, *params],
            ).fetchall()
            sent_map = {str(r[0]): int(r[1]) for r in sent_rows}

            # Descobre qual foi a última touch antes da primeira reply do lead.
            replied_rows = conn.execute(
                f"""
                WITH ranked_replies AS (
                  SELECT r.lead_id, r.timestamp_utc, r.body,
                         ROW_NUMBER() OVER (PARTITION BY r.lead_id ORDER BY r.timestamp_utc ASC, r.id ASC) AS rn
                  FROM replies r
                ),
                first_reply AS (
                  SELECT lead_id, timestamp_utc, body
                  FROM ranked_replies
                  WHERE rn = 1
                ),
                ranked_touches AS (
                  SELECT t.lead_id, t.template_id, t.timestamp_utc,
                         ROW_NUMBER() OVER (PARTITION BY t.lead_id ORDER BY t.timestamp_utc DESC, t.id DESC) AS rn
                  FROM touches t
                  JOIN first_reply fr ON fr.lead_id = t.lead_id AND t.timestamp_utc <= fr.timestamp_utc
                  JOIN leads l ON l.id = t.lead_id
                  WHERE t.channel='EMAIL'
                    AND t.template_id IN ({placeholders})
                    {lead_where}
                )
                SELECT rt.template_id, COUNT(*)
                FROM ranked_touches rt
                WHERE rt.rn = 1
                GROUP BY rt.template_id
                """,
                [*template_ids, *params],
            ).fetchall()
            replied_map = {str(r[0]): int(r[1]) for r in replied_rows}

            sample_rows = conn.execute(
                f"""
                SELECT t.template_id, t.body
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE t.channel='EMAIL'
                  AND t.template_id IN ({placeholders})
                  {lead_where}
                  AND trim(COALESCE(t.body, '')) != ''
                ORDER BY t.id DESC
                LIMIT 300
                """,
                [*template_ids, *params],
            ).fetchall()
            sample_map: dict[str, str] = {}
            for tpl, body in sample_rows:
                key = str(tpl or "")
                if key and key not in sample_map and str(body or "").strip():
                    sample_map[key] = str(body or "").strip()

            detail_rows = conn.execute(
                f"""
                WITH ranked_replies AS (
                  SELECT r.lead_id, r.timestamp_utc, r.body,
                         ROW_NUMBER() OVER (PARTITION BY r.lead_id ORDER BY r.timestamp_utc ASC, r.id ASC) AS rn
                  FROM replies r
                ),
                first_reply AS (
                  SELECT lead_id, timestamp_utc, body
                  FROM ranked_replies
                  WHERE rn = 1
                ),
                ranked_touches AS (
                  SELECT t.lead_id, t.template_id, t.timestamp_utc,
                         ROW_NUMBER() OVER (PARTITION BY t.lead_id ORDER BY t.timestamp_utc DESC, t.id DESC) AS rn
                  FROM touches t
                  JOIN first_reply fr ON fr.lead_id = t.lead_id AND t.timestamp_utc <= fr.timestamp_utc
                  JOIN leads l ON l.id = t.lead_id
                  WHERE t.channel='EMAIL'
                    AND t.template_id IN ({placeholders})
                    {lead_where}
                )
                SELECT rt.template_id, l.id, l.business_name, l.email, fr.timestamp_utc, fr.body
                FROM ranked_touches rt
                JOIN first_reply fr ON fr.lead_id = rt.lead_id
                JOIN leads l ON l.id = rt.lead_id
                WHERE rt.rn = 1
                ORDER BY fr.timestamp_utc DESC
                LIMIT 120
                """,
                [*template_ids, *params],
            ).fetchall()

    except sqlite3.Error:
        return defaults

    details_map: dict[str, list[dict[str, Any]]] = {tpl: [] for tpl, _, _ in templates}
    for tpl, lead_id, business_name, email, ts, body in detail_rows:
        key = str(tpl or "")
        if key not in details_map:
            continue
        if len(details_map[key]) >= 30:
            continue
        details_map[key].append(
            {
                "lead_id": int(lead_id),
                "business_name": str(business_name or ""),
                "email": str(email or ""),
                "timestamp_utc": str(ts or ""),
                "reply_body": str(body or ""),
            }
        )

    stages: list[dict[str, Any]] = []
    for tpl, order, label in templates:
        reply_examples = details_map.get(tpl, [])[:5]
        stages.append(
            {
                "order": order,
                "template_id": tpl,
                "label": label,
                "sent_count": int(sent_map.get(tpl, 0)),
                "replied_count": int(replied_map.get(tpl, 0)),
                "eligible_count": int(sent_map.get(tpl, 0)),
                "skipped_count": 0,
                "skip_reasons_top": [],
                "sample_message": sample_map.get(tpl, ""),
                "details": details_map.get(tpl, []),
                "reply_examples": reply_examples,
            }
        )
    return {"stages": stages}


def _reply_attribution_snapshot(
    db_path: Path,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {
        "human_replies_count": 0,
        "auto_replies_count": 0,
        "unknown_replies_count": 0,
        "rows": [],
    }
    if not db_path.exists():
        return defaults
    human_classes = {"positive", "negative", "not_now", "objection_price", "objection_trust", "neutral", "opt_out"}
    auto_classes = {"auto_reply", "shared_inbox"}
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            where_clause = " AND ".join([*clauses, "r.channel='EMAIL'"]) if clauses else "r.channel='EMAIL'"
            rows = conn.execute(
                f"""
                SELECT r.classification, COUNT(*)
                FROM replies r
                JOIN leads l ON l.id = r.lead_id
                WHERE {where_clause}
                GROUP BY r.classification
                ORDER BY COUNT(*) DESC
                """,
                params,
            ).fetchall()
    except sqlite3.Error:
        return defaults
    out_rows = [{"classification": str(r[0] or ""), "count": int(r[1] or 0)} for r in rows]
    human = sum(r["count"] for r in out_rows if r["classification"] in human_classes)
    auto = sum(r["count"] for r in out_rows if r["classification"] in auto_classes)
    unknown = sum(r["count"] for r in out_rows if r["classification"] not in human_classes and r["classification"] not in auto_classes)
    return {
        "human_replies_count": human,
        "auto_replies_count": auto,
        "unknown_replies_count": unknown,
        "rows": out_rows,
    }


def _email_coverage_snapshot(
    db_path: Path,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {"rows": [], "contactable_7d": 0, "leads_7d": 0, "coverage_rate_7d": 0.0}
    if not db_path.exists():
        return defaults
    since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
            where_all = " AND ".join([*clauses, "created_at_utc >= ?"]) if clauses else "created_at_utc >= ?"
            where_contactable = (
                " AND ".join([*clauses, "created_at_utc >= ?", "trim(COALESCE(email, '')) != ''"])
                if clauses
                else "created_at_utc >= ? AND trim(COALESCE(email, '')) != ''"
            )
            leads_7d = int(conn.execute(f"SELECT COUNT(*) FROM leads WHERE {where_all}", [*params, since_7d]).fetchone()[0])
            contactable_7d = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM leads WHERE {where_contactable}",
                    [*params, since_7d],
                ).fetchone()[0]
            )
            lead_clauses, lead_params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
            where_sql = f"WHERE {' AND '.join(lead_clauses)}" if lead_clauses else ""
            rows = conn.execute(
                f"""
                SELECT COALESCE(country_code, ''), COUNT(*),
                       SUM(CASE WHEN trim(COALESCE(email, '')) != '' THEN 1 ELSE 0 END)
                FROM leads
                {where_sql}
                GROUP BY COALESCE(country_code, '')
                ORDER BY COUNT(*) DESC
                """,
                lead_params,
            ).fetchall()
    except sqlite3.Error:
        return defaults
    out_rows: list[dict[str, Any]] = []
    for country_code, total, contactable in rows:
        total_i = int(total or 0)
        contact_i = int(contactable or 0)
        out_rows.append(
            {
                "country": _derive_country("", "", str(country_code or "")),
                "leads": total_i,
                "contactable": contact_i,
                "coverage_rate": (contact_i / total_i) if total_i else 0.0,
            }
        )
    return {
        "rows": out_rows,
        "contactable_7d": contactable_7d,
        "leads_7d": leads_7d,
        "coverage_rate_7d": (contactable_7d / leads_7d) if leads_7d else 0.0,
    }


def _top_audience_snapshot(
    db_path: Path,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {"rows": []}
    if not db_path.exists():
        return defaults
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            where_sent = " AND ".join([*clauses, "t.channel='EMAIL'"]) if clauses else "t.channel='EMAIL'"
            rows = conn.execute(
                f"""
                SELECT trim(COALESCE(l.audience, 'sem nicho')) AS audience_value,
                       COUNT(*) AS sent_count,
                       COUNT(DISTINCT CASE
                         WHEN EXISTS (
                           SELECT 1 FROM replies r
                           WHERE r.lead_id = t.lead_id
                             AND r.channel='EMAIL'
                             AND r.timestamp_utc >= t.timestamp_utc
                         ) THEN t.lead_id
                         ELSE NULL
                       END) AS replied_count
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE {where_sent}
                GROUP BY lower(trim(COALESCE(l.audience, 'sem nicho')))
                ORDER BY replied_count DESC, sent_count DESC, audience_value ASC
                LIMIT 5
                """,
                params,
            ).fetchall()
    except sqlite3.Error:
        return defaults
    out = []
    for audience_value, sent_count, replied_count in rows:
        sent_i = int(sent_count or 0)
        replied_i = int(replied_count or 0)
        out.append(
            {
                "audience": str(audience_value or "sem nicho"),
                "sent_count": sent_i,
                "replied_count": replied_i,
                "reply_rate": (replied_i / sent_i) if sent_i else 0.0,
            }
        )
    return {"rows": out}


def _top_country_snapshot(
    db_path: Path,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    defaults = {"rows": []}
    if not db_path.exists():
        return defaults
    try:
        with sqlite3.connect(db_path) as conn:
            clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter, "l")
            where_sent = " AND ".join([*clauses, "t.channel='EMAIL'"]) if clauses else "t.channel='EMAIL'"
            rows = conn.execute(
                f"""
                SELECT COALESCE(l.country_code, '') AS country_code,
                       COUNT(*) AS sent_count,
                       COUNT(DISTINCT CASE
                         WHEN EXISTS (
                           SELECT 1 FROM replies r
                           WHERE r.lead_id = t.lead_id
                             AND r.channel='EMAIL'
                             AND r.timestamp_utc >= t.timestamp_utc
                         ) THEN t.lead_id
                         ELSE NULL
                       END) AS replied_count
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                WHERE {where_sent}
                GROUP BY COALESCE(l.country_code, '')
                ORDER BY replied_count DESC, sent_count DESC, country_code ASC
                LIMIT 5
                """,
                params,
            ).fetchall()
    except sqlite3.Error:
        return defaults
    out = []
    for country_code, sent_count, replied_count in rows:
        sent_i = int(sent_count or 0)
        replied_i = int(replied_count or 0)
        out.append(
            {
                "country": _derive_country("", "", str(country_code or "")),
                "sent_count": sent_i,
                "replied_count": replied_i,
                "reply_rate": (replied_i / sent_i) if sent_i else 0.0,
            }
        )
    return {"rows": out}


def _recent_blockers_snapshot(events: list[dict[str, Any]]) -> dict[str, Any]:
    recent_24h = _events_in_window(events, hours=24)
    lead_skipped = Counter()
    followup_skipped = Counter()
    timeout_counter = Counter()
    for event in recent_24h:
        payload = event.get("payload") or {}
        reason = str(payload.get("reason") or payload.get("detail") or payload.get("step") or "unknown")
        event_type = str(event.get("event_type") or "")
        if event_type == "lead_skipped":
            lead_skipped[reason] += 1
        elif event_type == "followup_skipped":
            followup_skipped[reason] += 1
        elif event_type == "campaign_step_timeout":
            timeout_counter[reason] += 1
    return {
        "lead_skipped_top": [{"reason": k, "count": v} for k, v in lead_skipped.most_common(6)],
        "followup_skipped_top": [{"reason": k, "count": v} for k, v in followup_skipped.most_common(6)],
        "timeouts_top": [{"reason": k, "count": v} for k, v in timeout_counter.most_common(6)],
        "lead_skipped_total": sum(lead_skipped.values()),
        "followup_skipped_total": sum(followup_skipped.values()),
        "timeouts_total": sum(timeout_counter.values()),
    }


def _owner_summary_snapshot(
    db_path: Path,
    events: list[dict[str, Any]],
    reply_stage: dict[str, Any],
    throughput: dict[str, Any],
    funnel: dict[str, Any],
    reply_attr: dict[str, Any],
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> dict[str, Any]:
    since_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    since_7d = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    leads_24h = 0
    waiting_confirmation = 0
    ready_to_approach = 0
    if db_path.exists():
        try:
            with sqlite3.connect(db_path) as conn:
                clauses, params = _lead_filter_clauses(country_filter, audience_filter, approach_filter)
                where_24h = " AND ".join([*clauses, "created_at_utc >= ?"]) if clauses else "created_at_utc >= ?"
                where_ready = " AND ".join([*clauses, "stage='QUALIFIED'"]) if clauses else "stage='QUALIFIED'"
                where_waiting = " AND ".join([*clauses, "stage='VERIFY_WAITING'"]) if clauses else "stage='VERIFY_WAITING'"
                leads_24h = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM leads WHERE {where_24h}",
                        [*params, since_24h],
                    ).fetchone()[0]
                )
                ready_to_approach = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM leads WHERE {where_ready}",
                        params,
                    ).fetchone()[0]
                )
                waiting_confirmation = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM leads WHERE {where_waiting}",
                        params,
                    ).fetchone()[0]
                )
        except sqlite3.Error:
            pass
    blockers = _recent_blockers_snapshot(events)
    sent_1 = int(reply_stage["stages"][0]["sent_count"]) if reply_stage["stages"] else 0
    sent_2 = int(reply_stage["stages"][1]["sent_count"]) if len(reply_stage["stages"]) > 1 else 0
    sent_3 = int(reply_stage["stages"][2]["sent_count"]) if len(reply_stage["stages"]) > 2 else 0
    replied_1 = int(reply_stage["stages"][0]["replied_count"]) if reply_stage["stages"] else 0
    replied_2 = int(reply_stage["stages"][1]["replied_count"]) if len(reply_stage["stages"]) > 1 else 0
    replied_3 = int(reply_stage["stages"][2]["replied_count"]) if len(reply_stage["stages"]) > 2 else 0
    return {
        "entered": {"last_24h": leads_24h, "last_7d": funnel["leads_7d"]},
        "approached": {"email_1": sent_1, "email_2": sent_2, "email_3": sent_3},
        "responded": {
            "email_1": replied_1,
            "email_2": replied_2,
            "email_3": replied_3,
            "human_total": reply_attr["human_replies_count"],
            "auto_total": reply_attr["auto_replies_count"],
            "unknown_total": reply_attr["unknown_replies_count"],
        },
        "advanced": {
            "consented_7d": funnel["consented_7d"],
            "offers_7d": funnel["offers_7d"],
            "won_7d": funnel["won_7d"],
            "waiting_confirmation": waiting_confirmation,
            "ready_to_approach": ready_to_approach,
        },
        "stuck": {
            "lead_skipped_24h": blockers["lead_skipped_total"],
            "followup_skipped_24h": blockers["followup_skipped_total"],
            "timeouts_24h": blockers["timeouts_total"],
            "no_response_total": max(0, sent_1 - replied_1),
        },
        "throughput": {
            "touches_24h": throughput["touches_24h_total"],
            "offers_24h": throughput["offers_24h"],
            "replies_24h": throughput["replies_24h"],
        },
    }


def _stage_loss_summary_snapshot(
    db_path: Path,
    events: list[dict[str, Any]],
    funnel: dict[str, Any],
    reply_stage: dict[str, Any],
    email_coverage: dict[str, Any],
    reply_attr: dict[str, Any],
) -> dict[str, Any]:
    blockers = _recent_blockers_snapshot(events)
    sent_1 = int(reply_stage["stages"][0]["sent_count"]) if reply_stage["stages"] else 0
    human_replies = int(reply_attr["human_replies_count"])
    stage_rows = [
        {
            "label": "Leads encontrados",
            "count": int(funnel["leads_7d"]),
            "next_rate": (email_coverage["contactable_7d"] / funnel["leads_7d"]) if funnel["leads_7d"] else 0.0,
            "loss_reason": "sem email valido" if email_coverage["contactable_7d"] < funnel["leads_7d"] else "sem perda relevante",
        },
        {
            "label": "Leads com contato valido",
            "count": int(email_coverage["contactable_7d"]),
            "next_rate": (sent_1 / email_coverage["contactable_7d"]) if email_coverage["contactable_7d"] else 0.0,
            "loss_reason": (blockers["lead_skipped_top"][0]["reason"] if blockers["lead_skipped_top"] else "sem bloqueios recentes"),
        },
        {
            "label": "1º email enviado",
            "count": sent_1,
            "next_rate": (human_replies / sent_1) if sent_1 else 0.0,
            "loss_reason": "sem resposta ao 1º email" if sent_1 > human_replies else "sem perda relevante",
        },
        {
            "label": "Responderam confirmacao",
            "count": human_replies,
            "next_rate": (funnel["offers_7d"] / human_replies) if human_replies else 0.0,
            "loss_reason": (blockers["followup_skipped_top"][0]["reason"] if blockers["followup_skipped_top"] else "sem oferta enviada"),
        },
        {
            "label": "Oferta enviada",
            "count": int(funnel["offers_7d"]),
            "next_rate": (funnel["won_7d"] / funnel["offers_7d"]) if funnel["offers_7d"] else 0.0,
            "loss_reason": "sem fechamento" if funnel["offers_7d"] > funnel["won_7d"] else "sem perda relevante",
        },
        {
            "label": "Venda",
            "count": int(funnel["won_7d"]),
            "next_rate": 1.0 if funnel["won_7d"] else 0.0,
            "loss_reason": "objetivo final",
        },
    ]
    return {"rows": stage_rows}


def _timeouts_summary_snapshot(events: list[dict[str, Any]]) -> dict[str, Any]:
    recent_24h = _events_in_window(events, hours=24)
    recent_7d = _events_in_window(events, days=7)
    timeouts_24h = [e for e in recent_24h if str(e.get("event_type") or "") == "campaign_step_timeout"]
    timeouts_7d = [e for e in recent_7d if str(e.get("event_type") or "") == "campaign_step_timeout"]
    by_step = Counter(str((e.get("payload") or {}).get("step") or "unknown") for e in timeouts_24h)
    return {
        "count_24h": len(timeouts_24h),
        "count_7d": len(timeouts_7d),
        "rows_24h": [{"step": k, "count": v} for k, v in by_step.most_common(5)],
    }


def _current_machine_status_snapshot(events: list[dict[str, Any]], throughput: dict[str, Any]) -> dict[str, Any]:
    recent_30m = _events_in_window(events, hours=1)
    recent_types = [str(e.get("event_type") or "") for e in recent_30m]
    has_ingest = "campaign_step_started" in recent_types
    has_timeout = "campaign_step_timeout" in recent_types
    has_email_activity = throughput["touches_24h_total"] > 0
    lines = [
        {
            "label": "A maquina esta buscando leads",
            "status": "OK" if has_ingest and not has_timeout else ("Atencao" if has_ingest else "Acao necessaria"),
        },
        {
            "label": "A maquina esta enviando emails",
            "status": "OK" if has_email_activity else "Atencao",
        },
        {
            "label": "A maquina esta travada por timeout",
            "status": "Acao necessaria" if has_timeout else "OK",
        },
    ]
    return {"lines": lines}


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
    country = _normalize_country_filter(country_filter)
    audience = _normalize_audience_filter(audience_filter)
    approach = _normalize_approach_filter(approach_filter)
    cache_key = (country, audience, approach, _snapshot_signature(cfg))
    now_mono = monotonic()
    cached = _SNAPSHOT_CACHE.get(cache_key)
    if cached and now_mono - cached[0] <= _SNAPSHOT_CACHE_TTL_SECONDS:
        return cached[1]
    events = _read_last_events(cfg.log_dir / "events.jsonl", max_lines=600)
    throughput = _throughput_snapshot(cfg.state_db, events, country, audience, approach)
    funnel = _funnel_7d(cfg.state_db, country, audience, approach)
    reply_stage = _reply_stage_snapshot(cfg.state_db, country, audience, approach)
    reply_attr = _reply_attribution_snapshot(cfg.state_db, country, audience, approach)
    email_coverage = _email_coverage_snapshot(cfg.state_db, country, audience, approach)
    snapshot = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db": _db_counts(cfg.state_db, country, audience, approach),
        "ops": _ops_snapshot(cfg.ops_state_db),
        "pricing": _pricing_snapshot(cfg.state_db),
        "funnel_7d": funnel,
        "geo_channels": _country_channel_snapshot(cfg.state_db, country, audience, approach),
        "throughput": throughput,
        "filters": {
            "country": country,
            "audience": audience,
            "approach": approach,
            "audience_options": _audience_options_snapshot(cfg.state_db, country, approach),
        },
        "domain_ops": _domain_ops_snapshot(cfg.state_db),
        "reply_queue": _reply_queue_snapshot(cfg.state_db),
        "template_performance": _template_performance_snapshot(cfg.state_db, country, audience, approach),
        "reply_stage": reply_stage,
        "reply_attribution": reply_attr,
        "email_coverage": email_coverage,
        "top_niches": _top_audience_snapshot(cfg.state_db, country, audience, approach),
        "top_countries": _top_country_snapshot(cfg.state_db, country, audience, approach),
        "followup_blockers": _recent_blockers_snapshot(events),
        "timeouts_summary": _timeouts_summary_snapshot(events),
        "owner_summary": _owner_summary_snapshot(cfg.state_db, events, reply_stage, throughput, funnel, reply_attr, country, audience, approach),
        "stage_loss_summary": _stage_loss_summary_snapshot(cfg.state_db, events, funnel, reply_stage, email_coverage, reply_attr),
        "current_machine": _current_machine_status_snapshot(events, throughput),
        "events_summary": _compute_event_summary(events),
        "events": events[-50:],
    }
    _SNAPSHOT_CACHE[cache_key] = (now_mono, snapshot)
    if len(_SNAPSHOT_CACHE) > 24:
        stale_keys = [key for key, (ts, _) in _SNAPSHOT_CACHE.items() if now_mono - ts > (_SNAPSHOT_CACHE_TTL_SECONDS * 4)]
        for key in stale_keys:
            _SNAPSHOT_CACHE.pop(key, None)
    return snapshot


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


def render_dashboard_html(
    snapshot: dict[str, Any] | None = None,
    country_filter: str = "ALL",
    audience_filter: str = "ALL",
    approach_filter: str = "ALL",
) -> str:
    cache_key: tuple[str, str, str, tuple[tuple[str, int, int], ...]] | None = None
    now_mono = monotonic()
    if snapshot is None:
        cfg = get_config()
        country = _normalize_country_filter(country_filter)
        audience = _normalize_audience_filter(audience_filter)
        approach = _normalize_approach_filter(approach_filter)
        cache_key = (country, audience, approach, _snapshot_signature(cfg))
        cached = _HTML_CACHE.get(cache_key)
        if cached and now_mono - cached[0] <= _HTML_CACHE_TTL_SECONDS:
            return cached[1]
        snap = build_snapshot(
            country_filter=country,
            audience_filter=audience,
            approach_filter=approach,
        )
    else:
        snap = snapshot
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
    reply_stage = snap["reply_stage"]
    reply_attr = snap["reply_attribution"]
    email_coverage = snap["email_coverage"]
    top_niches = snap["top_niches"]
    top_countries = snap["top_countries"]
    followup_blockers = snap["followup_blockers"]
    timeouts_summary = snap["timeouts_summary"]
    owner_summary = snap["owner_summary"]
    stage_loss_summary = snap["stage_loss_summary"]
    current_machine = snap["current_machine"]
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

    max_reply_rate = max(max([float(it["reply_rate"]) for it in template_perf["rows"]] or [0.0]), 0.01)
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

    stage_blocks = ""
    for stage in reply_stage["stages"]:
        sample = str(stage.get("sample_message") or "").strip()
        sample_short = (sample[:220] + "...") if len(sample) > 220 else sample
        sent_count = int(stage.get("sent_count", 0))
        replied_count = int(stage.get("replied_count", 0))
        rate = (replied_count / sent_count * 100.0) if sent_count else 0.0
        tooltip_text = _safe(sample_short or "Sem amostra da mensagem dessa etapa ainda.")
        details_rows = "".join(
            (
                f"<tr>"
                f"<td>#{d['lead_id']}</td>"
                f"<td>{_safe(d['business_name'])}</td>"
                f"<td>{_safe(d['email'])}</td>"
                f"<td>{_safe(d['timestamp_utc'])}</td>"
                f"<td><code>{_safe((d['reply_body'] or '')[:280])}</code></td>"
                f"</tr>"
            )
            for d in stage.get("details", [])
        ) or "<tr><td colspan='5'>Sem respostas registradas para esta etapa.</td></tr>"
        stage_blocks += (
            f"<article class='stage-card'>"
            f"<div class='stage-head'>"
            f"<div class='stage-title' title='{tooltip_text}'>{_safe(stage['label'])} <span class='muted'>({ _safe(stage['template_id']) })</span></div>"
            f"<div class='stage-metrics'><b>{replied_count}</b>/<span>{sent_count}</span> <small>{rate:.1f}%</small></div>"
            f"</div>"
            f"<div class='muted stage-tooltip'>Passe o mouse no título para ver a mensagem enviada nesta etapa.</div>"
            f"<details class='stage-details'>"
            f"<summary>Ver quem respondeu e o que respondeu</summary>"
            f"<div class='scroll-table' style='margin-top:8px; max-height:220px;'>"
            f"<table>"
            f"<thead><tr><th>Lead</th><th>Empresa</th><th>Email</th><th>Quando</th><th>Resposta</th></tr></thead>"
            f"<tbody>{details_rows}</tbody>"
            f"</table>"
            f"</div>"
            f"</details>"
            f"</article>"
        )

    owner_cards = [
        (
            "Entraram",
            f"{owner_summary['entered']['last_24h']}",
            f"Ultimas 24h | 7 dias: {owner_summary['entered']['last_7d']}",
            "#sec-funil",
        ),
        (
            "Foram abordados",
            f"{owner_summary['approached']['email_1']}",
            f"1o: {owner_summary['approached']['email_1']} | 2o: {owner_summary['approached']['email_2']} | 3o: {owner_summary['approached']['email_3']}",
            "#sec-etapas",
        ),
        (
            "Responderam",
            f"{reply_attr['human_replies_count']}",
            f"1o: {owner_summary['responded']['email_1']} | 2o: {owner_summary['responded']['email_2']} | 3o: {owner_summary['responded']['email_3']}",
            "#sec-etapas",
        ),
        (
            "Avancaram",
            f"{owner_summary['advanced']['consented_7d']}",
            f"Oferta: {owner_summary['advanced']['offers_7d']} | Venda: {owner_summary['advanced']['won_7d']}",
            "#sec-funil",
        ),
        (
            "Travaram",
            f"{owner_summary['stuck']['lead_skipped_24h'] + owner_summary['stuck']['followup_skipped_24h'] + owner_summary['stuck']['timeouts_24h']}",
            f"Pulados: {owner_summary['stuck']['lead_skipped_24h']} | Follow-up: {owner_summary['stuck']['followup_skipped_24h']} | Timeout: {owner_summary['stuck']['timeouts_24h']}",
            "#sec-travas",
        ),
    ]
    owner_cards_html = "".join(
        (
            f"<a class='owner-card' href='{link}'>"
            f"<div class='owner-label'>{_safe(label)}</div>"
            f"<div class='owner-value'>{_safe(value)}</div>"
            f"<div class='owner-foot'>{_safe(foot)}</div>"
            f"</a>"
        )
        for label, value, foot, link in owner_cards
    )

    machine_lines_html = "".join(
        (
            f"<div class='machine-line'>"
            f"<span>{_safe(item['label'])}</span>"
            f"<span class='status-chip {_status_badge('ACTIVE' if item['status'] == 'OK' else ('PAUSED' if item['status'] == 'Atencao' else 'ERROR'))}'>{_safe(item['status'])}</span>"
            f"</div>"
        )
        for item in current_machine["lines"]
    )

    stage_loss_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(item['label'])}</b></td>"
            f"<td>{item['count']}</td>"
            f"<td>{item['next_rate'] * 100:.1f}%</td>"
            f"<td>{_safe(item['loss_reason'])}</td>"
            f"</tr>"
        )
        for item in stage_loss_summary["rows"]
    ) or "<tr><td colspan='4'>Sem dados do funil.</td></tr>"

    blocker_rows = (
        "".join(
            f"<tr><td><b>{_safe(item['reason'])}</b></td><td>{item['count']}</td><td>Lead pulado</td></tr>"
            for item in followup_blockers["lead_skipped_top"][:4]
        )
        + "".join(
            f"<tr><td><b>{_safe(item['reason'])}</b></td><td>{item['count']}</td><td>Follow-up travado</td></tr>"
            for item in followup_blockers["followup_skipped_top"][:4]
        )
        + "".join(
            f"<tr><td><b>{_safe(item['step'])}</b></td><td>{item['count']}</td><td>Timeout</td></tr>"
            for item in timeouts_summary["rows_24h"][:4]
        )
    ) or "<tr><td colspan='3'>Sem travas recentes detectadas.</td></tr>"

    coverage_rows = "".join(
        (
            f"<tr>"
            f"<td><b>{_safe(item['country'])}</b></td>"
            f"<td>{item['contactable']}/{item['leads']}</td>"
            f"<td>{item['coverage_rate'] * 100:.1f}%</td>"
            f"</tr>"
        )
        for item in email_coverage["rows"][:5]
    ) or "<tr><td colspan='3'>Sem cobertura por pais ainda.</td></tr>"

    top_niche_rows = "".join(
        (
            f"<tr><td><b>{_safe(item['audience'])}</b></td><td>{item['sent_count']}</td><td>{item['replied_count']}</td><td>{item['reply_rate'] * 100:.1f}%</td></tr>"
        )
        for item in top_niches["rows"][:5]
    ) or "<tr><td colspan='4'>Sem dados por nicho ainda.</td></tr>"

    top_country_rows = "".join(
        (
            f"<tr><td><b>{_safe(item['country'])}</b></td><td>{item['sent_count']}</td><td>{item['replied_count']}</td><td>{item['reply_rate'] * 100:.1f}%</td></tr>"
        )
        for item in top_countries["rows"][:5]
    ) or "<tr><td colspan='4'>Sem dados por pais ainda.</td></tr>"

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
    if owner_summary["approached"]["email_1"] >= 25 and reply_attr["human_replies_count"] == 0:
        alerts.append(("critical", "Sem resposta humana", "O topo do funil esta andando, mas ainda nao houve resposta humana registrada."))
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

    html_out = f"""<!doctype html>
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
    .stage-grid {{
      display:grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap:10px;
    }}
    .stage-card {{
      border:1px solid var(--line);
      background: var(--card-soft);
      border-radius: 10px;
      padding: 10px;
    }}
    .stage-head {{
      display:flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
    }}
    .stage-title {{
      font-size: 14px;
      font-weight: 800;
      cursor: help;
    }}
    .stage-metrics b {{
      font-size: 20px;
      font-weight: 800;
      color: var(--text);
    }}
    .stage-metrics span {{
      font-size: 16px;
      color: var(--muted);
    }}
    .stage-metrics small {{
      margin-left:6px;
      font-size: 12px;
      color: var(--accent);
      font-weight: 700;
    }}
    .stage-tooltip {{
      margin-top: 4px;
      margin-bottom: 6px;
    }}
    .stage-details summary {{
      cursor: pointer;
      font-size: 12px;
      font-weight: 700;
      color: var(--accent);
      list-style: none;
    }}
    .stage-details summary::-webkit-details-marker {{
      display:none;
    }}
    .owner-grid {{
      display:grid;
      grid-template-columns: 1.8fr 1fr;
      gap:10px;
      margin-bottom:10px;
    }}
    .owner-cards {{
      display:grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap:10px;
    }}
    .owner-card {{
      display:block;
      text-decoration:none;
      color:inherit;
      border:1px solid var(--line);
      border-radius:14px;
      background: linear-gradient(180deg, var(--card), var(--card-soft));
      box-shadow: var(--shadow);
      padding:12px;
      transition: .18s ease;
    }}
    .owner-card:hover {{
      transform: translateY(-1px);
      border-color: color-mix(in srgb, var(--accent) 45%, var(--line));
    }}
    .owner-label {{
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      font-weight: 800;
      letter-spacing: 0.45px;
    }}
    .owner-value {{
      margin-top: 5px;
      font-size: 34px;
      font-weight: 900;
      line-height: 1;
    }}
    .owner-foot {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.3;
    }}
    .machine-line {{
      display:flex;
      justify-content:space-between;
      align-items:center;
      gap:10px;
      padding:8px 0;
      border-bottom:1px solid var(--line);
      font-size:13px;
    }}
    .machine-line:last-child {{
      border-bottom:none;
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
      .owner-grid {{ grid-template-columns: 1fr; }}
      .owner-cards {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    }}
    @media (max-width: 920px) {{
      .kpi-grid {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }}
      .sub-grid {{ grid-template-columns: 1fr; }}
      .kpi-value {{ font-size: 24px; }}
      .hero h1 {{ font-size: 23px; }}
      .owner-cards {{ grid-template-columns: 1fr; }}
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

    <section class='owner-grid'>
      <div class='owner-cards'>
        {owner_cards_html}
      </div>
      <article class='card'>
        <h2 class='card-title'>O que está acontecendo agora</h2>
        <div class='muted' style='margin-bottom:8px;'>Resumo direto do estado atual da máquina.</div>
        {machine_lines_html}
      </article>
    </section>

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
        <div class='kpi-foot'>Respostas aguardando analise: {queue_label}</div>
      </article>
    </section>

    <section class='main-layout' id='sec-funil'>
      <div>
        <article class='card' style='margin-bottom:10px;'>
          <h2 class='card-title'>Funil executivo (7 dias)</h2>
          <div class='funnel-stack'>{funnel_rows}</div>
          <div class='legend'>
            <span class='chip is-neutral'>Pronto para abordagem: {owner_summary['advanced']['ready_to_approach']}</span>
            <span class='chip is-neutral'>Aguardando confirmacao: {owner_summary['advanced']['waiting_confirmation']}</span>
            <span class='chip is-neutral'>Ofertas: {funnel['offers_7d']}</span>
            <span class='chip is-ok'>Vendas: {funnel['won_7d']}</span>
            <span class='chip is-warn'>Sem resposta ao 1o email: {owner_summary['stuck']['no_response_total']}</span>
          </div>
        </article>

        <article class='card' style='margin-bottom:10px;'>
          <h2 class='card-title'>Onde o funil está travando</h2>
          <div class='scroll-table'>
            <table>
              <thead><tr><th>Etapa</th><th>Volume</th><th>Passagem</th><th>Principal perda</th></tr></thead>
              <tbody>{stage_loss_rows}</tbody>
            </table>
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
        <article class='card' style='margin-top:10px;' id='sec-etapas'>
          <h2 class='card-title'>Respostas por etapa de e-mail</h2>
          <div class='muted' style='margin-bottom:8px;'>Aqui você vê quantos responderam no 1º, 2º e 3º e-mail.</div>
          <div class='stage-grid'>{stage_blocks}</div>
        </article>
      </div>

      <aside class='aside-stack'>
        <article class='card'>
          <h2 class='card-title'>Saude operacional</h2>
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
          <h2 class='card-title'>Respostas aguardando analise</h2>
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
          <details>
            <summary style='cursor:pointer; font-weight:700; color:var(--accent);'>Abrir timeline detalhada</summary>
            <div class='event-feed' style='margin-top:8px;'>{event_feed}</div>
          </details>
        </article>
      </aside>
    </section>

    <section class='sub-grid'>
      <article class='card' id='sec-travas'>
        <h2 class='card-title'>Travas reais das ultimas 24h</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Causa</th><th>Volume</th><th>Tipo</th></tr></thead>
            <tbody>{blocker_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Cobertura de email valido por pais</h2>
        <div class='muted'>Contato valido em 7 dias: {email_coverage['contactable_7d']}/{email_coverage['leads_7d']} ({email_coverage['coverage_rate_7d'] * 100:.1f}%)</div>
        <div class='scroll-table' style='margin-top:8px;'>
          <table>
            <thead><tr><th>Pais</th><th>Com email</th><th>Cobertura</th></tr></thead>
            <tbody>{coverage_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Top nichos por resposta</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Nicho</th><th>Envios</th><th>Respostas</th><th>Taxa</th></tr></thead>
            <tbody>{top_niche_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Top paises por resposta</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Pais</th><th>Envios</th><th>Respostas</th><th>Taxa</th></tr></thead>
            <tbody>{top_country_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Leads por pais</h2>
        <div class='scroll-table'>
          <table>
            <thead><tr><th>Pais</th><th>Leads</th><th>%</th><th>Participacao</th></tr></thead>
            <tbody>{country_rows}</tbody>
          </table>
        </div>
      </article>
      <article class='card'>
        <h2 class='card-title'>Abordagens por canal</h2>
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
        <h2 class='card-title'>Abordagens por pais x canal</h2>
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
    if cache_key is not None:
        _HTML_CACHE[cache_key] = (now_mono, html_out)
        if len(_HTML_CACHE) > 24:
            stale_keys = [key for key, (ts, _) in _HTML_CACHE.items() if now_mono - ts > (_HTML_CACHE_TTL_SECONDS * 4)]
            for key in stale_keys:
                _HTML_CACHE.pop(key, None)
    return html_out


def run_dashboard(host: str = "0.0.0.0", port: int = 8789) -> None:
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.serve_forever()
