from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .config import get_config


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


def _db_counts(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        return {"leads_total": 0, "stage_counts": {}, "touches_total": 0, "replies_total": 0}
    with sqlite3.connect(db_path) as conn:
        leads_total = int(conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0])
        rows = conn.execute("SELECT stage, COUNT(*) FROM leads GROUP BY stage").fetchall()
        touches_total = int(conn.execute("SELECT COUNT(*) FROM touches").fetchone()[0])
        replies_total = int(conn.execute("SELECT COUNT(*) FROM replies").fetchone()[0])
    return {
        "leads_total": leads_total,
        "stage_counts": {str(r[0]): int(r[1]) for r in rows},
        "touches_total": touches_total,
        "replies_total": replies_total,
    }


def _derive_country(phone: str, address: str) -> str:
    phone_s = (phone or "").strip()
    address_l = (address or "").lower()
    if phone_s.startswith("+55") or "brasil" in address_l or "brazil" in address_l:
        return "BR"
    if phone_s.startswith("+44") or "london" in address_l or "united kingdom" in address_l or "uk" in address_l:
        return "UK"
    if not phone_s and not address_l:
        return "UNKNOWN"
    return "OTHER"


def _country_channel_snapshot(db_path: Path) -> dict[str, Any]:
    defaults = {"by_country": [], "approaches_by_channel": [], "approaches_by_country_channel": []}
    if not db_path.exists():
        return defaults
    try:
        with sqlite3.connect(db_path) as conn:
            lead_rows = conn.execute("SELECT phone, address FROM leads").fetchall()
            by_country_counter: Counter[str] = Counter()
            for row in lead_rows:
                by_country_counter[_derive_country(str(row[0] or ""), str(row[1] or ""))] += 1

            channel_rows = conn.execute(
                """
                SELECT channel, COUNT(*)
                FROM touches
                GROUP BY channel
                ORDER BY COUNT(*) DESC
                """
            ).fetchall()

            country_channel_rows = conn.execute(
                """
                SELECT l.phone, l.address, t.channel, COUNT(*)
                FROM touches t
                JOIN leads l ON l.id = t.lead_id
                GROUP BY l.phone, l.address, t.channel
                """
            ).fetchall()
    except sqlite3.Error:
        return defaults

    country_channel_counter: Counter[tuple[str, str]] = Counter()
    for phone, address, channel, count in country_channel_rows:
        country = _derive_country(str(phone or ""), str(address or ""))
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


def _throughput_snapshot(db_path: Path, events: list[dict[str, Any]]) -> dict[str, Any]:
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
            rows_1h = conn.execute(
                """
                SELECT channel, COUNT(*)
                FROM touches
                WHERE timestamp_utc >= ?
                GROUP BY channel
                ORDER BY COUNT(*) DESC
                """,
                (since_1h,),
            ).fetchall()
            rows_24h = conn.execute(
                """
                SELECT channel, COUNT(*)
                FROM touches
                WHERE timestamp_utc >= ?
                GROUP BY channel
                ORDER BY COUNT(*) DESC
                """,
                (since_24h,),
            ).fetchall()
            new_leads_24h = int(
                conn.execute("SELECT COUNT(*) FROM leads WHERE created_at_utc >= ?", (since_24h,)).fetchone()[0]
            )
            replies_24h = int(
                conn.execute("SELECT COUNT(*) FROM replies WHERE timestamp_utc >= ?", (since_24h,)).fetchone()[0]
            )
            offers_24h = int(
                conn.execute("SELECT COUNT(*) FROM offer_snapshots WHERE offered_at_utc >= ?", (since_24h,)).fetchone()[0]
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


def _funnel_7d(db_path: Path) -> dict[str, Any]:
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
            leads_7d = int(conn.execute("SELECT COUNT(*) FROM leads WHERE created_at_utc >= ?", (since,)).fetchone()[0])
            consented_7d = int(
                conn.execute(
                    "SELECT COUNT(*) FROM leads WHERE consent_accepted=1 AND updated_at_utc >= ?",
                    (since,),
                ).fetchone()[0]
            )
            offers_7d = int(conn.execute("SELECT COUNT(*) FROM offer_snapshots WHERE offered_at_utc >= ?", (since,)).fetchone()[0])
            won_7d = int(conn.execute("SELECT COUNT(*) FROM leads WHERE stage='WON' AND updated_at_utc >= ?", (since,)).fetchone()[0])
            lost_7d = int(conn.execute("SELECT COUNT(*) FROM leads WHERE stage='LOST' AND updated_at_utc >= ?", (since,)).fetchone()[0])
            avg_days_row = conn.execute(
                """
                SELECT AVG(julianday(won_at_utc) - julianday(created_at_utc))
                FROM leads
                WHERE stage='WON' AND won_at_utc != '' AND won_at_utc >= ?
                """,
                (since,),
            ).fetchone()
            revenue_row = conn.execute(
                """
                SELECT COALESCE(SUM(sale_amount), 0)
                FROM leads
                WHERE stage='WON' AND won_at_utc != '' AND won_at_utc >= ?
                """,
                (since,),
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
    }


def build_snapshot() -> dict[str, Any]:
    cfg = get_config()
    events = _read_last_events(cfg.log_dir / "events.jsonl")
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db": _db_counts(cfg.state_db),
        "ops": _ops_snapshot(cfg.ops_state_db),
        "pricing": _pricing_snapshot(cfg.state_db),
        "funnel_7d": _funnel_7d(cfg.state_db),
        "geo_channels": _country_channel_snapshot(cfg.state_db),
        "throughput": _throughput_snapshot(cfg.state_db, events),
        "domain_ops": _domain_ops_snapshot(cfg.state_db),
        "reply_queue": _reply_queue_snapshot(cfg.state_db),
        "events_summary": _compute_event_summary(events),
        "events": events[-50:],
    }


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/api/status"):
            self._json(200, build_snapshot())
            return
        if self.path.startswith("/health"):
            self._json(200, {"status": "ok"})
            return
        if self.path == "/" or self.path.startswith("/?"):
            self._html(200, self._render_dashboard())
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

    def _render_dashboard(self) -> str:
        return render_dashboard_html()


def render_dashboard_html() -> str:
    snap = build_snapshot()
    pricing = snap["pricing"]
    funnel = snap["funnel_7d"]
    queue = snap["reply_queue"]
    domains = snap["domain_ops"]
    ops = snap["ops"]
    geo = snap["geo_channels"]
    throughput = snap["throughput"]
    es = snap["events_summary"]
    progress_pct = min(100, int((pricing["offers_in_window"] / 10) * 100))
    safe_mode = "ATIVO" if ops["global_safe_mode"] else "DESATIVADO"
    safe_class = "bad" if ops["global_safe_mode"] else "ok"
    baseline_txt = f"{pricing['baseline_conversion'] * 100:.1f}%" if pricing["baseline_conversion"] is not None else "n/a"
    conv_txt = f"{funnel['conversion_7d'] * 100:.1f}%"
    event_age = throughput.get("last_event_age_min")
    if event_age is None:
        activity_txt = "Sem atividade recente"
        activity_class = "warn"
    elif event_age <= 3:
        activity_txt = "Agora mesmo"
        activity_class = "ok"
    elif event_age <= 20:
        activity_txt = f"{event_age} min atras"
        activity_class = "ok"
    elif event_age <= 60:
        activity_txt = f"{event_age} min atras"
        activity_class = "warn"
    else:
        activity_txt = f"{event_age} min atras"
        activity_class = "bad"
    lead_total_geo = max(1, sum(int(it["leads"]) for it in geo["by_country"]))
    max_channel_touches = max([int(it["touches"]) for it in geo["approaches_by_channel"]] or [1])
    max_country_channel_touches = max([int(it["touches"]) for it in geo["approaches_by_country_channel"]] or [1])
    ch_1h = {str(it["channel"]): int(it["count"]) for it in throughput["touches_1h_by_channel"]}
    ch_24h = {str(it["channel"]): int(it["count"]) for it in throughput["touches_24h_by_channel"]}
    pace_channels = sorted(set(ch_1h.keys()) | set(ch_24h.keys()))
    pace_max_1h = max([ch_1h.get(ch, 0) for ch in pace_channels] or [1])
    pace_max_24h = max([ch_24h.get(ch, 0) for ch in pace_channels] or [1])

    stage_funnel = [
        ("Leads 7d", funnel["leads_7d"]),
        ("Consentidos 7d", funnel["consented_7d"]),
        ("Ofertas 7d", funnel["offers_7d"]),
        ("Vendas 7d", funnel["won_7d"]),
        ("Perdidos 7d", funnel["lost_7d"]),
    ]
    funnel_rows = "".join(
        f"<div class='funnel-item'><span>{label}</span><b>{value}</b></div>" for label, value in stage_funnel
    )
    pending_rows = "".join(
        f"<tr><td>#{it['id']}</td><td>Lead {it['lead_id']}</td><td>{it['created_at_utc']}</td><td><code>{it['inbound_text']}</code></td></tr>"
        for it in queue["top_pending"]
    ) or "<tr><td colspan='4'>Sem pendencias.</td></tr>"

    domain_rows = "".join(
        f"<tr><td>{d['id']}</td><td>{d['domain_name'] or '-'}</td><td>{d['status']}</td><td>{d['days_left'] if d['days_left'] is not None else '-'}</td></tr>"
        for d in domains["next_expiring"][:8]
    ) or "<tr><td colspan='4'>Sem dominios com expiracao registrada.</td></tr>"

    channel_rows = "".join(
        f"<tr><td>{c['channel']}</td><td>{c['status']}</td><td>{c['reason'] or '-'}</td></tr>" for c in ops["channels"]
    ) or "<tr><td colspan='3'>Sem canais registrados.</td></tr>"
    country_rows = "".join(
        (
            f"<tr><td><b>{it['country']}</b></td><td>{it['leads']}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((int(it['leads']) / lead_total_geo) * 100))}%'></i></div></td></tr>"
        )
        for it in geo["by_country"]
    ) or "<tr><td colspan='3'>Sem dados por pais.</td></tr>"
    approach_channel_rows = "".join(
        (
            f"<tr><td><b>{it['channel']}</b></td><td>{it['touches']}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((int(it['touches']) / max_channel_touches) * 100))}%'></i></div></td></tr>"
        )
        for it in geo["approaches_by_channel"]
    ) or "<tr><td colspan='3'>Sem abordagens registradas.</td></tr>"
    approach_country_channel_rows = "".join(
        (
            f"<tr><td>{it['country']}</td><td>{it['channel']}</td><td>{it['touches']}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((int(it['touches']) / max_country_channel_touches) * 100))}%'></i></div></td></tr>"
        )
        for it in geo["approaches_by_country_channel"][:20]
    ) or "<tr><td colspan='4'>Sem cruzamento pais/canal.</td></tr>"
    pace_rows = "".join(
        (
            f"<tr><td><b>{ch}</b></td><td>{ch_1h.get(ch, 0)}</td><td>{ch_24h.get(ch, 0)}</td>"
            f"<td><div class='meter'><i style='width:{max(4, int((ch_24h.get(ch, 0) / pace_max_24h) * 100))}%'></i></div></td>"
            f"<td><div class='meter meter-cool'><i style='width:{max(4, int((ch_1h.get(ch, 0) / pace_max_1h) * 100))}%'></i></div></td></tr>"
        )
        for ch in pace_channels
    ) or "<tr><td colspan='5'>Sem ritmo por canal ainda.</td></tr>"

    event_rows = ""
    for e in reversed(snap["events"][-18:]):
        ts = str(e.get("timestamp_utc", ""))
        ev = str(e.get("event_type", ""))
        payload = json.dumps(e.get("payload", {}), ensure_ascii=False)
        event_rows += f"<tr><td>{ts}</td><td>{ev}</td><td><code>{payload}</code></td></tr>"
    if not event_rows:
        event_rows = "<tr><td colspan='3'>Sem eventos ainda.</td></tr>"

    return f"""<!doctype html>
<html lang='pt-BR'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <meta http-equiv='refresh' content='10'/>
  <title>LeadGenerator - Painel Comercial</title>
  <style>
    :root {{
      --bg0:#f7fafc;
      --bg1:#e7eef7;
      --card:#ffffff;
      --ink:#111827;
      --muted:#4b5563;
      --line:#e5e7eb;
      --ok:#15803d;
      --warn:#c2410c;
      --bad:#b91c1c;
      --brand:#0f766e;
      --brand2:#0369a1;
      --accent:#1d4ed8;
      --shadow:0 8px 24px rgba(2, 6, 23, 0.07);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1000px 400px at 100% -10%, rgba(3, 105, 161, .16) 0%, transparent 70%),
        radial-gradient(900px 300px at -10% -10%, rgba(15, 118, 110, .18) 0%, transparent 75%),
        linear-gradient(180deg, var(--bg1), var(--bg0));
    }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 18px 18px 30px; }}
    .hero {{
      border: 1px solid #dbe3ee;
      background: linear-gradient(135deg, #ffffff 0%, #f0f7ff 45%, #ecfdf5 100%);
      border-radius: 16px;
      padding: 16px 18px;
      margin-bottom: 14px;
      box-shadow: var(--shadow);
    }}
    .hero h1 {{ margin: 0; font-size: 24px; letter-spacing: 0.2px; }}
    .hero p {{ margin: 4px 0 0; color: var(--muted); font-size: 13px; }}
    .hero-top {{ display:flex; justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap; }}
    .badge {{
      border-radius: 999px;
      padding: 5px 10px;
      font-size: 12px;
      font-weight: 600;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--muted);
    }}
    .badge-activity {{
      color: #0f172a;
      font-weight: 700;
      background: #eff6ff;
      border-color: #bfdbfe;
    }}
    .grid {{ display: grid; gap: 10px; }}
    .kpis {{ grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); }}
    .split {{ grid-template-columns: 1.6fr 1fr; }}
    .triple {{ grid-template-columns: 1fr 1fr 1fr; }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 14px;
      box-shadow: var(--shadow);
    }}
    .kpi-title {{ font-size: 12px; text-transform: uppercase; color: var(--muted); letter-spacing: 0.6px; }}
    .kpi-value {{ font-size: 30px; font-weight: 750; margin-top: 2px; line-height: 1.05; }}
    .ok {{ color: var(--ok); }} .bad {{ color: var(--bad); }} .warn {{ color: var(--warn); }}
    .price-row {{ display:flex; gap:10px; margin-top:6px; }}
    .pill {{
      border-radius: 999px;
      border: 1px solid var(--line);
      padding: 4px 10px;
      font-size: 12px;
      color: var(--muted);
      background: #f8fafc;
    }}
    .progress {{
      width: 100%; height: 11px; border-radius: 999px; background: #e5e7eb; overflow: hidden; margin-top: 8px;
    }}
    .progress > i {{
      display:block; height: 100%; width: {progress_pct}%;
      background: linear-gradient(90deg, var(--brand), var(--accent), var(--brand2));
    }}
    h2 {{ margin: 0 0 8px; font-size: 16px; }}
    .funnel {{ display:grid; gap:7px; }}
    .funnel-item {{
      display:flex; justify-content:space-between; align-items:center;
      border:1px solid var(--line); border-radius:10px; padding:9px 11px;
      background: linear-gradient(180deg, #ffffff, #f8fafc);
    }}
    table {{ width:100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom:1px solid var(--line); text-align:left; padding:7px 6px; vertical-align:top; }}
    th {{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.4px; }}
    code {{ white-space: pre-wrap; word-break: break-word; color:#0f172a; font-family: "SFMono-Regular", Menlo, monospace; }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    .meter {{
      width: 100%;
      height: 8px;
      background: #e5e7eb;
      border-radius: 999px;
      overflow: hidden;
      min-width: 120px;
    }}
    .meter > i {{
      display:block;
      height:100%;
      background: linear-gradient(90deg, var(--brand), var(--accent));
    }}
    .meter-cool > i {{
      background: linear-gradient(90deg, #0ea5e9, #6366f1);
    }}
    .compact {{ max-height: 420px; overflow: auto; }}
    @media (max-width: 940px) {{
      .split, .triple {{ grid-template-columns: 1fr; }}
      .kpi-value {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
<div class='wrap'>
  <section class='hero'>
    <div class='hero-top'>
      <h1>Painel Comercial LeadGenerator</h1>
      <span class='badge'>Atualizacao automatica: 10s</span>
    </div>
    <p>Atualizado em {snap['generated_at_utc']} (UTC). Status global: <b class='{safe_class}'>{safe_mode}</b>.</p>
    <div class='hero-top' style='margin-top:8px'>
      <span class='badge badge-activity'>Ultima atividade: <b class='{activity_class}'>{activity_txt}</b></span>
      <span class='badge'>Abordagens 1h: <b>{throughput['touches_1h_total']}</b></span>
      <span class='badge'>Abordagens 24h: <b>{throughput['touches_24h_total']}</b></span>
      <span class='badge'>Leads novos 24h: <b>{throughput['new_leads_24h']}</b></span>
      <span class='badge'>Respostas 24h: <b>{throughput['replies_24h']}</b></span>
    </div>
  </section>

  <section class='grid kpis'>
    <div class='card'>
      <div class='kpi-title'>Preco Atual</div>
      <div class='kpi-value'>R$ {pricing['price_full']} / R$ {pricing['price_simple']}</div>
      <div class='price-row'>
        <span class='pill'>Nivel {pricing['price_level']}</span>
        <span class='pill'>Baseline {baseline_txt}</span>
      </div>
    </div>
    <div class='card'>
      <div class='kpi-title'>Conversao 7d</div>
      <div class='kpi-value'>{conv_txt}</div>
      <div class='muted'>Vendas: {funnel['won_7d']} | Ofertas: {funnel['offers_7d']}</div>
    </div>
    <div class='card'>
      <div class='kpi-title'>Ofertas no Bloco</div>
      <div class='kpi-value'>{pricing['offers_in_window']}/10</div>
      <div class='progress'><i></i></div>
    </div>
    <div class='card'>
      <div class='kpi-title'>Vendas 7d</div>
      <div class='kpi-value'>{funnel['won_7d']}</div>
      <div class='muted'>Tempo medio ate venda: {funnel['avg_days_to_win_7d']:.1f} dias</div>
    </div>
    <div class='card'>
      <div class='kpi-title'>Receita Estimada 7d</div>
      <div class='kpi-value'>R$ {funnel['revenue_estimated_7d']:.0f}</div>
      <div class='muted'>Apenas vendas marcadas como WON</div>
    </div>
    <div class='card'>
      <div class='kpi-title'>Fila Codex</div>
      <div class='kpi-value'>{queue['counts']['pending'] + queue['counts']['review_required']}</div>
      <div class='muted'>PENDENTE + REVISAO</div>
    </div>
  </section>

  <section class='grid split' style='margin-top:10px'>
    <div class='card'>
      <h2>Funil Comercial (7 dias)</h2>
      <div class='funnel'>{funnel_rows}</div>
    </div>
    <div class='card'>
      <h2>Canal e Saude Operacional</h2>
      <table>
        <thead><tr><th>Canal</th><th>Status</th><th>Motivo</th></tr></thead>
        <tbody>{channel_rows}</tbody>
      </table>
      <div class='muted' style='margin-top:8px'>
        Entregues: {es['contact_delivered']} | Falhas: {es['contact_failed']} | Ofertas enviadas: {es['offer_sent']}
      </div>
    </div>
  </section>

  <section class='card' style='margin-top:10px'>
    <h2>Ritmo por Canal (1h vs 24h)</h2>
    <table>
      <thead><tr><th>Canal</th><th>1h</th><th>24h</th><th>Volume 24h</th><th>Intensidade 1h</th></tr></thead>
      <tbody>{pace_rows}</tbody>
    </table>
  </section>

  <section class='grid split' style='margin-top:10px'>
    <div class='card'>
      <h2>Leads por Pais</h2>
      <table>
        <thead><tr><th>Pais</th><th>Leads</th><th>Participacao</th></tr></thead>
        <tbody>{country_rows}</tbody>
      </table>
    </div>
    <div class='card'>
      <h2>Abordagens por Canal</h2>
      <table>
        <thead><tr><th>Canal</th><th>Total de abordagens</th><th>Volume relativo</th></tr></thead>
        <tbody>{approach_channel_rows}</tbody>
      </table>
    </div>
  </section>

  <section class='card' style='margin-top:10px'>
    <h2>Abordagens por Pais x Canal</h2>
    <table class='compact'>
      <thead><tr><th>Pais</th><th>Canal</th><th>Abordagens</th><th>Forca</th></tr></thead>
      <tbody>{approach_country_channel_rows}</tbody>
    </table>
  </section>

  <section class='grid triple' style='margin-top:10px'>
    <div class='card'>
      <h2>Revisao Codex Pendente</h2>
      <table>
        <thead><tr><th>ID</th><th>Lead</th><th>Recebido</th><th>Mensagem</th></tr></thead>
        <tbody>{pending_rows}</tbody>
      </table>
    </div>
    <div class='card'>
      <h2>Dominios em Implantacao</h2>
      <div class='muted'>Total jobs: {domains['total_jobs']} | Em andamento: {domains['in_progress']}</div>
      <table>
        <thead><tr><th>Job</th><th>Dominio</th><th>Status</th><th>Dias</th></tr></thead>
        <tbody>{domain_rows}</tbody>
      </table>
    </div>
    <div class='card'>
      <h2>Timeline Recente</h2>
      <table>
        <thead><tr><th>UTC</th><th>Evento</th><th>Payload</th></tr></thead>
        <tbody>{event_rows}</tbody>
      </table>
    </div>
  </section>
</div>
</body>
</html>"""


def run_dashboard(host: str = "0.0.0.0", port: int = 8789) -> None:
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.serve_forever()
