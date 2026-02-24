from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .config import get_config


def _read_last_events(path: Path, max_lines: int = 120) -> list[dict[str, Any]]:
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


def _compute_event_summary(events: list[dict[str, Any]]) -> dict[str, Any]:
    counter = Counter()
    for e in events:
        counter[str(e.get("event_type", "unknown"))] += 1
    return {
        "contact_delivered": counter.get("contact_delivered", 0),
        "contact_failed": counter.get("contact_failed", 0),
        "lead_qualified": counter.get("lead_qualified", 0),
        "demo_published": counter.get("demo_published", 0),
        "offer_sent": counter.get("offer_sent", 0),
        "opt_out_registered": counter.get("opt_out_registered", 0),
        "safe_mode_enabled": counter.get("safe_mode_enabled", 0),
        "channel_paused": counter.get("channel_paused", 0),
    }


def build_snapshot() -> dict[str, Any]:
    cfg = get_config()
    events = _read_last_events(cfg.log_dir / "events.jsonl")
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "db": _db_counts(cfg.state_db),
        "ops": _ops_snapshot(cfg.ops_state_db),
        "events_summary": _compute_event_summary(events),
        "events": events[-40:],
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
        raw = json.dumps(payload).encode("utf-8")
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
    stage_items = "".join(
        f"<li><b>{k}</b>: {v}</li>" for k, v in sorted(snap["db"]["stage_counts"].items())
    ) or "<li>Sem leads ainda</li>"
    channel_items = "".join(
        f"<li><b>{c['channel']}</b> - {c['status']} {('(' + c['reason'] + ')') if c['reason'] else ''}</li>"
        for c in snap["ops"]["channels"]
    ) or "<li>Sem status de canal ainda</li>"
    metric_rows = "".join(
        f"<tr><td>{m['channel']}</td><td>{m['sent']}</td><td>{m['failed']}</td><td>{m['bounces']}</td><td>{m['complaints']}</td></tr>"
        for m in snap["ops"]["metrics"]
    ) or "<tr><td colspan='5'>Sem metricas hoje</td></tr>"

    event_rows = ""
    for e in reversed(snap["events"][-20:]):
        ts = str(e.get("timestamp_utc", ""))
        ev = str(e.get("event_type", ""))
        payload = json.dumps(e.get("payload", {}), ensure_ascii=False)
        event_rows += f"<tr><td>{ts}</td><td>{ev}</td><td><code>{payload}</code></td></tr>"

    safe_mode = "ATIVO" if snap["ops"]["global_safe_mode"] else "DESATIVADO"
    safe_color = "#b91c1c" if snap["ops"]["global_safe_mode"] else "#166534"
    es = snap["events_summary"]

    return f"""<!doctype html>
<html lang='pt-BR'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <meta http-equiv='refresh' content='10'/>
  <title>LeadGenerator Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; background: #f3f4f6; color: #111827; }}
    .wrap {{ max-width: 1200px; margin: 18px auto; padding: 0 16px 24px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(240px,1fr)); gap: 12px; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px; }}
    h1 {{ margin: 0 0 6px; }}
    h2 {{ margin: 0 0 8px; font-size: 16px; }}
    .kpi {{ font-size: 28px; font-weight: 700; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; text-align: left; padding: 6px; vertical-align: top; }}
    code {{ white-space: pre-wrap; word-break: break-word; }}
  </style>
</head>
<body>
<div class='wrap'>
  <h1>LeadGenerator - Status ao vivo</h1>
  <p>Atualizado em {snap['generated_at_utc']}</p>

  <div class='grid'>
    <div class='card'><h2>Safe Mode</h2><div class='kpi' style='color:{safe_color}'>{safe_mode}</div></div>
    <div class='card'><h2>Leads no banco</h2><div class='kpi'>{snap['db']['leads_total']}</div></div>
    <div class='card'><h2>Contatos enviados</h2><div class='kpi'>{es['contact_delivered']}</div></div>
    <div class='card'><h2>Falhas de contato</h2><div class='kpi'>{es['contact_failed']}</div></div>
    <div class='card'><h2>Demos publicadas</h2><div class='kpi'>{es['demo_published']}</div></div>
    <div class='card'><h2>Ofertas enviadas</h2><div class='kpi'>{es['offer_sent']}</div></div>
  </div>

  <div class='grid' style='margin-top:12px'>
    <div class='card'>
      <h2>Estagios dos leads</h2>
      <ul>{stage_items}</ul>
    </div>
    <div class='card'>
      <h2>Status dos canais</h2>
      <ul>{channel_items}</ul>
    </div>
  </div>

  <div class='card' style='margin-top:12px'>
    <h2>Metricas de canal (hoje)</h2>
    <table>
      <thead><tr><th>Canal</th><th>Enviados</th><th>Falhas</th><th>Bounces</th><th>Complaints</th></tr></thead>
      <tbody>{metric_rows}</tbody>
    </table>
  </div>

  <div class='card' style='margin-top:12px'>
    <h2>Eventos recentes</h2>
    <table>
      <thead><tr><th>Timestamp UTC</th><th>Evento</th><th>Payload</th></tr></thead>
      <tbody>{event_rows or "<tr><td colspan='3'>Sem eventos ainda</td></tr>"}</tbody>
    </table>
  </div>
</div>
</body>
</html>"""


def run_dashboard(host: str = "0.0.0.0", port: int = 8789) -> None:
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    server.serve_forever()
