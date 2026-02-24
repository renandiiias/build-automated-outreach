from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import get_config
from .crm_store import CrmStore
from .logging_utils import JsonlLogger
from .monitor_dashboard import build_snapshot, render_dashboard_html
from .outreach import classify_reply, is_opt_out_reply


class LeadgenApiHandler(BaseHTTPRequestHandler):
    store = CrmStore(get_config().state_db)
    logger = JsonlLogger(get_config().log_dir / "events.jsonl")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {"status": "ok"})
            return
        if parsed.path == "/api/status":
            self._send_json(200, build_snapshot())
            return
        if parsed.path in {"/dashboard", "/"}:
            self._send_html(200, render_dashboard_html())
            return

        if parsed.path == "/unsubscribe":
            query = parse_qs(parsed.query)
            lead_id_raw = (query.get("lead_id") or [""])[0]
            channel = ((query.get("channel") or ["EMAIL"])[0] or "EMAIL").upper()
            try:
                lead_id = int(lead_id_raw)
            except Exception:
                self._send_json(400, {"ok": False, "error": "invalid_lead_id"})
                return

            email, phone = self.store.get_contact(lead_id)
            contact = email if channel == "EMAIL" else phone
            if not contact:
                self._send_json(404, {"ok": False, "error": "contact_not_found"})
                return

            self.store.register_opt_out(contact, channel, "unsubscribe_link")
            self.store.update_stage(lead_id, "UNSUBSCRIBED")
            self.logger.write("opt_out_registered", {"lead_id": lead_id, "channel": channel, "source": "unsubscribe_link"})

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>OK</h1><p>Voce foi removido da lista.</p></body></html>")
            return

        self._send_json(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/webhooks/resend-inbound":
            self._handle_resend_inbound()
            return
        self._send_json(404, {"ok": False, "error": "not_found"})

    def log_message(self, format: str, *args) -> None:
        return

    def _send_json(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, code: int, html: str) -> None:
        raw = html.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _handle_resend_inbound(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(body.decode("utf-8", errors="ignore"))
        except Exception:
            self._send_json(400, {"ok": False, "error": "invalid_json"})
            return

        # Resend structures can vary by event type; keep extraction defensive.
        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        from_email = str(
            data.get("from")
            or data.get("from_email")
            or data.get("sender")
            or ""
        ).strip()
        text = str(data.get("text") or data.get("text_body") or data.get("body") or "").strip()
        html = str(data.get("html") or data.get("html_body") or "").strip()
        if not text and html:
            text = html

        lead_id = self.store.get_lead_id_by_email(from_email)
        if not lead_id:
            self.logger.write(
                "reply_received_unmatched",
                {"channel": "EMAIL", "from": from_email, "payload_keys": list(data.keys()) if isinstance(data, dict) else []},
            )
            self._send_json(202, {"ok": True, "matched": False})
            return

        classification, confidence = classify_reply(text)
        self.store.save_reply(lead_id, "EMAIL", text, classification, confidence)
        self.logger.write(
            "reply_received",
            {
                "lead_id": lead_id,
                "channel": "EMAIL",
                "classification": classification,
                "confidence": confidence,
            },
        )

        if classification == "opt_out" or is_opt_out_reply(text):
            self.store.register_opt_out(from_email, "EMAIL", "inbound_reply")
            self.store.update_stage(lead_id, "UNSUBSCRIBED")
            self.logger.write("opt_out_registered", {"lead_id": lead_id, "channel": "EMAIL", "source": "inbound_reply"})
        elif classification == "positive":
            self.store.set_consent(lead_id, accepted=True)
            self.logger.write("consent_received", {"lead_id": lead_id, "channel": "EMAIL", "source": "inbound_reply"})
        else:
            self.store.update_stage(lead_id, "WAITING_REPLY")

        self._send_json(200, {"ok": True, "matched": True, "lead_id": lead_id, "classification": classification})


def run_server(host: str = "0.0.0.0", port: int = 8787) -> None:
    httpd = ThreadingHTTPServer((host, port), LeadgenApiHandler)
    httpd.serve_forever()
