from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import get_config
from .crm_store import CrmStore
from .logging_utils import JsonlLogger


class LeadgenApiHandler(BaseHTTPRequestHandler):
    store = CrmStore(get_config().state_db)
    logger = JsonlLogger(get_config().log_dir / "events.jsonl")

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {"status": "ok"})
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

    def log_message(self, format: str, *args) -> None:
        return

    def _send_json(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def run_server(host: str = "0.0.0.0", port: int = 8787) -> None:
    httpd = ThreadingHTTPServer((host, port), LeadgenApiHandler)
    httpd.serve_forever()
