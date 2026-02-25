from __future__ import annotations

import json
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import get_config
from .crm_store import CrmStore
from .logging_utils import JsonlLogger
from .monitor_dashboard import build_snapshot, render_dashboard_html
from .outreach import classify_codex_intent, detect_plan_choice, get_resend_client_from_env, is_opt_out_reply
from .payment import get_stripe_client_from_env
from .time_utils import UTC

EMAIL_RE = re.compile(r"([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})")


class LeadgenApiHandler(BaseHTTPRequestHandler):
    store = CrmStore(get_config().state_db)
    logger = JsonlLogger(get_config().log_dir / "events.jsonl")
    email_client = get_resend_client_from_env()
    stripe_client = get_stripe_client_from_env()
    codex_confidence_min = float(__import__("os").getenv("LEADGEN_REPLY_CONFIDENCE_MIN", "0.65"))

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
        if parsed.path == "/api/pricing/state":
            st = self.store.get_pricing_state()
            self._send_json(
                200,
                {
                    "price_level": st.price_level,
                    "price_full": st.price_full,
                    "price_simple": st.price_simple,
                    "baseline_conversion": st.baseline_conversion,
                    "offers_in_window": st.offers_in_window,
                    "sales_in_window": st.sales_in_window,
                    "updated_at_utc": st.updated_at_utc,
                },
            )
            return
        if parsed.path == "/api/domains/jobs":
            self._send_json(200, {"jobs": self.store.list_domain_jobs(limit=250)})
            return
        if parsed.path == "/api/replies/queue":
            query = parse_qs(parsed.query)
            raw_status = (query.get("status") or [""])[0]
            statuses = [x.strip().upper() for x in raw_status.split(",") if x.strip()] if raw_status else None
            items = self.store.list_reply_review_queue(statuses=statuses, limit=250)
            self._send_json(
                200,
                {
                    "counts": self.store.pending_reply_counts(),
                    "items": [
                        {
                            "id": it.id,
                            "lead_id": it.lead_id,
                            "channel": it.channel,
                            "inbound_text": it.inbound_text,
                            "status": it.status,
                            "intent_final": it.intent_final,
                            "draft_reply": it.draft_reply,
                            "confidence": it.confidence,
                            "created_at_utc": it.created_at_utc,
                            "updated_at_utc": it.updated_at_utc,
                        }
                        for it in items
                    ],
                },
            )
            return
        if parsed.path == "/api/payments/health":
            self._send_json(200, {"stripe_configured": self.stripe_client is not None})
            return

        if parsed.path == "/unsubscribe":
            self._handle_unsubscribe(parsed.query)
            return

        self._send_json(404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/webhooks/resend-inbound":
            self._handle_resend_inbound()
            return
        if parsed.path == "/webhooks/stripe":
            self._handle_stripe_webhook()
            return
        if parsed.path.startswith("/api/replies/") and parsed.path.endswith("/codex-decision"):
            self._handle_codex_decision(parsed.path)
            return
        if parsed.path.startswith("/api/replies/") and parsed.path.endswith("/send"):
            self._handle_send_reply(parsed.path)
            return
        if parsed.path == "/api/sales/mark":
            self._handle_sales_mark()
            return
        if parsed.path.startswith("/api/domains/") and parsed.path.endswith("/status"):
            self._handle_domain_status(parsed.path)
            return
        if parsed.path == "/api/payments/checkout":
            self._handle_create_checkout()
            return
        self._send_json(404, {"ok": False, "error": "not_found"})

    def log_message(self, format: str, *args) -> None:
        return

    def _send_json(self, code: int, payload: dict) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
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

    def _read_json(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            raw = json.loads(body.decode("utf-8", errors="ignore"))
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _extract_email(self, value: str) -> str:
        m = EMAIL_RE.search(value or "")
        return str(m.group(1)).strip().lower() if m else ""

    def _parse_id_path(self, path: str, prefix: str, suffix: str) -> int | None:
        if not path.startswith(prefix) or not path.endswith(suffix):
            return None
        middle = path[len(prefix) : -len(suffix)]
        middle = middle.strip("/")
        if not middle.isdigit():
            return None
        return int(middle)

    def _handle_unsubscribe(self, query_raw: str) -> None:
        query = parse_qs(query_raw)
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

    def _handle_resend_inbound(self) -> None:
        payload = self._read_json()
        data = payload.get("data", payload) if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            data = {}

        from_raw = str(data.get("from") or data.get("from_email") or data.get("sender") or "").strip()
        from_email = self._extract_email(from_raw)
        text = str(data.get("text") or data.get("text_body") or data.get("body") or "").strip()
        html = str(data.get("html") or data.get("html_body") or "").strip()
        if not text and html:
            text = re.sub(r"<[^>]+>", " ", html).strip()

        lead_id = self.store.get_lead_id_by_email(from_email)
        if not lead_id:
            self.logger.write(
                "reply_received_unmatched",
                {"channel": "EMAIL", "from": from_email, "payload_keys": list(data.keys())},
            )
            self._send_json(202, {"ok": True, "matched": False, "queued": False})
            return

        queue_id = self.store.enqueue_reply_review(lead_id=lead_id, channel="EMAIL", inbound_text=text)
        self.store.save_reply(lead_id, "EMAIL", text, "queued_for_codex", 0.0)
        self.logger.write(
            "reply_queued_for_codex",
            {
                "lead_id": lead_id,
                "queue_id": queue_id,
                "channel": "EMAIL",
                "from": from_email,
            },
        )
        self._send_json(202, {"ok": True, "matched": True, "queued": True, "lead_id": lead_id, "queue_id": queue_id})

    def _handle_codex_decision(self, path: str) -> None:
        queue_id = self._parse_id_path(path, "/api/replies/", "/codex-decision")
        if not queue_id:
            self._send_json(400, {"ok": False, "error": "invalid_queue_id"})
            return
        item = self.store.get_reply_review_item(queue_id)
        if not item:
            self._send_json(404, {"ok": False, "error": "queue_item_not_found"})
            return

        payload = self._read_json()
        intent = str(payload.get("intent_final") or "").strip().lower() or classify_codex_intent(item.inbound_text)
        draft = str(payload.get("draft_reply") or "").strip()
        confidence = float(payload.get("confidence") if payload.get("confidence") is not None else 0.0)
        if confidence < 0:
            confidence = 0.0
        if confidence > 1:
            confidence = 1.0
        status = "CODEX_DONE" if confidence >= self.codex_confidence_min else "REVIEW_REQUIRED"
        self.store.set_reply_codex_decision(queue_id, intent_final=intent, draft_reply=draft, confidence=confidence, status=status)
        self.logger.write(
            "reply_codex_decided",
            {
                "queue_id": queue_id,
                "lead_id": item.lead_id,
                "intent_final": intent,
                "confidence": confidence,
                "status": status,
            },
        )

        action = "none"
        if status == "REVIEW_REQUIRED":
            self._send_json(200, {"ok": True, "queue_id": queue_id, "status": status, "intent_final": intent, "action": "review_required"})
            return

        if intent == "opt_out" or is_opt_out_reply(item.inbound_text):
            email, _phone = self.store.get_contact(item.lead_id)
            if email:
                self.store.register_opt_out(email, "EMAIL", "codex_decision")
            self.store.update_stage(item.lead_id, "UNSUBSCRIBED")
            self.logger.write("opt_out_registered", {"lead_id": item.lead_id, "channel": "EMAIL", "source": "codex_decision"})
            action = "opt_out"
        elif intent == "positive_offer_accept":
            accepted_plan = str(payload.get("accepted_plan") or "").strip().upper() or detect_plan_choice(item.inbound_text)
            if self.store.has_offer_sent(item.lead_id):
                run_id = str(payload.get("run_id") or f"api-codex-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}")
                sale = self.store.mark_sale(
                    lead_id=item.lead_id,
                    run_id=run_id,
                    reason="positive_offer_accept",
                    accepted_plan=accepted_plan,
                )
                self.logger.write(
                    "pricing_level_up",
                    {
                        "run_id": run_id,
                        "lead_id": item.lead_id,
                        "from_level": sale["old_level"],
                        "to_level": sale["new_level"],
                        "sale_amount": sale["sale_amount"],
                        "accepted_plan": sale["accepted_plan"],
                    },
                )
                self.logger.write("domain_job_created", {"run_id": run_id, "lead_id": item.lead_id, "status": "DOMAIN_SELECTED"})
                action = "sale_marked"
            else:
                self.store.set_consent(item.lead_id, accepted=True)
                self.logger.write("consent_received", {"lead_id": item.lead_id, "channel": "EMAIL", "source": "codex_decision"})
                action = "consent_marked"
        elif intent in {"objection_price", "objection_trust", "not_now", "other"}:
            self.store.update_stage(item.lead_id, "WAITING_REPLY")
            action = "waiting_reply"

        self._send_json(200, {"ok": True, "queue_id": queue_id, "status": status, "intent_final": intent, "action": action})

    def _handle_send_reply(self, path: str) -> None:
        queue_id = self._parse_id_path(path, "/api/replies/", "/send")
        if not queue_id:
            self._send_json(400, {"ok": False, "error": "invalid_queue_id"})
            return
        item = self.store.get_reply_review_item(queue_id)
        if not item:
            self._send_json(404, {"ok": False, "error": "queue_item_not_found"})
            return
        if item.status != "CODEX_DONE":
            self._send_json(409, {"ok": False, "error": "queue_item_not_ready", "status": item.status})
            return
        if not item.draft_reply.strip():
            self._send_json(400, {"ok": False, "error": "missing_draft_reply"})
            return
        if not self.email_client:
            self._send_json(500, {"ok": False, "error": "email_client_not_configured"})
            return

        email, _phone = self.store.get_contact(item.lead_id)
        if not email:
            self._send_json(404, {"ok": False, "error": "lead_email_not_found"})
            return
        payload = self._read_json()
        subject = str(payload.get("subject") or "Re: atualizacao da sua pagina").strip()
        html = item.draft_reply.replace("\n", "<br>")
        sent = self.email_client.send(email, subject, html)
        self.store.save_touch(
            item.lead_id,
            "EMAIL",
            "REPLY_AUTOMATION",
            f"reply_{item.intent_final or 'generic'}",
            sent.status,
            sent.message_id,
            item.draft_reply,
        )
        if not sent.ok:
            self._send_json(502, {"ok": False, "error": "send_failed", "detail": sent.detail})
            return
        self.store.mark_reply_sent(queue_id)
        self.logger.write(
            "reply_sent",
            {
                "queue_id": queue_id,
                "lead_id": item.lead_id,
                "intent_final": item.intent_final,
                "message_id": sent.message_id,
            },
        )
        self._send_json(200, {"ok": True, "queue_id": queue_id, "message_id": sent.message_id})

    def _handle_sales_mark(self) -> None:
        payload = self._read_json()
        try:
            lead_id = int(payload.get("lead_id"))
        except Exception:
            self._send_json(400, {"ok": False, "error": "invalid_lead_id"})
            return
        if not self.store.has_offer_sent(lead_id):
            self._send_json(409, {"ok": False, "error": "offer_not_sent_for_lead"})
            return
        run_id = str(payload.get("run_id") or f"api-sale-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}")
        reason = str(payload.get("reason") or "manual_sale_mark")
        accepted_plan = str(payload.get("accepted_plan") or "COMPLETO").upper()
        sale = self.store.mark_sale(lead_id=lead_id, run_id=run_id, reason=reason, accepted_plan=accepted_plan)
        self.logger.write(
            "pricing_level_up",
            {
                "run_id": run_id,
                "lead_id": lead_id,
                "from_level": sale["old_level"],
                "to_level": sale["new_level"],
                "sale_amount": sale["sale_amount"],
                "accepted_plan": sale["accepted_plan"],
            },
        )
        self.logger.write("domain_job_created", {"run_id": run_id, "lead_id": lead_id, "status": "DOMAIN_SELECTED"})
        self._send_json(200, {"ok": True, "sale": sale})

    def _handle_domain_status(self, path: str) -> None:
        job_id = self._parse_id_path(path, "/api/domains/", "/status")
        if not job_id:
            self._send_json(400, {"ok": False, "error": "invalid_job_id"})
            return
        payload = self._read_json()
        status = str(payload.get("status") or "").strip().upper()
        if not status:
            self._send_json(400, {"ok": False, "error": "missing_status"})
            return
        domain_name = str(payload.get("domain_name") or "").strip()
        expires_at_utc = str(payload.get("expires_at_utc") or "").strip()
        notes = str(payload.get("notes") or "").strip()
        self.store.update_domain_job(
            job_id=job_id,
            status=status,
            domain_name=domain_name,
            expires_at_utc=expires_at_utc,
            notes=notes,
        )
        self.logger.write(
            "domain_job_updated",
            {
                "job_id": job_id,
                "status": status,
                "domain_name": domain_name,
                "expires_at_utc": expires_at_utc,
            },
        )
        self._send_json(200, {"ok": True, "job_id": job_id, "status": status})

    def _handle_create_checkout(self) -> None:
        if not self.stripe_client:
            self._send_json(500, {"ok": False, "error": "stripe_not_configured"})
            return
        payload = self._read_json()
        try:
            lead_id = int(payload.get("lead_id"))
        except Exception:
            self._send_json(400, {"ok": False, "error": "invalid_lead_id"})
            return
        plan = str(payload.get("plan") or "COMPLETO").strip().upper()
        context = self.store.get_lead_sale_context(lead_id)
        if not context:
            self._send_json(404, {"ok": False, "error": "lead_not_found"})
            return
        pricing = self.store.get_pricing_state()
        amount = pricing.price_simple if plan == "SIMPLES" else pricing.price_full
        success_url = str(payload.get("success_url") or "https://api.renandias.site/payment/success?session_id={CHECKOUT_SESSION_ID}")
        cancel_url = str(payload.get("cancel_url") or "https://api.renandias.site/payment/cancel")
        checkout = self.stripe_client.create_checkout_session(
            amount_brl=amount,
            lead_id=lead_id,
            plan=plan,
            business_name=context.get("business_name") or f"Lead {lead_id}",
            success_url=success_url,
            cancel_url=cancel_url,
        )
        if not checkout.ok:
            self._send_json(502, {"ok": False, "error": "stripe_checkout_failed", "detail": checkout.detail})
            return
        self._send_json(200, {"ok": True, "checkout_url": checkout.url, "session_id": checkout.session_id, "plan": plan, "amount_brl": amount})

    def _handle_stripe_webhook(self) -> None:
        payload = self._read_json()
        event_type = str(payload.get("type") or "")
        data_obj = payload.get("data", {}).get("object", {}) if isinstance(payload.get("data"), dict) else {}
        if not isinstance(data_obj, dict):
            data_obj = {}
        if event_type != "checkout.session.completed":
            self._send_json(200, {"ok": True, "ignored": True, "event_type": event_type})
            return

        metadata = data_obj.get("metadata", {}) if isinstance(data_obj.get("metadata"), dict) else {}
        lead_id_raw = metadata.get("lead_id") or data_obj.get("client_reference_id")
        plan = str(metadata.get("plan") or "COMPLETO").strip().upper()
        try:
            lead_id = int(lead_id_raw)
        except Exception:
            self._send_json(400, {"ok": False, "error": "missing_lead_id"})
            return

        amount_total = data_obj.get("amount_total")
        sale_amount = (float(amount_total) / 100.0) if isinstance(amount_total, (int, float)) else None
        run_id = f"stripe-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        sale = self.store.mark_sale(
            lead_id=lead_id,
            run_id=run_id,
            reason="stripe_webhook_checkout_completed",
            accepted_plan=plan,
            sale_amount=sale_amount,
        )
        self.logger.write(
            "sale_marked",
            {
                "run_id": run_id,
                "lead_id": lead_id,
                "source": "stripe_webhook",
                "accepted_plan": sale["accepted_plan"],
                "sale_amount": sale["sale_amount"],
            },
        )
        self.logger.write(
            "pricing_level_up",
            {
                "run_id": run_id,
                "lead_id": lead_id,
                "from_level": sale["old_level"],
                "to_level": sale["new_level"],
                "accepted_plan": sale["accepted_plan"],
                "sale_amount": sale["sale_amount"],
            },
        )
        self.logger.write("domain_job_created", {"run_id": run_id, "lead_id": lead_id, "status": "DOMAIN_SELECTED"})
        self._send_json(200, {"ok": True, "sale": sale})


def run_server(host: str = "0.0.0.0", port: int = 8787) -> None:
    httpd = ThreadingHTTPServer((host, port), LeadgenApiHandler)
    httpd.serve_forever()
