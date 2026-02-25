from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .anti_ban import (
    AntiBanThresholds,
    email_warmup_daily_limit,
    should_enable_global_safe_mode,
    should_pause_email,
    should_pause_whatsapp,
)
from .config import get_config
from .crm_store import CrmStore
from .demo_site import DemoSiteBuilder, slugify
from .enrichment import enrich_with_website_contacts
from .incident import IncidentEngine
from .logging_utils import JsonlLogger
from .ops_state import OperationalState
from .payment import get_stripe_client_from_env
from .outreach import (
    detect_plan_choice,
    classify_reply,
    get_resend_client_from_env,
    get_wpp_client_from_env,
    initial_consent_email,
    initial_consent_whatsapp,
    is_opt_out_reply,
    normalize_phone_br,
    offer_email,
    offer_followup_email,
    offer_whatsapp,
    followup_consent_email,
    followup_consent_whatsapp,
    random_human_delay,
    build_unsubscribe_url,
)
from .scraper import GoogleMapsScraper, ScrapePausedError, ScrapeRequest
from .time_utils import UTC


@dataclass
class PipelineSummary:
    run_id: str
    leads_ingested: int
    consent_sent: int
    followups_sent: int
    offers_sent: int


class LeadPipelineRunner:
    def __init__(self) -> None:
        self.cfg = get_config()
        self.logger = JsonlLogger(self.cfg.log_dir / "events.jsonl")
        self.scraper = GoogleMapsScraper()
        self.store = CrmStore(self.cfg.state_db)
        self.ops = OperationalState(self.cfg.ops_state_db)
        self.stripe_client = get_stripe_client_from_env()
        self.incident_engine = IncidentEngine(
            db_path=self.cfg.log_dir / "incident_state.db",
            policy=self.cfg.incident,
            incident_dir=self.cfg.log_dir / "incidents",
        )
        self.thresholds = AntiBanThresholds()
        self.email_client = get_resend_client_from_env()
        self.wa_client = get_wpp_client_from_env()
        preview_base = os.getenv("PREVIEW_BASE_URL", "http://localhost:8080")
        preview_dir = Path(os.getenv("PREVIEW_PUBLISH_DIR", str(self.cfg.preview_dir)))
        self.demo_builder = DemoSiteBuilder(base_url=preview_base, publish_dir=preview_dir)
        self.unsubscribe_base = os.getenv("UNSUBSCRIBE_BASE_URL", preview_base)
        self.payment_success_url = os.getenv("STRIPE_SUCCESS_URL", f"{preview_base}/payment/success?session_id={{CHECKOUT_SESSION_ID}}")
        self.payment_cancel_url = os.getenv("STRIPE_CANCEL_URL", f"{preview_base}/payment/cancel")
        self.wa_daily_limit = int(os.getenv("LEADGEN_WA_DAILY_LIMIT", "40"))
        self.allow_relaxed_icp = os.getenv("LEADGEN_ALLOW_RELAXED_ICP", "1").strip().lower() in {"1", "true", "yes", "on"}
        self.email_only = os.getenv("LEADGEN_EMAIL_ONLY", "0").strip().lower() in {"1", "true", "yes", "on"}
        self.close_days = int(os.getenv("LEADGEN_CLOSE_DAYS", "7"))
        self.reply_confidence_min = float(os.getenv("LEADGEN_REPLY_CONFIDENCE_MIN", "0.65"))
        if self.email_only:
            self.wa_client = None

    def run_all(
        self,
        audience: str,
        location: str,
        max_results: int,
        headless: bool,
        enrich_website: bool,
        payment_url: str,
    ) -> PipelineSummary:
        run_id = datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ")
        self.logger.write("pipeline_run_started", {"run_id": run_id, "audience": audience, "location": location})

        leads_ingested = self.ingest(run_id, audience, location, max_results, headless, enrich_website)
        consent_sent = self.send_initial_outreach(run_id)
        followups_sent = self.send_followups(run_id)
        offers_sent = self.send_offers_for_consented(run_id, payment_url=payment_url)
        closed_lost = self.close_stale_sequences(run_id)
        self._emit_domain_expiry_alerts(run_id)

        self.logger.write(
            "pipeline_run_finished",
            {
                "run_id": run_id,
                "leads_ingested": leads_ingested,
                "consent_sent": consent_sent,
                "followups_sent": followups_sent,
                "offers_sent": offers_sent,
                "closed_lost": closed_lost,
            },
        )
        return PipelineSummary(
            run_id=run_id,
            leads_ingested=leads_ingested,
            consent_sent=consent_sent,
            followups_sent=followups_sent,
            offers_sent=offers_sent,
        )

    def ingest(
        self,
        run_id: str,
        audience: str,
        location: str,
        max_results: int,
        headless: bool,
        enrich_website: bool,
    ) -> int:
        if self.ops.global_safe_mode_enabled():
            self.logger.write("safe_mode_enabled", {"run_id": run_id, "reason": "global_safe_mode"})

        if self.ops.is_channel_paused("SCRAPE"):
            self.logger.write("channel_paused", {"run_id": run_id, "channel": "SCRAPE", "reason": "cooldown"})
            return 0

        try:
            result = self.scraper.scrape(
                ScrapeRequest(
                    audience=audience,
                    location=location,
                    max_results=max_results,
                    headless=headless,
                )
            )
            rows = result.rows
            if enrich_website:
                rows = enrich_with_website_contacts(rows, self.logger, run_id)

            rows_qualified = [row for row in rows if not str(row.get("website", "")).strip()]
            relaxed = False
            if not rows_qualified and self.allow_relaxed_icp:
                # Fallback pragmatica: se zero sem-site, permite leads com telefone/email para nao travar a run.
                rows_qualified = [
                    row
                    for row in rows
                    if str(row.get("phone", "")).strip() or str(row.get("website_emails", "")).strip()
                ]
                relaxed = True
                self.logger.write(
                    "icp_relaxed_mode_enabled",
                    {
                        "run_id": run_id,
                        "reason": "no_website_leads_found",
                        "strict_candidates": 0,
                        "relaxed_candidates": len(rows_qualified),
                    },
                )

            leads_before = self.store.count_leads()
            for row in rows_qualified:
                lead_id = self.store.upsert_lead_from_row(run_id, row)
                self.store.update_stage(lead_id, "QUALIFIED")
                self.logger.write(
                    "lead_qualified",
                    {
                        "run_id": run_id,
                        "lead_id": lead_id,
                        "business_name": row.get("name", ""),
                        "qualified_mode": "RELAXED" if relaxed else "STRICT",
                        "channel_selected": "EMAIL" if str(row.get("website_emails", "")).strip() else "WHATSAPP",
                    },
                )
            leads_after = self.store.count_leads()
            new_leads = max(0, leads_after - leads_before)
            self.logger.write(
                "ingest_summary",
                {
                    "run_id": run_id,
                    "qualified_rows": len(rows_qualified),
                    "new_leads": new_leads,
                    "mode": "RELAXED" if relaxed else "STRICT",
                },
            )

            self.ops.record_run(run_id, "SCRAPE", unstable=result.unstable, reason="risk_signals" if result.unstable else "ok")
            self.logger.write(
                "scrape_metrics",
                {
                    "run_id": run_id,
                    "captcha_events": result.captcha_events,
                    "timeout_events": result.timeout_events,
                    "http_429_events": result.http_429_events,
                    "consecutive_error_peak": result.consecutive_error_peak,
                },
            )
            if result.captcha_events > 0:
                self.logger.write("captcha_detected", {"run_id": run_id, "count": result.captcha_events})

            if self.ops.unstable_streak("SCRAPE") >= 2:
                self.ops.set_channel_paused("SCRAPE", "unstable_runs", cooldown_hours=12)
                self.logger.write("channel_paused", {"run_id": run_id, "channel": "SCRAPE", "reason": "unstable_runs"})

            self._evaluate_global_safe_mode(run_id)
            return new_leads
        except ScrapePausedError as exc:
            self.ops.record_run(run_id, "SCRAPE", unstable=True, reason=str(exc))
            self.ops.set_channel_paused("SCRAPE", "error_streak", cooldown_hours=12)
            self.logger.write("channel_paused", {"run_id": run_id, "channel": "SCRAPE", "reason": str(exc)})
            self._register_incident(
                run_id=run_id,
                error_type="ScrapePausedError",
                message=str(exc),
                context={"stage": "ingest", "audience": audience, "location": location},
            )
            self._evaluate_global_safe_mode(run_id)
            return 0
        except Exception as exc:
            reason = f"SCRAPE_RUNTIME_ERROR:{type(exc).__name__}"
            self.ops.record_run(run_id, "SCRAPE", unstable=True, reason=reason)
            self.ops.set_channel_paused("SCRAPE", "runtime_error", cooldown_hours=12)
            self.logger.write(
                "channel_paused",
                {"run_id": run_id, "channel": "SCRAPE", "reason": reason, "detail": str(exc)[:220]},
            )
            self._register_incident(
                run_id=run_id,
                error_type=type(exc).__name__,
                message=str(exc),
                context={"stage": "ingest", "audience": audience, "location": location},
            )
            self._evaluate_global_safe_mode(run_id)
            return 0

    def send_initial_outreach(self, run_id: str) -> int:
        if self.ops.global_safe_mode_enabled():
            self.logger.write("safe_mode_enabled", {"run_id": run_id, "reason": "global_safe_mode_blocks_outreach"})
            return 0

        leads = self.store.list_leads_for_initial_contact(limit=250)
        count = 0
        day_index = self._campaign_day_index()
        email_limit = email_warmup_daily_limit(day_index)
        email_metrics = self.ops.get_channel_metrics("EMAIL")
        wa_metrics = self.ops.get_channel_metrics("WHATSAPP")

        for lead in leads:
            if count >= 250:
                break
            if self.email_only and lead.channel_preferred == "WHATSAPP":
                self.logger.write(
                    "lead_skipped",
                    {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "reason": "email_only_mode"},
                )
                continue
            if lead.channel_preferred == "EMAIL" and lead.email:
                if self.ops.is_channel_paused("EMAIL"):
                    continue
                if email_metrics.sent >= email_limit:
                    self.logger.write(
                        "deliverability_alert",
                        {
                            "run_id": run_id,
                            "channel": "EMAIL",
                            "daily_sent": email_metrics.sent,
                            "daily_limit": email_limit,
                        },
                    )
                    break

                if self.store.is_opted_out(lead.email, "EMAIL"):
                    continue
                if not self.email_client:
                    self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "EMAIL", "reason": "client_not_configured"})
                    continue

                variant = (lead.id % 3) + 1
                unsub = build_unsubscribe_url(self.unsubscribe_base, lead.id, "EMAIL")
                subject, body_text, html = initial_consent_email(
                    lead.business_name,
                    unsub,
                    variant=variant,
                    city=lead.address,
                )
                sent = self.email_client.send(lead.email, subject, html)
                self.store.save_touch(lead.id, "EMAIL", "CONSENT_REQUEST", f"email_v{variant}", sent.status, sent.message_id, body_text)
                self.ops.add_channel_metrics("EMAIL", sent=1, failed=0 if sent.ok else 1)
                email_metrics = self.ops.get_channel_metrics("EMAIL")
                count += 1
                if sent.ok:
                    self.store.update_stage(lead.id, "WAITING_REPLY")
                    self.logger.write("contact_delivered", {"run_id": run_id, "lead_id": lead.id, "channel": "EMAIL", "daily_sent": email_metrics.sent})
                else:
                    self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "EMAIL", "detail": sent.detail})

                random_human_delay()
                self._evaluate_email_health(run_id)
                continue

            if lead.channel_preferred == "WHATSAPP" and lead.phone:
                if self.ops.is_channel_paused("WHATSAPP"):
                    continue
                if wa_metrics.sent >= self.wa_daily_limit:
                    self.logger.write(
                        "deliverability_alert",
                        {
                            "run_id": run_id,
                            "channel": "WHATSAPP",
                            "daily_sent": wa_metrics.sent,
                            "daily_limit": self.wa_daily_limit,
                        },
                    )
                    break
                if self.store.is_opted_out(lead.phone, "WHATSAPP"):
                    continue
                if not self.wa_client:
                    self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "reason": "client_not_configured"})
                    continue
                normalized = normalize_phone_br(lead.phone)
                if not normalized:
                    self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "reason": "invalid_phone"})
                    continue
                msg = initial_consent_whatsapp(lead.business_name)
                sent = self.wa_client.send(normalized, msg)
                self.store.save_touch(lead.id, "WHATSAPP", "CONSENT_REQUEST", "wa_v1", sent.status, sent.message_id, msg)
                self.ops.add_channel_metrics("WHATSAPP", sent=1, failed=0 if sent.ok else 1)
                wa_metrics = self.ops.get_channel_metrics("WHATSAPP")
                count += 1
                if sent.ok:
                    self.store.update_stage(lead.id, "WAITING_REPLY")
                    self.logger.write("contact_delivered", {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP"})
                else:
                    self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "detail": sent.detail})
                random_human_delay()
                self._evaluate_whatsapp_health(run_id)

        self._evaluate_global_safe_mode(run_id)
        return count

    def send_followups(self, run_id: str) -> int:
        if self.ops.global_safe_mode_enabled():
            return 0
        count = 0

        # Consent follow-ups: D+2 and D+4.
        leads = self.store.list_leads_waiting_reply(limit=300)
        for lead in leads:
            if self.email_only and lead.channel_preferred == "WHATSAPP":
                self.logger.write(
                    "followup_skipped",
                    {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "reason": "email_only_mode"},
                )
                continue

            step = self.store.count_touches(lead.id, intent="CONSENT_REQUEST")
            first_touch = self.store.get_first_touch_timestamp(lead.id, intent="CONSENT_REQUEST")
            if not first_touch:
                continue
            days_since = max(0, (datetime.now(UTC) - datetime.fromisoformat(first_touch)).days)
            if (step == 1 and days_since < 2) or (step == 2 and days_since < 4):
                continue
            if step < 1 or step >= 3:
                continue

            if lead.channel_preferred == "EMAIL" and lead.email and self.email_client and not self.ops.is_channel_paused("EMAIL"):
                unsub = build_unsubscribe_url(self.unsubscribe_base, lead.id, "EMAIL")
                subject, body_text, html = followup_consent_email(lead.business_name, unsub, step=step)
                sent = self.email_client.send(lead.email, subject, html)
                self.store.save_touch(lead.id, "EMAIL", "CONSENT_REQUEST", f"email_followup_{step}", sent.status, sent.message_id, body_text)
                self.ops.add_channel_metrics("EMAIL", sent=1, failed=0 if sent.ok else 1)
                count += 1
                random_human_delay()
                continue

            if lead.channel_preferred == "WHATSAPP" and lead.phone and self.wa_client and not self.ops.is_channel_paused("WHATSAPP"):
                normalized = normalize_phone_br(lead.phone)
                if not normalized:
                    continue
                body = followup_consent_whatsapp(lead.business_name, step=step)
                sent = self.wa_client.send(normalized, body)
                self.store.save_touch(lead.id, "WHATSAPP", "CONSENT_REQUEST", f"wa_followup_{step}", sent.status, sent.message_id, body)
                self.ops.add_channel_metrics("WHATSAPP", sent=1, failed=0 if sent.ok else 1)
                count += 1
                random_human_delay()

        # Offer follow-ups: D+1 and D+3.
        offered = self.store.list_leads_by_stage("PAYMENT_SENT", limit=300)
        for lead in offered:
            if lead.channel_preferred != "EMAIL" or not lead.email or not self.email_client:
                continue
            if self.ops.is_channel_paused("EMAIL"):
                continue
            offer_step = self.store.count_touches(lead.id, intent="OFFER")
            first_offer = self.store.get_first_touch_timestamp(lead.id, intent="OFFER")
            if not first_offer:
                continue
            days_since_offer = max(0, (datetime.now(UTC) - datetime.fromisoformat(first_offer)).days)
            next_step = 0
            if offer_step == 1 and days_since_offer >= 1:
                next_step = 1
            elif offer_step == 2 and days_since_offer >= 3:
                next_step = 2
            if next_step == 0:
                continue
            unsub = build_unsubscribe_url(self.unsubscribe_base, lead.id, "EMAIL")
            subject, body_text, html = offer_followup_email(lead.business_name, unsub, step=next_step)
            sent = self.email_client.send(lead.email, subject, html)
            self.store.save_touch(lead.id, "EMAIL", "OFFER", f"email_offer_followup_{next_step}", sent.status, sent.message_id, body_text)
            self.ops.add_channel_metrics("EMAIL", sent=1, failed=0 if sent.ok else 1)
            count += 1
            random_human_delay()

        closed_lost = self.close_stale_sequences(run_id)
        if count or closed_lost:
            self.logger.write("followup_batch_sent", {"run_id": run_id, "count": count, "closed_lost": closed_lost})
        self._evaluate_email_health(run_id)
        self._evaluate_whatsapp_health(run_id)
        self._evaluate_global_safe_mode(run_id)
        return count

    def process_reply(self, run_id: str, lead_id: int, channel: str, text: str) -> None:
        classification, confidence = classify_reply(text)
        self.store.save_reply(lead_id, channel, text, classification, confidence)
        self.logger.write(
            "reply_received",
            {
                "run_id": run_id,
                "lead_id": lead_id,
                "channel": channel,
                "classification": classification,
                "confidence": confidence,
            },
        )
        if classification == "opt_out" or is_opt_out_reply(text):
            email, phone = self.store.get_contact(lead_id)
            contact = email if channel == "EMAIL" else phone
            if contact:
                self.store.register_opt_out(contact, channel, "user_request")
            self.store.update_stage(lead_id, "UNSUBSCRIBED")
            self.logger.write("opt_out_registered", {"run_id": run_id, "lead_id": lead_id, "channel": channel})
            return
        if classification == "positive":
            if self.store.has_offer_sent(lead_id):
                plan = detect_plan_choice(text)
                info = self.mark_sale(run_id=run_id, lead_id=lead_id, accepted_plan=plan, reason="positive_after_offer")
                self.logger.write(
                    "sale_marked",
                    {
                        "run_id": run_id,
                        "lead_id": lead_id,
                        "channel": channel,
                        "accepted_plan": info["accepted_plan"],
                        "sale_amount": info["sale_amount"],
                        "price_level": info["new_level"],
                    },
                )
            else:
                self.store.set_consent(lead_id, accepted=True)
                self.logger.write("consent_received", {"run_id": run_id, "lead_id": lead_id, "channel": channel})
            return
        self.store.update_stage(lead_id, "WAITING_REPLY")
        self.logger.write("objection_handled", {"run_id": run_id, "lead_id": lead_id, "classification": classification})

    def send_offers_for_consented(self, run_id: str, payment_url: str) -> int:
        if self.ops.global_safe_mode_enabled():
            self.logger.write("safe_mode_enabled", {"run_id": run_id, "reason": "global_safe_mode_blocks_offer"})
            return 0

        leads = self.store.list_leads_for_offer(limit=200)
        sent_count = 0
        wa_metrics = self.ops.get_channel_metrics("WHATSAPP")
        for lead in leads:
            if self.email_only and lead.channel_preferred == "WHATSAPP":
                self.logger.write(
                    "offer_skipped",
                    {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "reason": "email_only_mode"},
                )
                continue
            slug = f"{slugify(lead.business_name)}-{lead.id}"
            demo = self.demo_builder.build_for_lead(slug, lead.business_name, "prestador de servico", lead.address)
            self.store.set_preview_and_payment(lead.id, demo.preview_url, payment_url)
            self.logger.write("demo_published", {"run_id": run_id, "lead_id": lead.id, "preview_url": demo.preview_url, "file_path": str(demo.file_path)})

            if lead.channel_preferred == "EMAIL" and lead.email and self.email_client and not self.ops.is_channel_paused("EMAIL"):
                pricing = self.store.get_pricing_state()
                unsub = build_unsubscribe_url(self.unsubscribe_base, lead.id, "EMAIL")
                payment_url_full = ""
                payment_url_simple = ""
                if self.stripe_client:
                    c_full = self.stripe_client.create_checkout_session(
                        amount_brl=pricing.price_full,
                        lead_id=lead.id,
                        plan="COMPLETO",
                        business_name=lead.business_name,
                        success_url=self.payment_success_url,
                        cancel_url=self.payment_cancel_url,
                    )
                    c_simple = self.stripe_client.create_checkout_session(
                        amount_brl=pricing.price_simple,
                        lead_id=lead.id,
                        plan="SIMPLES",
                        business_name=lead.business_name,
                        success_url=self.payment_success_url,
                        cancel_url=self.payment_cancel_url,
                    )
                    if c_full.ok:
                        payment_url_full = c_full.url
                    else:
                        self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "EMAIL", "reason": "stripe_checkout_full_failed", "detail": c_full.detail})
                    if c_simple.ok:
                        payment_url_simple = c_simple.url
                    else:
                        self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "EMAIL", "reason": "stripe_checkout_simple_failed", "detail": c_simple.detail})
                subject, body_text, html = offer_email(
                    lead.business_name,
                    demo.preview_url,
                    payment_url,
                    unsub,
                    price_full=pricing.price_full,
                    price_simple=pricing.price_simple,
                    payment_url_full=payment_url_full,
                    payment_url_simple=payment_url_simple,
                )
                result = self.email_client.send(lead.email, subject, html)
                self.store.save_touch(lead.id, "EMAIL", "OFFER", "email_offer_v1", result.status, result.message_id, body_text)
                self.ops.add_channel_metrics("EMAIL", sent=1, failed=0 if result.ok else 1)
                if result.ok:
                    pricing_eval = self.store.record_offer_snapshot(lead.id, run_id=run_id)
                    self.store.update_stage(lead.id, "PAYMENT_SENT")
                    self.logger.write(
                        "offer_sent",
                        {
                            "run_id": run_id,
                            "lead_id": lead.id,
                            "channel": "EMAIL",
                            "preview_url": demo.preview_url,
                            "price_level": pricing.price_level,
                            "price_full": pricing.price_full,
                            "price_simple": pricing.price_simple,
                            "offers_in_window": pricing_eval.get("offers_in_window", 0),
                            "sales_in_window": pricing_eval.get("sales_in_window", 0),
                        },
                    )
                    if pricing_eval.get("window_closed"):
                        self.logger.write(
                            "pricing_window_closed",
                            {
                                "run_id": run_id,
                                "price_level": pricing.price_level,
                                "window_conversion": pricing_eval.get("window_conversion", 0),
                            },
                        )
                    for evt in pricing_eval.get("events", []):
                        self.logger.write(evt["event"], {"run_id": run_id, **{k: v for k, v in evt.items() if k != "event"}})
                    sent_count += 1
                else:
                    self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "EMAIL", "detail": result.detail})

            elif lead.channel_preferred == "WHATSAPP" and lead.phone and self.wa_client and not self.ops.is_channel_paused("WHATSAPP"):
                if wa_metrics.sent >= self.wa_daily_limit:
                    self.logger.write(
                        "deliverability_alert",
                        {
                            "run_id": run_id,
                            "channel": "WHATSAPP",
                            "daily_sent": wa_metrics.sent,
                            "daily_limit": self.wa_daily_limit,
                        },
                    )
                    break
                phone = normalize_phone_br(lead.phone)
                if phone:
                    body = offer_whatsapp(lead.business_name, demo.preview_url, payment_url)
                    result = self.wa_client.send(phone, body)
                    self.store.save_touch(lead.id, "WHATSAPP", "OFFER", "wa_offer_v1", result.status, result.message_id, body)
                    self.ops.add_channel_metrics("WHATSAPP", sent=1, failed=0 if result.ok else 1)
                    wa_metrics = self.ops.get_channel_metrics("WHATSAPP")
                    if result.ok:
                        self.store.update_stage(lead.id, "PAYMENT_SENT")
                        self.logger.write("offer_sent", {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "preview_url": demo.preview_url})
                        sent_count += 1
                    else:
                        self.logger.write("contact_failed", {"run_id": run_id, "lead_id": lead.id, "channel": "WHATSAPP", "detail": result.detail})
            random_human_delay()

        self._evaluate_email_health(run_id)
        self._evaluate_whatsapp_health(run_id)
        self._evaluate_global_safe_mode(run_id)
        return sent_count

    def mark_sale(self, run_id: str, lead_id: int, accepted_plan: str, reason: str) -> dict:
        info = self.store.mark_sale(
            lead_id=lead_id,
            run_id=run_id,
            reason=reason,
            accepted_plan=accepted_plan,
        )
        self.logger.write(
            "pricing_level_up",
            {
                "run_id": run_id,
                "lead_id": lead_id,
                "from_level": info["old_level"],
                "to_level": info["new_level"],
                "accepted_plan": info["accepted_plan"],
                "sale_amount": info["sale_amount"],
            },
        )
        self.logger.write(
            "domain_job_created",
            {
                "run_id": run_id,
                "lead_id": lead_id,
                "status": "DOMAIN_SELECTED",
            },
        )
        return info

    def close_stale_sequences(self, run_id: str) -> int:
        lost_ids = self.store.close_expired_sequences(max_days=self.close_days)
        for lead_id in lost_ids:
            self.logger.write(
                "lead_closed_lost",
                {"run_id": run_id, "lead_id": lead_id, "reason": f"no_close_within_{self.close_days}d"},
            )
        return len(lost_ids)

    def _emit_domain_expiry_alerts(self, run_id: str) -> None:
        alerts = self.store.list_domain_alert_candidates([30, 15, 7])
        for alert in alerts:
            self.logger.write(
                "domain_expiry_alert",
                {
                    "run_id": run_id,
                    "domain_job_id": alert["job_id"],
                    "lead_id": alert["lead_id"],
                    "domain_name": alert["domain_name"],
                    "days_left": alert["days_left"],
                },
            )
            self.store.mark_domain_alert_sent(alert["job_id"], alert["days_left"])

    def register_email_feedback(self, bounces: int, complaints: int, sent: int) -> None:
        self.ops.add_channel_metrics("EMAIL", sent=sent, bounces=bounces, complaints=complaints)

    def _evaluate_email_health(self, run_id: str) -> None:
        metrics = self.ops.get_channel_metrics("EMAIL")
        pause, reason = should_pause_email(metrics.bounce_rate, metrics.complaint_rate, self.thresholds)
        if pause:
            self.ops.set_channel_paused("EMAIL", reason, cooldown_hours=12)
            self.logger.write(
                "channel_paused",
                {
                    "run_id": run_id,
                    "channel": "EMAIL",
                    "reason": reason,
                    "daily_sent": metrics.sent,
                    "bounce_rate": metrics.bounce_rate,
                    "complaint_rate": metrics.complaint_rate,
                },
            )
            self.logger.write("deliverability_alert", {"run_id": run_id, "channel": "EMAIL", "reason": reason})

    def _evaluate_whatsapp_health(self, run_id: str) -> None:
        if self.email_only:
            return
        metrics = self.ops.get_channel_metrics("WHATSAPP")
        pause, reason = should_pause_whatsapp(metrics.fail_rate, self.thresholds)
        if pause:
            self.ops.set_channel_paused("WHATSAPP", reason, cooldown_hours=12)
            self.logger.write(
                "channel_paused",
                {
                    "run_id": run_id,
                    "channel": "WHATSAPP",
                    "reason": reason,
                    "daily_sent": metrics.sent,
                    "wa_fail_rate": metrics.fail_rate,
                },
            )

    def _evaluate_global_safe_mode(self, run_id: str) -> None:
        channels = ["EMAIL", "SCRAPE"] if self.email_only else ["EMAIL", "WHATSAPP", "SCRAPE"]
        paused_count = self.ops.count_paused_channels(channels)
        should_enable = should_enable_global_safe_mode(paused_count, self.thresholds)
        current = self.ops.global_safe_mode_enabled()
        if should_enable and not current:
            self.ops.set_global_safe_mode(True)
            self.logger.write("safe_mode_enabled", {"run_id": run_id, "paused_channels": paused_count})
        if (not should_enable) and current:
            self.ops.set_global_safe_mode(False)
            self.logger.write("safe_mode_disabled", {"run_id": run_id, "paused_channels": paused_count})

    def _campaign_day_index(self) -> int:
        # Uses first run timestamp from ops_state run_history as campaign start.
        db = self.cfg.ops_state_db
        if not db.exists():
            return 1
        import sqlite3

        with sqlite3.connect(db) as conn:
            row = conn.execute("SELECT timestamp_utc FROM run_history ORDER BY id ASC LIMIT 1").fetchone()
        if not row:
            return 1
        first = datetime.fromisoformat(str(row[0]))
        now = datetime.now(UTC)
        delta = now.date() - first.date()
        return max(1, delta.days + 1)

    def _register_incident(self, run_id: str, error_type: str, message: str, context: dict[str, str]) -> None:
        merged_context = {"run_id": run_id, **context}
        fingerprint = self.incident_engine.fingerprint(
            error_type=error_type,
            message=message,
            stack="pipeline_runner.ingest",
            context=merged_context,
        )
        state = self.incident_engine.register(
            fingerprint=fingerprint,
            error_type=error_type,
            message=message,
        )
        self.logger.write(
            "incident_registered",
            {
                "run_id": run_id,
                "fingerprint": fingerprint,
                "level": state.level,
                "count_window": state.count_window,
                "error_type": error_type,
                "message": message[:200],
            },
        )
        if state.should_generate_report:
            report = self.incident_engine.write_report(
                state=state,
                error_type=error_type,
                message=message,
                context=merged_context,
                attempts=["ingest run", "selector fallback", "auto pause channel"],
                impact="Ingestao de leads degradada ou interrompida.",
                hypothesis="Mudanca de layout no Google Maps, latencia de rede, ou anti-bot temporario.",
                next_steps=[
                    "Reduzir volume e aumentar delay de scraping",
                    "Revalidar seletores e fallback de busca",
                    "Executar nova janela apos cooldown",
                ],
                status="open",
            )
            self.logger.write(
                "incident_report_generated",
                {"run_id": run_id, "fingerprint": fingerprint, "level": state.level, "report_path": str(report)},
            )
