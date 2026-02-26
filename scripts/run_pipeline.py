#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv() -> None:
        return None

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Lead pipeline com anti-ban, consentimento e fallback WhatsApp")
    sub = parser.add_subparsers(dest="command", required=True)

    run_all = sub.add_parser("run-all", help="Executa ingest + outreach inicial + ofertas para consentidos")
    _add_common_search_args(run_all)
    run_all.add_argument("--payment-url", default="", help="URL de pagamento a ser enviada apos demo")

    ingest = sub.add_parser("ingest", help="Executa apenas scraping/qualificacao")
    _add_common_search_args(ingest)
    ingest.add_argument("--run-id", default="manual", help="Run id para correlacao de logs")

    outreach = sub.add_parser("outreach", help="Executa apenas primeiro contato (consent-first)")
    outreach.add_argument("--run-id", default="manual", help="Run id para correlacao de logs")

    followups = sub.add_parser("followups", help="Executa follow-up de consentimento (maximo 2)")
    followups.add_argument("--run-id", default="manual", help="Run id para correlacao de logs")

    offers = sub.add_parser("offers", help="Publica demo e envia oferta para leads consentidos")
    offers.add_argument("--run-id", default="manual", help="Run id para correlacao de logs")
    offers.add_argument("--payment-url", required=True, help="URL de pagamento")

    reply = sub.add_parser("reply", help="Registra resposta inbound e classifica")
    reply.add_argument("--run-id", default="manual", help="Run id para correlacao de logs")
    reply.add_argument("--lead-id", type=int, required=True, help="ID do lead")
    reply.add_argument("--channel", choices=["EMAIL", "WHATSAPP"], required=True)
    reply.add_argument("--text", required=True, help="Texto da resposta")

    feedback = sub.add_parser("email-feedback", help="Atualiza metricas diarias de bounce/complaint")
    feedback.add_argument("--bounces", type=int, default=0)
    feedback.add_argument("--complaints", type=int, default=0)
    feedback.add_argument("--sent", type=int, default=0)

    sales = sub.add_parser("sales-mark", help="Marca venda manual e sobe nivel de preco")
    sales.add_argument("--lead-id", type=int, required=True)
    sales.add_argument("--run-id", default="manual")
    sales.add_argument("--accepted-plan", choices=["COMPLETO", "SIMPLES"], default="COMPLETO")
    sales.add_argument("--reason", default="manual_sale_mark")

    close = sub.add_parser("close-stale", help="Fecha sequencias estagnadas em LOST pelo prazo configurado")
    close.add_argument("--run-id", default="manual")

    return parser


def _add_common_search_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--audience", required=True, help="Ex: eletricistas")
    parser.add_argument("--location", required=True, help="Ex: Sao Paulo SP")
    parser.add_argument("--max-results", type=int, default=60, help="Padrao anti-ban v1")
    parser.add_argument("--headful", action="store_true", help="Browser visivel")
    parser.add_argument("--enrich-website", action="store_true", help="Tenta achar email/telefone no website")


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()

    from leadgen.pipeline_runner import LeadPipelineRunner

    runner = LeadPipelineRunner()

    if args.command == "run-all":
        summary = runner.run_all(
            audience=args.audience,
            location=args.location,
            max_results=args.max_results,
            headless=not args.headful,
            enrich_website=args.enrich_website,
            payment_url=args.payment_url,
        )
        print(
            f"run_id={summary.run_id} leads_ingested={summary.leads_ingested} "
            f"consent_sent={summary.consent_sent} followups_sent={summary.followups_sent} offers_sent={summary.offers_sent}"
        )
        return 0

    if args.command == "ingest":
        n = runner.ingest(
            run_id=args.run_id,
            audience=args.audience,
            location=args.location,
            max_results=args.max_results,
            headless=not args.headful,
            enrich_website=args.enrich_website,
        )
        print(f"ingested={n}")
        return 0

    if args.command == "outreach":
        sent = runner.send_initial_outreach(run_id=args.run_id)
        print(f"consent_sent={sent}")
        return 0

    if args.command == "followups":
        sent = runner.send_followups(run_id=args.run_id)
        print(f"followups_sent={sent}")
        return 0

    if args.command == "offers":
        sent = runner.send_offers_for_consented(run_id=args.run_id, payment_url=args.payment_url)
        print(f"offers_sent={sent}")
        return 0

    if args.command == "reply":
        runner.process_reply(run_id=args.run_id, lead_id=args.lead_id, channel=args.channel, text=args.text)
        print("reply_processed=1")
        return 0

    if args.command == "email-feedback":
        runner.register_email_feedback(bounces=args.bounces, complaints=args.complaints, sent=args.sent)
        print("email_feedback_recorded=1")
        return 0

    if args.command == "sales-mark":
        info = runner.mark_sale(
            run_id=args.run_id,
            lead_id=args.lead_id,
            accepted_plan=args.accepted_plan,
            reason=args.reason,
        )
        print(
            f"sale_marked=1 lead_id={args.lead_id} accepted_plan={info['accepted_plan']} "
            f"sale_amount={info['sale_amount']} new_level={info['new_level']} "
            f"price_full={info['price_full']} price_simple={info['price_simple']}"
        )
        return 0

    if args.command == "close-stale":
        n = runner.close_stale_sequences(run_id=args.run_id)
        print(f"closed_lost={n}")
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
