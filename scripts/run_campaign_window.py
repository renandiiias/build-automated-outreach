#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import random
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
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
    parser = argparse.ArgumentParser(description="Roda campanha por janela de tempo com ciclos automáticos.")
    parser.add_argument("--audience", required=True, help="Ex: manutencao de ar condicionado")
    parser.add_argument("--location", required=True, help="Ex: Belo Horizonte MG")
    parser.add_argument(
        "--fallback-locations",
        default="",
        help="Lista separada por virgula de localidades de fallback (ex: Fortaleza CE,Recife PE)",
    )
    parser.add_argument("--minutes", type=int, default=30, help="Duracao da janela da campanha")
    parser.add_argument("--max-results", type=int, default=60, help="Maximo de leads por ingestao")
    parser.add_argument("--headful", action="store_true", help="Executa navegador visivel")
    parser.add_argument("--enrich-website", action="store_true", help="Tenta enriquecer website para contatos")
    parser.add_argument(
        "--payment-url",
        default=os.getenv("LEADGEN_PAYMENT_URL", ""),
        help="URL de pagamento para envio da oferta",
    )
    parser.add_argument(
        "--cycle-sleep-min",
        type=int,
        default=45,
        help="Pausa minima entre ciclos (segundos)",
    )
    parser.add_argument(
        "--cycle-sleep-max",
        type=int,
        default=90,
        help="Pausa maxima entre ciclos (segundos)",
    )
    parser.add_argument(
        "--force-resume-scrape",
        action="store_true",
        help="Forca retomada do canal SCRAPE no inicio da janela",
    )
    parser.add_argument(
        "--disable-audience-variants",
        action="store_true",
        help="Desativa rotacao automatica de variacoes de busca do publico",
    )
    parser.add_argument(
        "--location-switch-streak",
        type=int,
        default=2,
        help="Qtd de ciclos seguidos sem ingestao para trocar para proxima localidade",
    )
    parser.add_argument(
        "--ingest-timeout-seconds",
        type=int,
        default=480,
        help="Tempo maximo para etapa de ingestao por ciclo; ao exceder, reinicia o processo",
    )
    parser.add_argument(
        "--outreach-timeout-seconds",
        type=int,
        default=240,
        help="Tempo maximo para cada etapa de outreach por ciclo; ao exceder, reinicia o processo",
    )
    return parser


def build_audience_variants(audience: str) -> list[str]:
    base = (audience or "").strip()
    variants: list[str] = [base]
    lowered = base.lower()
    replacements = [
        ("manutencao", "conserto"),
        ("manutenção", "conserto"),
        ("manutencao", "limpeza"),
        ("manutenção", "limpeza"),
    ]
    for old, new in replacements:
        if old in lowered:
            variants.append(lowered.replace(old, new))
    if "ar condicionado" in lowered:
        variants.extend(
            [
                "conserto de ar condicionado",
                "limpeza de ar condicionado",
                "refrigeracao residencial",
            ]
        )
    dedup: list[str] = []
    seen: set[str] = set()
    for item in variants:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        dedup.append(item.strip())
    return dedup or [base]


class _StepTimeoutError(TimeoutError):
    pass


def _run_with_timeout(seconds: int, fn, *args, **kwargs):
    if seconds <= 0:
        return fn(*args, **kwargs)

    def _handler(_signum, _frame):
        raise _StepTimeoutError("step_timeout")

    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _handler)
    signal.alarm(seconds)
    try:
        return fn(*args, **kwargs)
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def _run_ingest_subprocess(
    *,
    audience: str,
    location: str,
    max_results: int,
    headful: bool,
    enrich_website: bool,
    timeout_seconds: int,
) -> int:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_pipeline.py"),
        "ingest",
        "--audience",
        audience,
        "--location",
        location,
        "--max-results",
        str(max_results),
    ]
    if headful:
        cmd.append("--headful")
    if enrich_website:
        cmd.append("--enrich-website")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=max(1, timeout_seconds),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise _StepTimeoutError("ingest_subprocess_timeout") from exc
    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        raise RuntimeError(f"ingest_subprocess_failed rc={proc.returncode} stderr={stderr} stdout={stdout}")
    output = f"{proc.stdout or ''}\n{proc.stderr or ''}"
    match = re.search(r"ingested=(\d+)", output)
    if not match:
        return 0
    return int(match.group(1))


def main() -> int:
    load_dotenv()
    args = build_parser().parse_args()

    from leadgen.pipeline_runner import LeadPipelineRunner

    runner = LeadPipelineRunner()
    run_id = datetime.now(timezone.utc).strftime("window-%Y%m%dT%H%M%SZ")
    deadline = datetime.now(timezone.utc) + timedelta(minutes=args.minutes)
    if args.force_resume_scrape:
        runner.ops.set_channel_resumed("SCRAPE")
        runner.logger.write("channel_resumed", {"run_id": run_id, "channel": "SCRAPE", "reason": "manual_force_resume"})
    runner.logger.write(
        "campaign_window_started",
        {
            "run_id": run_id,
            "audience": args.audience,
            "location": args.location,
            "minutes": args.minutes,
            "max_results": args.max_results,
        },
    )

    cycle = 0
    totals = {"ingested": 0, "consent_sent": 0, "followups_sent": 0, "offers_sent": 0}
    audience_variants = [args.audience]
    if not args.disable_audience_variants:
        audience_variants = build_audience_variants(args.audience)
    locations = [args.location] + [x.strip() for x in args.fallback_locations.split(",") if x.strip()]
    location_idx = 0
    no_ingest_streak = 0

    while datetime.now(timezone.utc) < deadline:
        cycle += 1
        cycle_id = f"{run_id}-c{cycle:03d}"
        audience_now = audience_variants[(cycle - 1) % len(audience_variants)]
        location_now = locations[location_idx]
        runner.logger.write(
            "campaign_cycle_started",
            {
                "run_id": run_id,
                "cycle_id": cycle_id,
                "cycle": cycle,
                "audience_variant": audience_now,
                "location_variant": location_now,
            },
        )
        runner.logger.write("campaign_step_started", {"run_id": run_id, "cycle_id": cycle_id, "step": "ingest"})
        try:
            ingested = _run_ingest_subprocess(
                audience=audience_now,
                location=location_now,
                max_results=args.max_results,
                headful=args.headful,
                enrich_website=args.enrich_website,
                timeout_seconds=args.ingest_timeout_seconds,
            )
        except _StepTimeoutError:
            runner.logger.write(
                "campaign_step_timeout",
                {
                    "run_id": run_id,
                    "cycle_id": cycle_id,
                    "step": "ingest",
                    "timeout_seconds": args.ingest_timeout_seconds,
                },
            )
            raise SystemExit(75)
        runner.logger.write("campaign_step_finished", {"run_id": run_id, "cycle_id": cycle_id, "step": "ingest", "ingested": ingested})

        runner.logger.write("campaign_step_started", {"run_id": run_id, "cycle_id": cycle_id, "step": "send_initial_outreach"})
        try:
            consent = _run_with_timeout(args.outreach_timeout_seconds, runner.send_initial_outreach, run_id=cycle_id)
        except _StepTimeoutError:
            runner.logger.write(
                "campaign_step_timeout",
                {
                    "run_id": run_id,
                    "cycle_id": cycle_id,
                    "step": "send_initial_outreach",
                    "timeout_seconds": args.outreach_timeout_seconds,
                },
            )
            raise SystemExit(75)
        runner.logger.write("campaign_step_finished", {"run_id": run_id, "cycle_id": cycle_id, "step": "send_initial_outreach", "sent": consent})

        runner.logger.write("campaign_step_started", {"run_id": run_id, "cycle_id": cycle_id, "step": "send_followups"})
        try:
            followups = _run_with_timeout(args.outreach_timeout_seconds, runner.send_followups, run_id=cycle_id)
        except _StepTimeoutError:
            runner.logger.write(
                "campaign_step_timeout",
                {
                    "run_id": run_id,
                    "cycle_id": cycle_id,
                    "step": "send_followups",
                    "timeout_seconds": args.outreach_timeout_seconds,
                },
            )
            raise SystemExit(75)
        runner.logger.write("campaign_step_finished", {"run_id": run_id, "cycle_id": cycle_id, "step": "send_followups", "sent": followups})
        offers = 0
        if args.payment_url:
            runner.logger.write("campaign_step_started", {"run_id": run_id, "cycle_id": cycle_id, "step": "send_offers_for_consented"})
            try:
                offers = _run_with_timeout(
                    args.outreach_timeout_seconds,
                    runner.send_offers_for_consented,
                    run_id=cycle_id,
                    payment_url=args.payment_url,
                )
            except _StepTimeoutError:
                runner.logger.write(
                    "campaign_step_timeout",
                    {
                        "run_id": run_id,
                        "cycle_id": cycle_id,
                        "step": "send_offers_for_consented",
                        "timeout_seconds": args.outreach_timeout_seconds,
                    },
                )
                raise SystemExit(75)
            runner.logger.write("campaign_step_finished", {"run_id": run_id, "cycle_id": cycle_id, "step": "send_offers_for_consented", "sent": offers})

        totals["ingested"] += ingested
        totals["consent_sent"] += consent
        totals["followups_sent"] += followups
        totals["offers_sent"] += offers

        runner.logger.write(
            "campaign_cycle_finished",
            {
                "run_id": run_id,
                "cycle_id": cycle_id,
                "cycle": cycle,
                "audience_variant": audience_now,
                "location_variant": location_now,
                "ingested": ingested,
                "consent_sent": consent,
                "followups_sent": followups,
                "offers_sent": offers,
                "deadline_utc": deadline.isoformat(),
            },
        )
        if ingested <= 0:
            no_ingest_streak += 1
            if no_ingest_streak >= max(1, args.location_switch_streak) and location_idx < (len(locations) - 1):
                prev = locations[location_idx]
                location_idx += 1
                no_ingest_streak = 0
                runner.logger.write(
                    "campaign_location_switched",
                    {
                        "run_id": run_id,
                        "from_location": prev,
                        "to_location": locations[location_idx],
                        "reason": "no_ingestion_streak",
                    },
                )
        else:
            no_ingest_streak = 0
        now = datetime.now(timezone.utc)
        if now >= deadline:
            break
        sleep_for = random.randint(args.cycle_sleep_min, args.cycle_sleep_max)
        remaining = int((deadline - now).total_seconds())
        time.sleep(max(1, min(sleep_for, remaining)))

    runner.logger.write("campaign_window_finished", {"run_id": run_id, **totals})
    print(
        f"run_id={run_id} total_ingested={totals['ingested']} total_consent={totals['consent_sent']} "
        f"total_followups={totals['followups_sent']} total_offers={totals['offers_sent']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
