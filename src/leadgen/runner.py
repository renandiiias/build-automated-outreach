from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from .config import get_config
from .exporters import export_csv, export_xlsx
from .incident import IncidentEngine
from .logging_utils import JsonlLogger
from .scraper import GoogleMapsScraper, ScrapeRequest


class LeadGeneratorRunner:
    def __init__(self) -> None:
        self.cfg = get_config()
        self.logger = JsonlLogger(self.cfg.log_dir / "events.jsonl")
        self.incident_engine = IncidentEngine(
            db_path=self.cfg.log_dir / "incident_state.db",
            policy=self.cfg.incident,
            incident_dir=self.cfg.log_dir / "incidents",
        )
        self.scraper = GoogleMapsScraper()

    def run(
        self,
        audience: str,
        location: str,
        max_results: int,
        out_format: str,
        headless: bool,
    ) -> list[Path]:
        run_id = datetime.now(UTC).strftime("run-%Y%m%dT%H%M%SZ")
        self.logger.write(
            "run_started",
            {
                "run_id": run_id,
                "audience": audience,
                "location": location,
                "max_results": max_results,
                "format": out_format,
            },
        )

        try:
            rows = self.scraper.scrape(
                ScrapeRequest(
                    audience=audience,
                    location=location,
                    max_results=max_results,
                    headless=headless,
                )
            )

            ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
            stem = f"leads-{audience.replace(' ', '_')}-{location.replace(' ', '_')}-{ts}"
            files: list[Path] = []

            if out_format in {"csv", "both"}:
                files.append(export_csv(rows, self.cfg.output_dir / f"{stem}.csv"))
            if out_format in {"xlsx", "both"}:
                files.append(export_xlsx(rows, self.cfg.output_dir / f"{stem}.xlsx"))

            self.logger.write(
                "run_finished",
                {
                    "run_id": run_id,
                    "rows": len(rows),
                    "files": [str(f) for f in files],
                },
            )
            return files
        except Exception as exc:
            message = str(exc)
            error_type = exc.__class__.__name__
            context = {
                "run_id": run_id,
                "audience": audience,
                "location": location,
                "max_results": max_results,
            }
            fingerprint = self.incident_engine.fingerprint(
                error_type=error_type,
                message=message,
                stack=repr(exc),
                context=context,
            )
            state = self.incident_engine.register(
                fingerprint=fingerprint,
                error_type=error_type,
                message=message,
            )
            self.logger.write(
                "run_failed",
                {
                    "run_id": run_id,
                    "fingerprint": fingerprint,
                    "level": state.level,
                    "count_window": state.count_window,
                    "error_type": error_type,
                    "message": message,
                    "context": context,
                },
            )

            if state.should_generate_report:
                report = self.incident_engine.write_report(
                    state=state,
                    error_type=error_type,
                    message=message,
                    context=context,
                    attempts=["run scraper", "collect maps results", "export sheets"],
                    impact="Falha na geracao de leads para o publico solicitado.",
                    hypothesis="Mudanca de seletor no Google Maps, bloqueio anti-bot ou timeout de rede.",
                    next_steps=[
                        "Reexecutar com max_results menor",
                        "Validar conectividade e IP reputation",
                        "Atualizar seletores no scraper",
                    ],
                    status="open",
                )
                self.logger.write(
                    "incident_report_generated",
                    {
                        "fingerprint": fingerprint,
                        "level": state.level,
                        "report": str(report),
                    },
                )

            raise
