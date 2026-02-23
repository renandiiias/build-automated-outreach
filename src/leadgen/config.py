from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class IncidentPolicy:
    window_min: int = int(os.getenv("LEADGEN_INCIDENT_WINDOW_MIN", "15"))
    reset_min: int = int(os.getenv("LEADGEN_INCIDENT_RESET_MIN", "30"))
    l1: int = int(os.getenv("LEADGEN_INCIDENT_L1", "3"))
    l2: int = int(os.getenv("LEADGEN_INCIDENT_L2", "5"))
    l3: int = int(os.getenv("LEADGEN_INCIDENT_L3", "8"))


@dataclass(frozen=True)
class AppConfig:
    log_dir: Path = Path(os.getenv("LEADGEN_LOG_DIR", "./logs"))
    output_dir: Path = Path(os.getenv("LEADGEN_OUTPUT_DIR", "./output"))
    timezone: str = os.getenv("LEADGEN_TIMEZONE", "UTC")
    incident: IncidentPolicy = IncidentPolicy()


def get_config() -> AppConfig:
    cfg = AppConfig()
    cfg.log_dir.mkdir(parents=True, exist_ok=True)
    cfg.output_dir.mkdir(parents=True, exist_ok=True)
    (cfg.log_dir / "incidents").mkdir(parents=True, exist_ok=True)
    return cfg
