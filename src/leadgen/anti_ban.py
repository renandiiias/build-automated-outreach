from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class AntiBanThresholds:
    scrape_error_streak_pause: int = 3
    email_bounce_pause_rate: float = 0.05
    email_complaint_pause_rate: float = 0.003
    whatsapp_fail_pause_rate: float = 0.10
    global_safe_mode_min_paused_channels: int = 2


def email_warmup_daily_limit(day_index: int) -> int:
    # Optional override for operations that need a fixed daily floor/cap.
    floor = int(os.getenv("LEADGEN_EMAIL_DAILY_FLOOR", "0") or "0")
    fixed = int(os.getenv("LEADGEN_EMAIL_DAILY_LIMIT", "0") or "0")

    if fixed > 0:
        return max(fixed, floor)

    if day_index <= 3:
        base = 30
    elif day_index <= 7:
        base = 60
    else:
        extra_weeks = max(0, (day_index - 8) // 7)
        base = 80 + (extra_weeks * 20)
    return max(base, floor)


def should_pause_email(bounce_rate: float, complaint_rate: float, cfg: AntiBanThresholds) -> tuple[bool, str]:
    if complaint_rate > cfg.email_complaint_pause_rate:
        return True, "complaint_rate"
    if bounce_rate > cfg.email_bounce_pause_rate:
        return True, "bounce_rate"
    return False, ""


def should_pause_whatsapp(fail_rate: float, cfg: AntiBanThresholds) -> tuple[bool, str]:
    if fail_rate > cfg.whatsapp_fail_pause_rate:
        return True, "wa_fail_rate"
    return False, ""


def should_pause_scrape(consecutive_errors: int, cfg: AntiBanThresholds) -> bool:
    return consecutive_errors >= cfg.scrape_error_streak_pause


def should_enable_global_safe_mode(paused_channels_today: int, cfg: AntiBanThresholds) -> bool:
    return paused_channels_today >= cfg.global_safe_mode_min_paused_channels
