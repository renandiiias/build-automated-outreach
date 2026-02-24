from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from leadgen.anti_ban import (
    AntiBanThresholds,
    email_warmup_daily_limit,
    should_enable_global_safe_mode,
    should_pause_email,
    should_pause_scrape,
    should_pause_whatsapp,
)
from leadgen.ops_state import OperationalState


class AntiBanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.thresholds = AntiBanThresholds()

    def test_scrape_pauses_on_three_consecutive_errors(self) -> None:
        self.assertTrue(should_pause_scrape(3, self.thresholds))

    def test_email_pauses_on_bounce_rate(self) -> None:
        paused, reason = should_pause_email(0.06, 0.0, self.thresholds)
        self.assertTrue(paused)
        self.assertEqual(reason, "bounce_rate")

    def test_email_pauses_on_complaint_rate(self) -> None:
        paused, reason = should_pause_email(0.0, 0.004, self.thresholds)
        self.assertTrue(paused)
        self.assertEqual(reason, "complaint_rate")

    def test_whatsapp_pauses_on_fail_rate(self) -> None:
        paused, reason = should_pause_whatsapp(0.12, self.thresholds)
        self.assertTrue(paused)
        self.assertEqual(reason, "wa_fail_rate")

    def test_global_safe_mode_enabled_when_two_channels_paused(self) -> None:
        self.assertTrue(should_enable_global_safe_mode(2, self.thresholds))

    def test_email_warmup(self) -> None:
        self.assertEqual(email_warmup_daily_limit(1), 30)
        self.assertEqual(email_warmup_daily_limit(5), 60)
        self.assertGreaterEqual(email_warmup_daily_limit(8), 80)


class OpsStateTests(unittest.TestCase):
    def test_channel_pause_and_safe_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ops.db"
            ops = OperationalState(db)
            ops.set_channel_paused("EMAIL", "bounce_rate", cooldown_hours=12)
            ops.set_channel_paused("WHATSAPP", "wa_fail_rate", cooldown_hours=12)
            paused = ops.count_paused_channels(["EMAIL", "WHATSAPP", "SCRAPE"])
            self.assertEqual(paused, 2)
            ops.set_global_safe_mode(True)
            self.assertTrue(ops.global_safe_mode_enabled())

    def test_count_paused_ignores_expired_cooldown(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ops.db"
            ops = OperationalState(db)
            ops.set_channel_paused("EMAIL", "bounce_rate", cooldown_hours=12)
            # Force cooldown expiration in DB.
            import sqlite3

            with sqlite3.connect(db) as conn:
                conn.execute("UPDATE channel_status SET cooldown_until_utc='1970-01-01T00:00:00+00:00' WHERE channel='EMAIL'")
                conn.commit()
            self.assertFalse(ops.is_channel_paused("EMAIL"))
            self.assertEqual(ops.count_paused_channels(["EMAIL"]), 0)


if __name__ == "__main__":
    unittest.main()
