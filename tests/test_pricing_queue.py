from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from leadgen.crm_store import CrmStore
from leadgen.time_utils import UTC


class PricingEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "pipeline.db"
        self.store = CrmStore(self.db)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _new_lead(self, idx: int = 1) -> int:
        return self.store.upsert_lead_from_row(
            run_id="test-run",
            row={
                "name": f"Lead {idx}",
                "phone": "",
                "website_emails": f"lead{idx}@example.com",
                "website": "",
                "maps_url": f"https://maps.google.com/?cid={idx}",
                "address": "Belo Horizonte MG",
            },
        )

    def test_price_level_up_after_sale(self) -> None:
        lead_id = self._new_lead(1)
        self.store.record_offer_snapshot(lead_id=lead_id, run_id="run-1")
        info = self.store.mark_sale(lead_id=lead_id, run_id="run-1", reason="test", accepted_plan="COMPLETO")
        st = self.store.get_pricing_state()
        self.assertEqual(info["new_level"], 1)
        self.assertEqual(st.price_level, 1)
        self.assertEqual(st.price_full, 300)
        self.assertEqual(st.price_simple, 200)

    def test_price_level_down_after_ten_offers_without_sale(self) -> None:
        lead_id = self._new_lead(2)
        # First sale moves to level 1.
        self.store.record_offer_snapshot(lead_id=lead_id, run_id="run-up")
        self.store.mark_sale(lead_id=lead_id, run_id="run-up", reason="seed", accepted_plan="COMPLETO")
        st = self.store.get_pricing_state()
        self.assertEqual(st.price_level, 1)

        # Ten offers without sale should bring it down one level.
        for i in range(10):
            out = self.store.record_offer_snapshot(lead_id=lead_id, run_id=f"run-window-{i}")
        st2 = self.store.get_pricing_state()
        self.assertTrue(out["window_closed"])
        self.assertEqual(st2.price_level, 0)
        self.assertEqual(st2.price_full, 200)
        self.assertEqual(st2.price_simple, 100)

    def test_close_expired_sequences_marks_lost(self) -> None:
        lead_id = self._new_lead(3)
        self.store.save_touch(
            lead_id=lead_id,
            channel="EMAIL",
            intent="CONSENT_REQUEST",
            template_id="email_v1",
            status="sent",
            provider_message_id="m1",
            body="body",
        )
        self.store.update_stage(lead_id, "WAITING_REPLY")
        # Force touch timestamp to 8 days ago.
        old_ts = (datetime.now(UTC) - timedelta(days=8)).isoformat()
        import sqlite3

        with sqlite3.connect(self.db) as conn:
            conn.execute("UPDATE touches SET timestamp_utc=? WHERE lead_id=?", (old_ts, lead_id))
            conn.commit()
        lost_ids = self.store.close_expired_sequences(max_days=7)
        self.assertIn(lead_id, lost_ids)
        ctx = self.store.get_lead_sale_context(lead_id)
        self.assertEqual(ctx["stage"], "LOST")


class ReplyQueueTests(unittest.TestCase):
    def test_queue_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "pipeline.db"
            store = CrmStore(db)
            lead_id = store.upsert_lead_from_row(
                run_id="test-run",
                row={
                    "name": "Lead Q",
                    "phone": "",
                    "website_emails": "q@example.com",
                    "website": "",
                    "maps_url": "https://maps.google.com/?cid=q",
                    "address": "Fortaleza CE",
                },
            )
            qid = store.enqueue_reply_review(lead_id=lead_id, channel="EMAIL", inbound_text="quero fechar")
            item = store.get_reply_review_item(qid)
            self.assertIsNotNone(item)
            self.assertEqual(item.status, "PENDING")
            store.set_reply_codex_decision(qid, intent_final="positive_offer_accept", draft_reply="Perfeito", confidence=0.91, status="CODEX_DONE")
            item2 = store.get_reply_review_item(qid)
            self.assertEqual(item2.status, "CODEX_DONE")
            store.mark_reply_sent(qid)
            item3 = store.get_reply_review_item(qid)
            self.assertEqual(item3.status, "SENT")


if __name__ == "__main__":
    unittest.main()
