from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from leadgen.contact_sources import ContactCandidate
from leadgen.crm_store import CrmStore
from leadgen.email_validation import validate_email
from leadgen.enrichment import enrich_with_website_contacts


class EmailValidationTests(unittest.TestCase):
    def test_mx_cache_hit_after_first_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CrmStore(Path(tmp) / "pipeline.db")
            with patch("leadgen.email_validation._resolve_mx", return_value=True):
                a = validate_email("foo@empresa.com", store=store)
                b = validate_email("foo@empresa.com", store=store)
            self.assertEqual(a.validation_status, "valid")
            self.assertFalse(a.mx_cache_hit)
            self.assertTrue(b.mx_cache_hit)

    def test_invalid_mx_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CrmStore(Path(tmp) / "pipeline.db")
            with patch("leadgen.email_validation._resolve_mx", return_value=False):
                out = validate_email("foo@invalid-domain-example.com", store=store)
            self.assertEqual(out.validation_status, "invalid_mx")
            self.assertFalse(out.mx_ok)


class EnrichmentMergeTests(unittest.TestCase):
    def test_merge_and_select_best_candidate(self) -> None:
        rows = [
            {
                "name": "Studio Alpha",
                "website": "https://alpha.test",
                "address": "London, United Kingdom",
                "country_code": "UK",
                "audience": "lawyer",
            }
        ]

        class _Logger:
            def write(self, *_args, **_kwargs):
                return None

        logger = _Logger()
        with tempfile.TemporaryDirectory() as tmp:
            store = CrmStore(Path(tmp) / "pipeline.db")
            with patch("leadgen.enrichment._fetch_website_html") as fetch_html, patch(
                "leadgen.enrichment.fetch_contacts_for_lead"
            ) as fetch_external, patch("leadgen.email_validation._resolve_mx", return_value=True):
                fetch_html.return_value = type("X", (), {"provider": "urllib", "html": "contato@sitealpha.com"})()
                fetch_external.return_value = [
                    ContactCandidate(
                        email="office@lawsociety.org.uk",
                        source_type="council",
                        source_name="law_society",
                        source_url="https://lawsociety.org.uk/x",
                        confidence=0.84,
                    )
                ]
                out = enrich_with_website_contacts(rows, logger, run_id="t-1", store=store)

        self.assertTrue(out)
        self.assertEqual(out[0].get("email"), "office@lawsociety.org.uk")
        self.assertIn("contact_candidates", out[0])
        self.assertGreaterEqual(len(out[0]["contact_candidates"]), 1)

    def test_no_valid_candidate_keeps_email_empty(self) -> None:
        rows = [
            {
                "name": "Studio Beta",
                "website": "",
                "address": "Madrid, Spain",
                "country_code": "ES",
                "audience": "abogado",
            }
        ]

        class _Logger:
            def write(self, *_args, **_kwargs):
                return None

        with tempfile.TemporaryDirectory() as tmp:
            store = CrmStore(Path(tmp) / "pipeline.db")
            with patch("leadgen.enrichment.fetch_contacts_for_lead", return_value=[]):
                out = enrich_with_website_contacts(rows, _Logger(), run_id="t-2", store=store)
        self.assertEqual(out[0].get("email", ""), "")


if __name__ == "__main__":
    unittest.main()
