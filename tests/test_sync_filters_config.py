import tempfile
import unittest

from tb_leads.cli.main import _sync_records  # type: ignore
from tb_leads.cli.main import RunCounters
from tb_leads.db.repository import Repository
from tb_leads.db.schema import init_db
from tb_leads.utils.http import HttpClient
from tb_leads.utils.retry import RetryPolicy
from tb_leads.utils.throttle import RateLimiter


class SyncFilterConfigTests(unittest.TestCase):
    def test_min_score_and_require_email_filters(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = f"{td}/sync.db"
            init_db(db_path)
            repo = Repository(db_path)

            run_id = repo.create_run("Krefeld", "Dienstleister", 10)

            # Lead 1: high score + email => should pass filters
            c1 = repo.upsert_company(
                {
                    "name": "Lead A",
                    "industry": "Dienstleister",
                    "city": "Krefeld",
                    "website_url": "https://a.example",
                    "email": "contact@lead-a.de",
                    "source_primary": "seed_public_demo",
                }
            )
            repo.insert_source_record(c1, run_id, "seed_public_demo", "seed:1", {"name": "Lead A"})
            repo.insert_lead_score(c1, run_id, score_total=82, score_class="A", breakdown={"x": 1}, priority_rank=1)

            # Lead 2: high score but no email => filtered out by require_email_for_sync
            c2 = repo.upsert_company(
                {
                    "name": "Lead B",
                    "industry": "Dienstleister",
                    "city": "Krefeld",
                    "website_url": "https://b.example",
                    "source_primary": "seed_public_demo",
                }
            )
            repo.insert_source_record(c2, run_id, "seed_public_demo", "seed:2", {"name": "Lead B"})
            repo.insert_lead_score(c2, run_id, score_total=85, score_class="A", breakdown={"x": 1}, priority_rank=2)

            # Lead 3: with email but below min_score => filtered out by min_score_for_sync
            c3 = repo.upsert_company(
                {
                    "name": "Lead C",
                    "industry": "Dienstleister",
                    "city": "Krefeld",
                    "website_url": "https://c.example",
                    "email": "contact@lead-c.de",
                    "source_primary": "seed_public_demo",
                }
            )
            repo.insert_source_record(c3, run_id, "seed_public_demo", "seed:3", {"name": "Lead C"})
            repo.insert_lead_score(c3, run_id, score_total=61, score_class="B", breakdown={"x": 1}, priority_rank=3)

            cfg = {
                "min_score_for_sync": 80,
                "filters": {
                    "require_website_for_sync": False,
                    "require_contact_for_sync": False,
                    "require_email_for_sync": True,
                },
                # no notion creds => selected leads become skipped
                "notion_token": None,
                "notion_db_id": None,
                "notion": {"api_base_url": "https://api.notion.com/v1"},
            }

            counters = RunCounters()
            http_client = HttpClient(
                timeout_s=2,
                rate_limiter=RateLimiter(max_requests_per_minute=1000),
                retry_policy=RetryPolicy(max_attempts=2, base_delay_s=0.01, max_delay_s=0.05, jitter_s=0),
            )
            result = _sync_records(
                run_id=run_id,
                min_class=None,
                min_score=None,
                cfg=cfg,
                repo=repo,
                counters=counters,
                http_client=http_client,
            )

            counts = result["counts"]
            self.assertEqual(counts["skipped"], 1)
            self.assertEqual(counts["success"], 0)
            self.assertEqual(counts["failed"], 0)


if __name__ == "__main__":
    unittest.main()
