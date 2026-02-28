import tempfile
import unittest

from tb_leads.db.repository import Repository
from tb_leads.db.schema import init_db


class DedupTests(unittest.TestCase):
    def test_upsert_company_deduplicates_by_name_city_domain(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = f"{td}/test.db"
            init_db(db_path)
            repo = Repository(db_path)

            payload = {
                "name": "Praxis am Stadtpark",
                "industry": "Arztpraxen",
                "city": "Krefeld",
                "website_url": "https://example.com",
                "source_primary": "seed_public_demo",
            }

            c1 = repo.upsert_company(payload)
            c2 = repo.upsert_company(payload)
            self.assertEqual(c1, c2)


if __name__ == "__main__":
    unittest.main()
