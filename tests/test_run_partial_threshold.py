import csv
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from tb_leads.cli.main import main as cli_main


class RunPartialThresholdTests(unittest.TestCase):
    def test_run_becomes_partial_on_network_threshold(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = str(Path(td) / "run.db")
            csv_path = str(Path(td) / "leads.csv")

            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(
                    f,
                    fieldnames=["name", "industry", "city", "postal_code", "address", "website_url", "phone", "source_ref"],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "name": "Threshold Test",
                        "industry": "Dienstleister",
                        "city": "Krefeld",
                        "postal_code": "47798",
                        "address": "",
                        "website_url": "https://example.invalid",
                        "phone": "02151-1",
                        "source_ref": "threshold:test",
                    }
                )

            os.environ["TB_LEADS_DB_PATH"] = db_path
            os.environ["TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN"] = "0"
            try:
                rc = cli_main(
                    [
                        "run",
                        "--region",
                        "Krefeld",
                        "--industry",
                        "Dienstleister",
                        "--limit",
                        "1",
                        "--source",
                        "csv",
                        "--csv-path",
                        csv_path,
                        "--skip-sync",
                        "--out",
                        str(Path(td) / "reports"),
                    ]
                )
                self.assertEqual(rc, 2)

                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT status, network_error_count FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
                conn.close()
                self.assertIsNotNone(row)
                self.assertEqual(row["status"], "partial")
                self.assertGreater(row["network_error_count"], 0)
            finally:
                os.environ.pop("TB_LEADS_DB_PATH", None)
                os.environ.pop("TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN", None)


if __name__ == "__main__":
    unittest.main()
