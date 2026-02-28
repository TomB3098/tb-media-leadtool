import csv
import tempfile
import unittest

from tb_leads.reporting.csv_exporter import export_scored_leads


class CsvExportTests(unittest.TestCase):
    def test_export_contains_email_and_address_columns(self):
        leads = [
            {
                "company_id": "c1",
                "name": "Firma A",
                "industry": "Dienstleister",
                "city": "Krefeld",
                "website_url": "https://firma-a.de",
                "email": "info@firma-a.de",
                "phone": "02151-123",
                "address": "Musterstraße 1, 47798 Krefeld",
                "contact_source_url": "https://firma-a.de/impressum",
                "score_total": 85,
                "score_class": "A",
                "priority_rank": 1,
            }
        ]

        with tempfile.TemporaryDirectory() as td:
            path = export_scored_leads(leads, td, "run-1")
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

            self.assertIn("email", reader.fieldnames)
            self.assertIn("address", reader.fieldnames)
            self.assertEqual(rows[0]["email"], "info@firma-a.de")
            self.assertEqual(rows[0]["address"], "Musterstraße 1, 47798 Krefeld")


if __name__ == "__main__":
    unittest.main()
