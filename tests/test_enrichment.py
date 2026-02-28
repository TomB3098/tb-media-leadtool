import unittest

from tb_leads.enrich.contact_enrichment import _extract_addresses, _extract_emails


class EnrichmentParsingTests(unittest.TestCase):
    def test_extract_emails(self):
        text = "Kontakt: info@beispiel.de oder support@example.com"
        emails = _extract_emails(text)
        self.assertIn("info@beispiel.de", emails)
        self.assertNotIn("support@example.com", emails)

    def test_extract_emails_filters_invalid_domain(self):
        text = "Mail: kontakt@firma.invalid"
        emails = _extract_emails(text)
        self.assertEqual(emails, [])

    def test_extract_address(self):
        text = """
        Musterfirma GmbH
        Hauptstraße 12
        47798 Krefeld
        """
        addresses = _extract_addresses(text)
        self.assertTrue(any("47798 Krefeld" in a for a in addresses))


if __name__ == "__main__":
    unittest.main()
