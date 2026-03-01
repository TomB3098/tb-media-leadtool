import unittest

from tb_leads.collectors.public_nominatim import collect_nominatim_public
from tb_leads.utils.http import HttpClient
from tb_leads.utils.retry import RetryPolicy
from tb_leads.utils.throttle import RateLimiter


class NominatimCollectorTests(unittest.TestCase):
    def test_collect_nominatim_public(self):
        http_client = HttpClient(
            timeout_s=2,
            rate_limiter=RateLimiter(max_requests_per_minute=1000),
            retry_policy=RetryPolicy(max_attempts=2, base_delay_s=0.01, max_delay_s=0.05, jitter_s=0),
        )

        original_get = http_client.get_json
        try:
            def fake_get(url, headers=None):
                return [
                    {
                        "place_id": 123,
                        "display_name": "Firma C, Krefeld",
                        "namedetails": {"name": "Firma C"},
                        "extratags": {
                            "website": "https://firma-c.de",
                            "email": "kontakt@firma-c.de",
                            "phone": "+49 2151 777777",
                        },
                        "address": {
                            "road": "Hafenstraße",
                            "house_number": "2",
                            "postcode": "47799",
                            "city": "Krefeld",
                        },
                    }
                ]

            http_client.get_json = fake_get  # type: ignore[method-assign]

            leads = collect_nominatim_public(
                region="Krefeld",
                industry="Dienstleister",
                limit=5,
                http_client=http_client,
            )
            self.assertEqual(len(leads), 1)
            self.assertEqual(leads[0]["source_primary"], "nominatim_public")
            self.assertEqual(leads[0]["email"], "kontakt@firma-c.de")
            self.assertIn("47799", leads[0]["address"])
        finally:
            http_client.get_json = original_get  # type: ignore[method-assign]


if __name__ == "__main__":
    unittest.main()
