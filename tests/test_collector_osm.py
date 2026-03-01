import unittest

from tb_leads.collectors.public_osm import collect_osm_public
from tb_leads.utils.http import HttpClient
from tb_leads.utils.retry import RetryPolicy
from tb_leads.utils.throttle import RateLimiter


class OsmCollectorTests(unittest.TestCase):
    def test_collect_osm_public(self):
        http_client = HttpClient(
            timeout_s=2,
            rate_limiter=RateLimiter(max_requests_per_minute=1000),
            retry_policy=RetryPolicy(max_attempts=2, base_delay_s=0.01, max_delay_s=0.05, jitter_s=0),
        )

        # monkeypatch endpoint calls to avoid external dependency
        from tb_leads.collectors import public_osm as mod

        original_center = mod._get_region_center
        original_get = http_client.get_json
        try:
            def fake_center(region: str, http_client: HttpClient):
                return 51.333, 6.566

            def fake_get(url, headers=None):
                return {
                    "elements": [
                        {
                            "type": "node",
                            "id": 1,
                            "tags": {
                                "name": "Firma A",
                                "website": "https://firma-a.de",
                                "contact:email": "info@firma-a.de",
                                "addr:street": "Musterstraße",
                                "addr:housenumber": "1",
                                "addr:postcode": "47798",
                                "addr:city": "Krefeld",
                            },
                        }
                    ]
                }

            mod._get_region_center = fake_center
            http_client.get_json = fake_get  # type: ignore[method-assign]

            leads = collect_osm_public(
                region="Krefeld",
                industry="Dienstleister",
                limit=5,
                http_client=http_client,
                radius_km=20,
            )
            self.assertEqual(len(leads), 1)
            self.assertEqual(leads[0]["source_primary"], "osm_overpass_public")
            self.assertEqual(leads[0]["email"], "info@firma-a.de")
            self.assertIn("47798", leads[0]["address"])
        finally:
            mod._get_region_center = original_center
            http_client.get_json = original_get  # type: ignore[method-assign]


if __name__ == "__main__":
    unittest.main()
