import os
import unittest

from tb_leads.cli.main import _make_http_client
from tb_leads.config.loader import load_config


class ConfigEffectiveTests(unittest.TestCase):
    def test_env_overrides_effective(self):
        os.environ["TB_LEADS_MAX_REQUESTS_PER_MINUTE"] = "77"
        os.environ["TB_LEADS_MAX_ERRORS_PER_RUN"] = "9"
        os.environ["TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN"] = "4"
        os.environ["TB_LEADS_TIMEOUT_SECONDS"] = "6"
        os.environ["TB_LEADS_MAX_RETRIES"] = "2"
        os.environ["TB_LEADS_ENRICHMENT_MAX_PAGES"] = "2"
        try:
            cfg = load_config()
            self.assertEqual(cfg["compliance"]["max_requests_per_minute"], 77)
            self.assertEqual(cfg["run"]["max_errors_per_run"], 9)
            self.assertEqual(cfg["run"]["max_network_errors_per_run"], 4)
            self.assertEqual(cfg["network"]["timeout_seconds"], 6.0)
            self.assertEqual(cfg["network"]["max_retries"], 2)
            self.assertEqual(cfg["enrichment"]["max_pages"], 2)
        finally:
            os.environ.pop("TB_LEADS_MAX_REQUESTS_PER_MINUTE", None)
            os.environ.pop("TB_LEADS_MAX_ERRORS_PER_RUN", None)
            os.environ.pop("TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN", None)
            os.environ.pop("TB_LEADS_TIMEOUT_SECONDS", None)
            os.environ.pop("TB_LEADS_MAX_RETRIES", None)
            os.environ.pop("TB_LEADS_ENRICHMENT_MAX_PAGES", None)

    def test_http_client_uses_configured_rate_limit(self):
        cfg = {
            "compliance": {"max_requests_per_minute": 42},
            "network": {
                "timeout_seconds": 8,
                "max_retries": 5,
                "backoff_base_seconds": 0.1,
                "backoff_max_seconds": 1.2,
                "jitter_seconds": 0.05,
            },
        }
        client = _make_http_client(cfg)
        self.assertIsNotNone(client.rate_limiter)
        self.assertEqual(client.rate_limiter.max_requests_per_minute, 42)
        self.assertEqual(client.retry_policy.max_attempts, 5)
        self.assertAlmostEqual(client.timeout_s, 8.0)


if __name__ == "__main__":
    unittest.main()
