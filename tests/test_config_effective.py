import os
import unittest

from tb_leads.config.loader import load_config


class ConfigEffectiveTests(unittest.TestCase):
    def test_env_overrides_effective(self):
        os.environ["TB_LEADS_MAX_REQUESTS_PER_MINUTE"] = "77"
        os.environ["TB_LEADS_MAX_ERRORS_PER_RUN"] = "9"
        os.environ["TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN"] = "4"
        try:
            cfg = load_config()
            self.assertEqual(cfg["compliance"]["max_requests_per_minute"], 77)
            self.assertEqual(cfg["run"]["max_errors_per_run"], 9)
            self.assertEqual(cfg["run"]["max_network_errors_per_run"], 4)
        finally:
            os.environ.pop("TB_LEADS_MAX_REQUESTS_PER_MINUTE", None)
            os.environ.pop("TB_LEADS_MAX_ERRORS_PER_RUN", None)
            os.environ.pop("TB_LEADS_MAX_NETWORK_ERRORS_PER_RUN", None)


if __name__ == "__main__":
    unittest.main()
