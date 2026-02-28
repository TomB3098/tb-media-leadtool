import unittest

from tb_leads.scoring.engine import score_lead


class ScoringTests(unittest.TestCase):
    def test_high_quality_lead_is_a(self):
        audit = {
            "website_present": True,
            "mobile_pagespeed_score": 92,
            "seo_score": 88,
            "has_contact_cta": True,
            "has_contact_form": True,
            "tech_health_score": 85,
        }
        result = score_lead(audit)
        self.assertEqual(result["class"], "A")
        self.assertGreaterEqual(result["total"], 80)

    def test_low_quality_lead_is_c(self):
        audit = {
            "website_present": False,
            "mobile_pagespeed_score": 20,
            "seo_score": 10,
            "has_contact_cta": False,
            "has_contact_form": False,
            "tech_health_score": 10,
        }
        result = score_lead(audit)
        self.assertEqual(result["class"], "C")


if __name__ == "__main__":
    unittest.main()
