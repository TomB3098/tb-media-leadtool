import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tb_leads.sync.notion_client import NotionClient
from tb_leads.utils.http import HttpClient
from tb_leads.utils.retry import RetryPolicy
from tb_leads.utils.throttle import RateLimiter


class _NotionHandler(BaseHTTPRequestHandler):
    pages: list[dict] = []
    create_calls = 0

    def _json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/v1/databases/db1":
            return self._json(
                200,
                {
                    "id": "db1",
                    "properties": {
                        "Name": {"type": "title", "title": {}},
                        "Company": {"type": "rich_text", "rich_text": {}},
                        "Contact Email": {"type": "email", "email": {}},
                        "Phone": {"type": "phone_number", "phone_number": {}},
                        "Source": {"type": "select", "select": {"options": [{"name": "Website"}]}},
                        "Priority": {"type": "select", "select": {"options": [{"name": "High"}, {"name": "Medium"}, {"name": "Low"}]}},
                        "Status": {"type": "select", "select": {"options": [{"name": "Active"}]}},
                        "Stage": {"type": "select", "select": {"options": [{"name": "Lead"}]}},
                        "Notes": {"type": "rich_text", "rich_text": {}},
                    },
                },
            )
        return self._json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")

        if self.path == "/v1/databases/db1/query":
            filters = payload.get("filter", {})
            target_title = None
            if "and" in filters:
                for f in filters["and"]:
                    if f.get("property") == "Name":
                        target_title = f.get("title", {}).get("equals")
            elif filters.get("property") == "Name":
                target_title = filters.get("title", {}).get("equals")

            matches = [p for p in type(self).pages if p["title"] == target_title]
            return self._json(200, {"results": [m["raw"] for m in matches]})

        if self.path == "/v1/pages":
            type(self).create_calls += 1
            # first create returns 429 -> should be retried by HttpClient
            if type(self).create_calls == 1:
                return self._json(429, {"error": "rate limited"})

            title = payload["properties"]["Name"]["title"][0]["text"]["content"]
            page_id = f"page-{len(type(self).pages)+1}"
            raw = {"id": page_id, "properties": payload["properties"]}
            type(self).pages.append({"id": page_id, "title": title, "raw": raw})
            return self._json(200, {"id": page_id})

        return self._json(404, {"error": "not found"})

    def do_PATCH(self):  # noqa: N802
        if self.path.startswith("/v1/pages/"):
            page_id = self.path.split("/")[-1]
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            for page in type(self).pages:
                if page["id"] == page_id:
                    page["raw"]["properties"] = payload.get("properties", page["raw"]["properties"])
                    return self._json(200, {"id": page_id})
            return self._json(404, {"error": "page not found"})

        return self._json(404, {"error": "not found"})

    def log_message(self, *_args):
        return


class NotionMockIntegrationTests(unittest.TestCase):
    def setUp(self):
        _NotionHandler.pages = []
        _NotionHandler.create_calls = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _NotionHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}/v1"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_idempotent_upsert_with_retry(self):
        client = NotionClient(
            token="token",
            database_id="db1",
            http_client=HttpClient(
                timeout_s=2,
                rate_limiter=RateLimiter(max_requests_per_minute=1000),
                retry_policy=RetryPolicy(max_attempts=3, base_delay_s=0.01, max_delay_s=0.05, jitter_s=0),
            ),
            api_base_url=self.base,
        )

        lead = {
            "name": "Firma Test",
            "city": "Krefeld",
            "industry": "Dienstleister",
            "score_total": 81,
            "score_class": "A",
            "website_url": "https://firma-test.de",
            "website_domain": "firma-test.de",
            "email": "info@firma-test.de",
            "phone": "02151-123",
            "address": "Musterstraße 1, 47798 Krefeld",
        }

        first = client.upsert_lead(lead)
        self.assertEqual(first["status"], "success")
        self.assertEqual(first["action"], "created")

        second = client.upsert_lead(lead)
        self.assertEqual(second["status"], "success")
        self.assertEqual(second["action"], "updated")
        self.assertEqual(len(_NotionHandler.pages), 1)


if __name__ == "__main__":
    unittest.main()
