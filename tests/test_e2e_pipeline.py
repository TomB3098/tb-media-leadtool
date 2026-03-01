import csv
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tb_leads.cli.main import main as cli_main


class _SiteHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path in ["/", "/index.html", "/impressum", "/kontakt"]:
            body = """
            <html>
              <head>
                <title>E2E Site</title>
                <meta name="description" content="E2E test site for leadtool" />
              </head>
              <body>
                <h1>E2E Lead</h1>
                <p>Kontakt: e2e@firma-krefeld.de</p>
                <p>Musterstraße 12, 47798 Krefeld</p>
                <form><input/></form>
              </body>
            </html>
            """.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, *_args):
        return


class _NotionE2EHandler(BaseHTTPRequestHandler):
    pages: list[dict] = []

    def _json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if self.path == "/v1/databases/db-e2e":
            return self._json(
                200,
                {
                    "id": "db-e2e",
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

        if self.path == "/v1/databases/db-e2e/query":
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
            return self._json(404, {"error": "not found"})
        return self._json(404, {"error": "not found"})

    def log_message(self, *_args):
        return


class E2EPipelineTests(unittest.TestCase):
    def setUp(self):
        _NotionE2EHandler.pages = []

        self.site_server = ThreadingHTTPServer(("127.0.0.1", 0), _SiteHandler)
        self.site_thread = threading.Thread(target=self.site_server.serve_forever, daemon=True)
        self.site_thread.start()

        self.notion_server = ThreadingHTTPServer(("127.0.0.1", 0), _NotionE2EHandler)
        self.notion_thread = threading.Thread(target=self.notion_server.serve_forever, daemon=True)
        self.notion_thread.start()

        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "e2e.db")
        self.report_dir = str(Path(self.tmp.name) / "reports")
        self.csv_path = str(Path(self.tmp.name) / "leads.csv")

        with open(self.csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["name", "industry", "city", "postal_code", "address", "website_url", "phone", "source_ref"],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "name": "E2E Testfirma",
                    "industry": "Dienstleister",
                    "city": "Krefeld",
                    "postal_code": "47798",
                    "address": "",
                    "website_url": f"http://127.0.0.1:{self.site_server.server_port}/index.html",
                    "phone": "02151-999999",
                    "source_ref": "local:e2e",
                }
            )

        os.environ["TB_LEADS_DB_PATH"] = self.db_path
        os.environ["NOTION_TOKEN"] = "token-e2e"
        os.environ["NOTION_DB_ID"] = "db-e2e"
        os.environ["NOTION_API_BASE_URL"] = f"http://127.0.0.1:{self.notion_server.server_port}/v1"
        os.environ["TB_LEADS_MAX_REQUESTS_PER_MINUTE"] = "120"

    def tearDown(self):
        self.site_server.shutdown()
        self.site_server.server_close()
        self.notion_server.shutdown()
        self.notion_server.server_close()
        self.tmp.cleanup()

        for key in [
            "TB_LEADS_DB_PATH",
            "NOTION_TOKEN",
            "NOTION_DB_ID",
            "NOTION_API_BASE_URL",
            "TB_LEADS_MAX_REQUESTS_PER_MINUTE",
        ]:
            os.environ.pop(key, None)

    def test_e2e_run_completes_and_syncs(self):
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
                self.csv_path,
                "--min-class",
                "C",
                "--out",
                self.report_dir,
            ]
        )
        self.assertEqual(rc, 0)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        run = conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT 1").fetchone()
        self.assertIsNotNone(run)
        self.assertEqual(run["status"], "completed")
        self.assertGreaterEqual(run["synced_count"], 1)
        conn.close()

        self.assertEqual(len(_NotionE2EHandler.pages), 1)


if __name__ == "__main__":
    unittest.main()
