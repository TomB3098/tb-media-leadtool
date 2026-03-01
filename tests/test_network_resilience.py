import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tb_leads.utils.http import HttpClient
from tb_leads.utils.retry import RetryPolicy
from tb_leads.utils.throttle import RateLimiter


class _FlakyHandler(BaseHTTPRequestHandler):
    counter = 0

    def do_GET(self):  # noqa: N802
        type(self).counter += 1
        if type(self).counter < 3:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b"temporary")
            return

        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *_args):
        return


class NetworkResilienceTests(unittest.TestCase):
    def setUp(self):
        _FlakyHandler.counter = 0
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _FlakyHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/test"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_http_client_retries_on_5xx(self):
        client = HttpClient(
            timeout_s=2,
            rate_limiter=RateLimiter(max_requests_per_minute=1000),
            retry_policy=RetryPolicy(max_attempts=4, base_delay_s=0.01, max_delay_s=0.05, jitter_s=0),
        )

        body = client.get_text(self.url)
        self.assertEqual(body, "ok")
        self.assertEqual(_FlakyHandler.counter, 3)


if __name__ == "__main__":
    unittest.main()
