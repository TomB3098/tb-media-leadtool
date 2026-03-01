from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from tb_leads.utils.errors import ErrorCode, ToolError
from tb_leads.utils.retry import RetryPolicy, retry_call
from tb_leads.utils.throttle import RateLimiter


@dataclass
class HttpResponse:
    status: int
    body: bytes
    headers: dict[str, str]


class HttpClient:
    def __init__(
        self,
        timeout_s: float = 10.0,
        rate_limiter: RateLimiter | None = None,
        retry_policy: RetryPolicy | None = None,
        user_agent: str = "tb-leads/1.0",
    ):
        self.timeout_s = timeout_s
        self.rate_limiter = rate_limiter
        self.retry_policy = retry_policy or RetryPolicy()
        self.user_agent = user_agent

    def _request_once(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None,
        payload: dict[str, Any] | None,
    ) -> HttpResponse:
        if self.rate_limiter:
            self.rate_limiter.acquire()

        req_headers = {"User-Agent": self.user_agent}
        if headers:
            req_headers.update(headers)

        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, method=method, headers=req_headers, data=data)

        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                body = resp.read()
                return HttpResponse(
                    status=int(resp.status),
                    body=body,
                    headers={k.lower(): v for k, v in resp.headers.items()},
                )
        except urllib.error.HTTPError as exc:
            body = exc.read() if hasattr(exc, "read") else b""
            status = int(exc.code)
            headers_map = {k.lower(): v for k, v in (exc.headers.items() if exc.headers else [])}

            if status == 429:
                raise ToolError(ErrorCode.NETWORK_RATE_LIMITED, "Remote rate limited request", detail=f"HTTP {status}")
            if 500 <= status <= 599:
                raise ToolError(ErrorCode.NETWORK_HTTP_5XX, "Server error", detail=f"HTTP {status}")
            if status in (401, 403):
                raise ToolError(ErrorCode.NETWORK_HTTP_4XX, "Auth/permission error", detail=f"HTTP {status}")
            raise ToolError(ErrorCode.NETWORK_HTTP_4XX, "Client error", detail=f"HTTP {status} body={body[:200]!r}")
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, socket.timeout):
                raise ToolError(ErrorCode.NETWORK_TIMEOUT, "Network timeout", detail=str(reason))
            raise ToolError(ErrorCode.NETWORK_UNREACHABLE, "Network unreachable", detail=str(reason))
        except TimeoutError as exc:
            raise ToolError(ErrorCode.NETWORK_TIMEOUT, "Timeout", detail=str(exc))

    def _retryable(self, exc: Exception) -> bool:
        if not isinstance(exc, ToolError):
            return False
        return exc.code in {
            ErrorCode.NETWORK_TIMEOUT,
            ErrorCode.NETWORK_UNREACHABLE,
            ErrorCode.NETWORK_RATE_LIMITED,
            ErrorCode.NETWORK_HTTP_5XX,
        }

    def request(
        self,
        method: str,
        url: str,
        headers: dict[str, str] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> HttpResponse:
        try:
            return retry_call(
                lambda: self._request_once(method, url, headers, payload),
                should_retry=self._retryable,
                policy=self.retry_policy,
            )
        except ToolError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ToolError(ErrorCode.NETWORK_MAX_RETRIES, "Request failed after retries", detail=str(exc)) from exc

    def get_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        response = self.request("GET", url, headers=headers)
        return response.body.decode("utf-8", errors="ignore")

    def get_json(self, url: str, headers: dict[str, str] | None = None) -> dict[str, Any]:
        response = self.request("GET", url, headers=headers)
        return json.loads(response.body.decode("utf-8"))

    def post_json(self, url: str, payload: dict[str, Any], headers: dict[str, str] | None = None) -> dict[str, Any]:
        req_headers = {"Content-Type": "application/json"}
        if headers:
            req_headers.update(headers)
        response = self.request("POST", url, headers=req_headers, payload=payload)
        return json.loads(response.body.decode("utf-8"))

    def patch_json(self, url: str, payload: dict[str, Any], headers: dict[str, str] | None = None) -> dict[str, Any]:
        req_headers = {"Content-Type": "application/json"}
        if headers:
            req_headers.update(headers)
        response = self.request("PATCH", url, headers=req_headers, payload=payload)
        return json.loads(response.body.decode("utf-8"))
