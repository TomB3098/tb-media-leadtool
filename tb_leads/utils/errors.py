from __future__ import annotations

from dataclasses import dataclass


class ErrorCode:
    NETWORK_TIMEOUT = "NETWORK_TIMEOUT"
    NETWORK_UNREACHABLE = "NETWORK_UNREACHABLE"
    NETWORK_HTTP_4XX = "NETWORK_HTTP_4XX"
    NETWORK_HTTP_5XX = "NETWORK_HTTP_5XX"
    NETWORK_RATE_LIMITED = "NETWORK_RATE_LIMITED"
    NETWORK_MAX_RETRIES = "NETWORK_MAX_RETRIES"

    NOTION_AUTH = "NOTION_AUTH"
    NOTION_FORBIDDEN = "NOTION_FORBIDDEN"
    NOTION_RATE_LIMITED = "NOTION_RATE_LIMITED"
    NOTION_SERVER_ERROR = "NOTION_SERVER_ERROR"

    RUN_ABORT_THRESHOLD = "RUN_ABORT_THRESHOLD"
    RUN_STEP_FAILED = "RUN_STEP_FAILED"


@dataclass
class ToolError(Exception):
    code: str
    message: str
    detail: str | None = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        if self.detail:
            return f"{self.code}: {self.message} ({self.detail})"
        return f"{self.code}: {self.message}"
