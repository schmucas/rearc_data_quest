"""Shared data shapes passed between the fetchers and the orchestrator."""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FetchOutcome:
    """The result of fetching one item, before it becomes a manifest row.

    Attributes:
        status: "FETCHED" | "UNCHANGED" | "ERROR".
        http_status: The response status code, or None on a request-level failure.
        content_sha256: sha256 of the response body, or None (304 has no body).
        last_modified: The raw Last-Modified header value (BLS only), verbatim.
        body: Raw bytes to land, present only when status == "FETCHED".
        byte_count: len(body), or None for UNCHANGED/ERROR.
        source_url: The exact URL fetched.
        error_message: Populated only when status == "ERROR".
    """

    status: str
    http_status: Optional[int]
    content_sha256: Optional[str]
    last_modified: Optional[str]
    body: Optional[bytes]
    byte_count: Optional[int]
    source_url: str
    error_message: Optional[str]
