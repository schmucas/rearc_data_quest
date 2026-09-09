"""DataUSA population fetcher.

The DataUSA query endpoint has no caching validators (no ETag / Last-Modified)
and returns the full result set on every call, so change detection here is a
sha256 of the raw response body compared against the last known FETCHED row's
hash -- never a conditional GET.
"""

from __future__ import annotations

import hashlib

import requests

from sourcing.models import FetchOutcome

SOURCE = "datausa_population"
DATASET = "population"
LANDING_FILENAME = "population.json"  # fixed by convention: a query endpoint has no "original filename"
URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
)
REQUEST_TIMEOUT_SECONDS = 60


def fetch(session: requests.Session, last_known_sha256: str | None, url: str = URL) -> FetchOutcome:
    """GET the DataUSA population endpoint and compare its hash to last time.

    Args:
        session: Any requests.Session; DataUSA does not require a specific User-Agent.
        last_known_sha256: Prior FETCHED row's content_sha256, or None on a first run.
        url: The query endpoint.

    Returns:
        FETCHED if the body is new or this is the first run, UNCHANGED if the
        hash matches, or ERROR on a request failure.
    """
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        status_code = exc.response.status_code if exc.response is not None else None
        return FetchOutcome(
            status="ERROR",
            http_status=status_code,
            content_sha256=None,
            last_modified=None,
            body=None,
            byte_count=None,
            source_url=url,
            error_message=str(exc),
        )

    body = response.content
    digest = hashlib.sha256(body).hexdigest()
    if digest == last_known_sha256:
        return FetchOutcome(
            status="UNCHANGED",
            http_status=response.status_code,
            content_sha256=digest,
            last_modified=None,
            body=None,
            byte_count=None,
            source_url=url,
            error_message=None,
        )
    return FetchOutcome(
        status="FETCHED",
        http_status=response.status_code,
        content_sha256=digest,
        last_modified=None,
        body=body,
        byte_count=len(body),
        source_url=url,
        error_message=None,
    )
