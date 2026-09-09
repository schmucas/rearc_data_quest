"""BLS productivity flat-file fetcher.

BLS's directory listing at download.bls.gov is an IIS-style plain listing
(absolute-path `<A HREF>`s, not Apache's relative autoindex), so every run
re-parses it rather than hardcoding filenames like `pr.data.1.AllData` --
BLS adding or removing a file changes nothing here.
BLS returns HTTP 403 to any request without a `User-Agent` carrying contact
info (https://www.bls.gov/bls/pss.htm); every request in this module,
including the listing GET itself, carries one.
"""

from __future__ import annotations

import hashlib
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

from sourcing.models import FetchOutcome

BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
SOURCE = "bls_pr"
REQUEST_TIMEOUT_SECONDS = 60


class _DirectoryListingParser(HTMLParser):
    """Collects plausible filenames from a plain directory-listing page.

    BLS actually serves an IIS-style listing: `<A HREF="/pub/time.series/pr/
    pr.class">` -- an *absolute* path, not a bare relative filename like a
    typical Apache mod_autoindex page. A parent-directory link
    (`<A HREF="/pub/time.series/">[To Parent Directory]</A>`) is the only
    other `<a>` on the page, distinguishable because it -- and any
    subdirectory link -- ends in "/", while every real file's href does not.
    Only the basename is kept, so this is agnostic to whether a given href is
    relative or absolute.
    """

    def __init__(self):
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if not href or href.startswith("?") or href.endswith("/"):
            return
        self.hrefs.append(href.rsplit("/", 1)[-1])


def build_session(contact_email: str) -> requests.Session:
    """Build a requests.Session carrying BLS's required contact User-Agent.

    Args:
        contact_email: Address BLS can use to reach the requester, sourced
            from the BLS_CONTACT_EMAIL environment variable (a GitHub Actions
            repository *variable* in CI, never a literal in source).

    Returns:
        A Session with User-Agent preset for every request made through it.
    """
    session = requests.Session()
    session.headers["User-Agent"] = f"rearc_data_quest data pipeline ({contact_email})"
    return session


def list_files(session: requests.Session, base_url: str = BASE_URL) -> list[str]:
    """Fetch and parse the BLS directory listing.

    Args:
        session: A Session from `build_session`.
        base_url: The directory listing URL.

    Returns:
        Filenames found on the page, in listing order.
    """
    response = session.get(base_url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    parser = _DirectoryListingParser()
    parser.feed(response.text)
    return parser.hrefs


def fetch_file(
    session: requests.Session, filename: str, last_modified: str | None, base_url: str = BASE_URL
) -> FetchOutcome:
    """Conditionally GET one BLS file.

    Sends `If-Modified-Since: <last_modified>` using the prior FETCHED row's
    raw header value verbatim (never reparsed/reformatted) when one exists.
    A brand-new filename -- no prior manifest row, so `last_modified is None`
    -- sends no conditional header and is therefore always FETCHED.

    Args:
        session: Session from `build_session`.
        filename: One href from `list_files`.
        last_modified: Prior FETCHED row's `last_modified`, or None.
        base_url: The listing URL, used to resolve `filename` to a full URL.

    Returns:
        FETCHED (200), UNCHANGED (304), or ERROR.
    """
    url = urljoin(base_url, filename)
    headers = {"If-Modified-Since": last_modified} if last_modified else {}
    try:
        response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        return FetchOutcome(
            status="ERROR",
            http_status=None,
            content_sha256=None,
            last_modified=None,
            body=None,
            byte_count=None,
            source_url=url,
            error_message=str(exc),
        )

    if response.status_code == 304:
        return FetchOutcome(
            status="UNCHANGED",
            http_status=304,
            content_sha256=None,
            last_modified=last_modified,
            body=None,
            byte_count=None,
            source_url=url,
            error_message=None,
        )
    if response.status_code >= 400:
        return FetchOutcome(
            status="ERROR",
            http_status=response.status_code,
            content_sha256=None,
            last_modified=None,
            body=None,
            byte_count=None,
            source_url=url,
            error_message=f"HTTP {response.status_code}",
        )

    body = response.content
    return FetchOutcome(
        status="FETCHED",
        http_status=response.status_code,
        content_sha256=hashlib.sha256(body).hexdigest(),
        last_modified=response.headers.get("Last-Modified"),
        body=body,
        byte_count=len(body),
        source_url=url,
        error_message=None,
    )
