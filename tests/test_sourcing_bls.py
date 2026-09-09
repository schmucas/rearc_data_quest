"""Unit tests for sourcing/bls.py.

All HTTP is faked with a stub requests.Session -- no real network calls.
"""

import requests

from sourcing.bls import BASE_URL, _DirectoryListingParser, build_session, fetch_file, list_files

# Real IIS-style markup as served by download.bls.gov (captured directly):
# absolute-path HREFs, uppercase tag/attr, no trailing-slash except the
# parent link -- not a relative-path Apache mod_autoindex page.
DIRECTORY_LISTING_HTML = (
    "<html><head><title>download.bls.gov - /pub/time.series/pr/</title></head><body>"
    "<H1>download.bls.gov - /pub/time.series/pr/</H1><hr>\n\n"
    '<pre><A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>'
    '  9/3/2026  8:30 AM          102 <A HREF="/pub/time.series/pr/pr.class">pr.class</A><br>'
    ' 9/13/2022  4:52 PM          562 <A HREF="/pub/time.series/pr/pr.contacts">pr.contacts</A><br>'
    '  9/3/2026  8:30 AM      3239525 <A HREF="/pub/time.series/pr/pr.data.1.AllData">pr.data.1.AllData</A><br>'
    "</pre><hr></body></html>"
)


def test_directory_listing_parser_extracts_basenames_from_absolute_hrefs():
    parser = _DirectoryListingParser()
    parser.feed(DIRECTORY_LISTING_HTML)
    assert parser.hrefs == ["pr.class", "pr.contacts", "pr.data.1.AllData"]


def test_directory_listing_parser_also_handles_relative_apache_style_hrefs():
    html = """
    <a href="?C=N;O=D">Name</a>
    <a href="../">Parent Directory</a>
    <a href="pr.class">pr.class</a>
    <a href="subdir/">subdir/</a>
    """
    parser = _DirectoryListingParser()
    parser.feed(html)
    assert parser.hrefs == ["pr.class"]


def test_build_session_sets_user_agent_with_contact_email():
    session = build_session("you@example.com")
    assert "you@example.com" in session.headers["User-Agent"]


class _FakeResponse:
    def __init__(self, status_code, text="", content=b"", headers=None):
        self.status_code = status_code
        self.text = text
        self.content = content
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class _FakeSession:
    def __init__(self, response=None, exception=None):
        self._response = response
        self._exception = exception
        self.requests = []

    def get(self, url, timeout=None, headers=None):
        self.requests.append({"url": url, "headers": headers or {}})
        if self._exception:
            raise self._exception
        return self._response


def test_list_files_parses_response_body():
    session = _FakeSession(response=_FakeResponse(200, text=DIRECTORY_LISTING_HTML))
    assert list_files(session, base_url=BASE_URL) == ["pr.class", "pr.contacts", "pr.data.1.AllData"]


def test_fetch_file_sends_if_modified_since_when_prior_value_known():
    session = _FakeSession(response=_FakeResponse(304))
    fetch_file(session, "pr.series", "Wed, 01 Jan 2026 00:00:00 GMT", base_url=BASE_URL)
    assert session.requests[0]["headers"]["If-Modified-Since"] == "Wed, 01 Jan 2026 00:00:00 GMT"


def test_fetch_file_sends_no_conditional_header_for_brand_new_file():
    session = _FakeSession(response=_FakeResponse(200, content=b"data"))
    fetch_file(session, "pr.series", None, base_url=BASE_URL)
    assert session.requests[0]["headers"] == {}


def test_fetch_file_304_is_unchanged():
    session = _FakeSession(response=_FakeResponse(304))
    outcome = fetch_file(session, "pr.series", "Wed, 01 Jan 2026 00:00:00 GMT", base_url=BASE_URL)
    assert outcome.status == "UNCHANGED"
    assert outcome.http_status == 304
    assert outcome.content_sha256 is None
    assert outcome.body is None
    assert outcome.last_modified == "Wed, 01 Jan 2026 00:00:00 GMT"


def test_fetch_file_200_is_fetched_with_hash_and_last_modified():
    session = _FakeSession(
        response=_FakeResponse(200, content=b"hello world", headers={"Last-Modified": "Thu, 02 Jan 2026 00:00:00 GMT"})
    )
    outcome = fetch_file(session, "pr.series", None, base_url=BASE_URL)
    assert outcome.status == "FETCHED"
    assert outcome.http_status == 200
    assert outcome.body == b"hello world"
    assert outcome.byte_count == 11
    assert outcome.last_modified == "Thu, 02 Jan 2026 00:00:00 GMT"
    import hashlib

    assert outcome.content_sha256 == hashlib.sha256(b"hello world").hexdigest()


def test_fetch_file_5xx_is_error():
    session = _FakeSession(response=_FakeResponse(500))
    outcome = fetch_file(session, "pr.series", None, base_url=BASE_URL)
    assert outcome.status == "ERROR"
    assert outcome.http_status == 500
    assert outcome.error_message == "HTTP 500"


def test_fetch_file_network_exception_is_error_with_no_http_status():
    session = _FakeSession(exception=requests.ConnectionError("boom"))
    outcome = fetch_file(session, "pr.series", None, base_url=BASE_URL)
    assert outcome.status == "ERROR"
    assert outcome.http_status is None
    assert "boom" in outcome.error_message
