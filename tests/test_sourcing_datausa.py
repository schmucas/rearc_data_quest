"""Unit tests for sourcing/datausa.py."""

import hashlib

import requests

from sourcing.datausa import URL, fetch


class _FakeResponse:
    def __init__(self, status_code, content=b""):
        self.status_code = status_code
        self.content = content

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error


class _FakeSession:
    def __init__(self, response=None, exception=None):
        self._response = response
        self._exception = exception

    def get(self, url, timeout=None):
        if self._exception:
            raise self._exception
        return self._response


def test_fetch_first_run_is_fetched():
    body = b'{"data": [1, 2, 3]}'
    session = _FakeSession(response=_FakeResponse(200, content=body))
    outcome = fetch(session, last_known_sha256=None, url=URL)
    assert outcome.status == "FETCHED"
    assert outcome.body == body
    assert outcome.byte_count == len(body)
    assert outcome.content_sha256 == hashlib.sha256(body).hexdigest()
    assert outcome.last_modified is None


def test_fetch_hash_match_is_unchanged():
    body = b'{"data": [1, 2, 3]}'
    digest = hashlib.sha256(body).hexdigest()
    session = _FakeSession(response=_FakeResponse(200, content=body))
    outcome = fetch(session, last_known_sha256=digest, url=URL)
    assert outcome.status == "UNCHANGED"
    assert outcome.content_sha256 == digest
    assert outcome.body is None
    assert outcome.byte_count is None


def test_fetch_hash_mismatch_is_fetched():
    old_digest = hashlib.sha256(b"old body").hexdigest()
    new_body = b"new body"
    session = _FakeSession(response=_FakeResponse(200, content=new_body))
    outcome = fetch(session, last_known_sha256=old_digest, url=URL)
    assert outcome.status == "FETCHED"
    assert outcome.body == new_body


def test_fetch_http_error_is_error_with_status_code():
    response = _FakeResponse(503)
    session = _FakeSession(response=response)
    outcome = fetch(session, last_known_sha256=None, url=URL)
    assert outcome.status == "ERROR"
    assert outcome.http_status == 503
    assert outcome.body is None


def test_fetch_network_exception_is_error_with_no_status():
    session = _FakeSession(exception=requests.ConnectionError("boom"))
    outcome = fetch(session, last_known_sha256=None, url=URL)
    assert outcome.status == "ERROR"
    assert outcome.http_status is None
    assert "boom" in outcome.error_message
