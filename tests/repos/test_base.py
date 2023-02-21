"""Tests for base objects and utilities for CherryDB API object repositories."""
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import httpx
import pydantic
import pytest

from cherrydb.cache import CacheResult
from cherrydb.client import CherryDB, WriteContext
from cherrydb.exceptions import OnlineError, ResultError, WriteContextError
from cherrydb.repos.base import err, match_etag, online, parse_etag, write_context
from cherrydb.schemas import BaseModel


@dataclass
class DummyRepo:
    """Dummy repository (no operations)."""

    session: "CherryDB"
    ctx: Optional["WriteContext"] = None


@dataclass
class MockResponse:
    """Partial mock of an HTTPX response."""

    headers: dict[str, str]


@pytest.fixture
def dummy_repo_offline():
    return DummyRepo(session=CherryDB(host="example.com", key="", offline=True))


def test_repos_err_decorator__http():
    @err("askew")
    def fn():
        raise httpx.HTTPError("request failed")

    with pytest.raises(ResultError, match="askew: HTTP"):
        fn()


def test_repos_err_decorator__validation():
    @err("askew")
    def fn():
        raise pydantic.ValidationError([], None)

    with pytest.raises(ResultError, match="askew: cannot parse"):
        fn()


def test_repos_online_decorator__offline(dummy_repo_offline):
    @online
    def fn(repo: DummyRepo):
        """Needs to be online."""

    with pytest.raises(OnlineError):
        fn(dummy_repo_offline)


def test_repos_write_context_decorator__no_write_context(dummy_repo_offline):
    @write_context
    def fn(repo: DummyRepo):
        """Needs to be online."""

    with pytest.raises(WriteContextError):
        fn(dummy_repo_offline)


def test_repos_match_etag__present():
    etag = uuid.uuid4()
    result = CacheResult(result=BaseModel(), cached_at=datetime.now(), etag=etag.bytes)
    assert match_etag(result) == {"If-None-Match": f'"{etag}"'}


def test_repos_match_etag__absent():
    result = CacheResult(result=BaseModel(), cached_at=datetime.now())
    assert match_etag(result) is None


def test_repos_parse_etag__present():
    etag = uuid.uuid4()
    response = MockResponse(headers={"ETag": f'"{etag}"'})
    assert parse_etag(response) == etag.bytes


def test_repos_etag__absent():
    response = MockResponse(headers={})
    assert parse_etag(response) is None