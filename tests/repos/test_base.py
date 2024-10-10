"""Tests for base objects and utilities for GerryDB API object repositories."""

from dataclasses import dataclass
from typing import Optional

import httpx
import pydantic
import pytest

from gerrydb.client import GerryDB, WriteContext
from gerrydb.exceptions import OnlineError, ResultError, WriteContextError
from gerrydb.repos.base import err, online, parse_path, write_context


@dataclass
class DummyRepo:
    """Dummy repository (no operations)."""

    session: "GerryDB"
    ctx: Optional["WriteContext"] = None


@dataclass
class MockResponse:
    """Partial mock of an HTTPX response."""

    headers: dict[str, str]


@pytest.fixture
def dummy_repo_offline():
    return DummyRepo(session=GerryDB(host="example.com", key="", offline=True))


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


def test_repos_parse_path__valid():
    namespace, path_in_namespace = parse_path("/a/b")
    assert namespace == "a"
    assert path_in_namespace == "b"


def test_repos_parse_path__invalid():
    with pytest.raises(KeyError, match="must contain"):
        parse_path("a")
