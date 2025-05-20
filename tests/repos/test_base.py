"""Tests for base objects and utilities for GerryDB API object repositories."""

from dataclasses import dataclass
from typing import Optional

import httpx
import pydantic
import pytest

from gerrydb.client import GerryDB, WriteContext
from gerrydb.exceptions import (
    OnlineError,
    ResultError,
    WriteContextError,
    RequestError,
    GerryPathError,
)
from gerrydb.repos.base import (
    err,
    online,
    write_context,
    namespaced,
    normalize_path,
    NamespacedObjectRepo,
)
from gerrydb.schemas import BaseModel


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


def test_namespaced_decorator__no_namespace(dummy_repo_offline):
    @namespaced
    def fn(repo: DummyRepo, path: str):
        """Needs a namespace."""
        pass

    with pytest.raises(RequestError):
        fn(dummy_repo_offline, "foo")


def test_bad_normalize_path():
    with pytest.raises(GerryPathError, match="Invalid path"):
        normalize_path("foo;bar")

    with pytest.raises(GerryPathError, match="Invalid path"):
        normalize_path("foo bar")

    with pytest.raises(GerryPathError, match="Invalid path"):
        normalize_path("foo..bar")

    with pytest.raises(GerryPathError, match="Invalid path"):
        normalize_path("foo//bar/", path_length=1)

    with pytest.raises(GerryPathError, match="Invalid path"):
        normalize_path("foo//bar/", case_sensitive_uid=True, path_length=1)


def test_basic_normalize_path():
    assert normalize_path("foo/bar") == "foo/bar"
    assert normalize_path("foo/bar/") == "foo/bar"
    assert normalize_path("foo//bar") == "foo/bar"
    assert normalize_path("foo//bar/") == "foo/bar"
    assert normalize_path("foo/bar/baz") == "foo/bar/baz"
    assert normalize_path("foo/bar/baz/") == "foo/bar/baz"
    assert normalize_path("foo/bar/baz//") == "foo/bar/baz"
    assert normalize_path("Foo/Bar") == "foo/bar"
    assert normalize_path("FoO/BaR") == "foo/bar"


def test_case_sensitive_normalize_path():
    assert normalize_path("foo/bar", case_sensitive_uid=True) == "foo/bar"
    assert normalize_path("foo/bar/", case_sensitive_uid=True) == "foo/bar"
    assert normalize_path("foo//bar", case_sensitive_uid=True) == "foo/bar"
    assert normalize_path("foo//bar/", case_sensitive_uid=True) == "foo/bar"
    assert normalize_path("foo/bar/baz", case_sensitive_uid=True) == "foo/bar/baz"
    assert normalize_path("foo/bar/baz/", case_sensitive_uid=True) == "foo/bar/baz"
    assert normalize_path("foo/bar/baz//", case_sensitive_uid=True) == "foo/bar/baz"
    assert normalize_path("Foo/Bar", case_sensitive_uid=True) == "foo/Bar"
    assert normalize_path("FoO/BaR", case_sensitive_uid=True) == "foo/BaR"
    assert normalize_path("FoO/BaR/BAZZ", case_sensitive_uid=True) == "foo/bar/BAZZ"


def test_missing_namespace():
    with pytest.raises(RequestError, match="No namespace specified"):
        NamespacedObjectRepo(
            schema=BaseModel,
            base_url="foo/bar",
            session=GerryDB(),
        ).all()


def test_bad__getitem__no_namespace():
    with pytest.raises(GerryPathError, match="Path cannot contain slashes"):
        NamespacedObjectRepo(
            schema=BaseModel,
            base_url="foo/bar",
            session=GerryDB(),
        )["foo/bar"]
