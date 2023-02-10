"""Tests for CherryDB session management."""
import os
import pytest

from unittest import mock
from cherrydb.client import CherryDB, CherryConfigError


def test_cherrydb_init_no_api_key():
    with pytest.raises(CherryConfigError, match="No API key"):
        CherryDB(host="example.com")


def test_cherrydb_init_no_host():
    with pytest.raises(CherryConfigError, match="No host"):
        CherryDB(key="key")


def test_cherrydb_init_host_key():
    assert CherryDB(key="key", host="example.com").cache is not None


def test_cherrydb_init_missing_config(tmp_path):
    with mock.patch.dict(os.environ, {"CHERRY_ROOT": str(tmp_path)}):
        with pytest.raises(CherryConfigError, match="Failed to read"):
            CherryDB()


def test_cherrydb_init_invalid_config(tmp_path):
    with mock.patch.dict(os.environ, {"CHERRY_ROOT": str(tmp_path)}):
        with open(tmp_path / "config", "w") as config_fp:
            config_fp.write("bad")
        with pytest.raises(CherryConfigError, match="Failed to parse"):
            CherryDB()


def test_cherrydb_init_missing_profile(tmp_path):
    with mock.patch.dict(os.environ, {"CHERRY_ROOT": str(tmp_path)}):
        open(tmp_path / "config", "w").close()
        with pytest.raises(CherryConfigError, match="Profile"):
            CherryDB()


@pytest.mark.parametrize("field", ["host", "key"])
def test_cherrydb_init_missing_field(tmp_path, field):
    with mock.patch.dict(os.environ, {"CHERRY_ROOT": str(tmp_path)}):
        other_field = "host" if field == "key" else "key"
        with open(tmp_path / "config", "w") as config_fp:
            print("[default]", file=config_fp)
            print(f'{other_field} = "test"', file=config_fp)
        with pytest.raises(CherryConfigError, match=f'Field "{field}"'):
            CherryDB()


def test_cherrydb_init_default_profile(tmp_path):
    with mock.patch.dict(os.environ, {"CHERRY_ROOT": str(tmp_path)}):
        with open(tmp_path / "config", "w") as config_fp:
            print("[default]", file=config_fp)
            print('host = "example.com"', file=config_fp)
            print('key = "test"', file=config_fp)
        assert CherryDB().cache is not None


def test_cherrydb_init_alt_profile(tmp_path):
    with mock.patch.dict(os.environ, {"CHERRY_ROOT": str(tmp_path)}):
        with open(tmp_path / "config", "w") as config_fp:
            print("[alt]", file=config_fp)
            print('host = "example.com"', file=config_fp)
            print('key = "test"', file=config_fp)
        assert CherryDB(profile="alt").cache is not None


def test_cherrydb_base_url():
    assert (
        CherryDB(key="key", host="example.com").base_url == "https://example.com/api/v1"
    )
