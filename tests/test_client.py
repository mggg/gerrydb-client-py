"""Tests for GerryDB session management."""
import os
from unittest import mock

import pytest
from gerrydb.client import ConfigError, GerryDB


def test_gerrydb_init_no_api_key():
    with pytest.raises(ConfigError, match="No API key"):
        GerryDB(host="example.com")


def test_gerrydb_init_no_host():
    with pytest.raises(ConfigError, match="No host"):
        GerryDB(key="key")


def test_gerrydb_init_host_key():
    assert GerryDB(key="key", host="example.com").cache is not None


def test_gerrydb_init_missing_config(tmp_path):
    with mock.patch.dict(os.environ, {"GERRYDB_ROOT": str(tmp_path)}):
        with pytest.raises(ConfigError, match="Failed to read"):
            GerryDB()


def test_gerrydb_init_invalid_config(tmp_path):
    with mock.patch.dict(os.environ, {"GERRYDB_ROOT": str(tmp_path)}):
        with open(tmp_path / "config", "w") as config_fp:
            config_fp.write("bad")
        with pytest.raises(ConfigError, match="Failed to parse"):
            GerryDB()


def test_gerrydb_init_missing_profile(tmp_path):
    with mock.patch.dict(os.environ, {"GERRYDB_ROOT": str(tmp_path)}):
        open(tmp_path / "config", "w").close()
        with pytest.raises(ConfigError, match="Profile"):
            GerryDB()


@pytest.mark.parametrize("field", ["host", "key"])
def test_gerrydb_init_missing_field(tmp_path, field):
    with mock.patch.dict(os.environ, {"GERRYDB_ROOT": str(tmp_path)}):
        other_field = "host" if field == "key" else "key"
        with open(tmp_path / "config", "w") as config_fp:
            print("[default]", file=config_fp)
            print(f'{other_field} = "test"', file=config_fp)
        with pytest.raises(ConfigError, match=f'Field "{field}"'):
            GerryDB()


def test_gerrydb_init_default_profile(tmp_path):
    with mock.patch.dict(os.environ, {"GERRYDB_ROOT": str(tmp_path)}):
        with open(tmp_path / "config", "w") as config_fp:
            print("[default]", file=config_fp)
            print('host = "example.com"', file=config_fp)
            print('key = "test"', file=config_fp)
        assert GerryDB().cache is not None


def test_gerrydb_init_alt_profile(tmp_path):
    with mock.patch.dict(os.environ, {"GERRYDB_ROOT": str(tmp_path)}):
        with open(tmp_path / "config", "w") as config_fp:
            print("[alt]", file=config_fp)
            print('host = "example.com"', file=config_fp)
            print('key = "test"', file=config_fp)
        assert GerryDB(profile="alt").cache is not None


def test_gerrydb_base_url():
    assert (
        GerryDB(key="key", host="example.com")._base_url == "https://example.com/api/v1"
    )


def test_gerrydb_base_url_localhost():
    assert (
        GerryDB(key="key", host="localhost:8080")._base_url
        == "http://localhost:8080/api/v1"
    )
