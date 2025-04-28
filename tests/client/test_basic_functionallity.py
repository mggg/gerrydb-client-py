"""Tests for GerryDB session management."""

import os
from unittest import mock
from pathlib import Path
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


def test_missing_profile(tmp_path, monkeypatch):
    # Create a valid TOML but without the default profile
    monkeypatch.setenv("GERRYDB_ROOT", str(tmp_path))
    good = tmp_path / "config"
    good.write_text(
        """
    [other_profile]
    host = "https://example.com"
    key = "abc"
    """
    )

    with pytest.raises(ConfigError) as excinfo:
        GerryDB()
    assert 'Profile "default" not found' in str(excinfo.value)


def test_missing_host_or_key(tmp_path, monkeypatch):
    with pytest.raises(ConfigError) as excinfo:
        GerryDB(host="https://example.com")
    assert "No API key specified for host" in str(excinfo.value)

    with pytest.raises(ConfigError) as excinfo:
        GerryDB(key="abc123")
    assert "No host specified for API key" in str(excinfo.value)


def test_gerrydb_cannot_create_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("GERRYDB_ROOT", str(tmp_path))


def test_failed_cache_directory_creation(tmp_path, monkeypatch):
    # 1) Point GerryDB at our empty tmp_path
    monkeypatch.setenv("GERRYDB_ROOT", str(tmp_path))

    # 2) Write a minimal valid config so we pass the parse & profile checks
    config_file = tmp_path / "config"
    config_file.write_text(
        """
[default]
host = "https://example.com"
key  = "secret-key"
"""
    )

    # 3) Stub out Path.mkdir to raise IOError only for the caches/default path
    real_mkdir = Path.mkdir

    def fake_mkdir(self, parents=False, exist_ok=False):
        # detect the exact path GerryDB will try to mk the cache dir
        bad = tmp_path / "caches" / "default"
        if self == bad:
            raise IOError("disk full, cannot create directory")
        # otherwise do the real thing (so parent dirs can be made if needed)
        return real_mkdir(self, parents=parents, exist_ok=exist_ok)

    monkeypatch.setattr(Path, "mkdir", fake_mkdir)

    # 4) Now calling GerryDB() should go through all the config steps
    #    and then hit our fake IOError → raise ConfigError
    with pytest.raises(ConfigError) as exc:
        GerryDB()

    assert "Failed to create cache directory." in str(exc.value)


def test_context_manager():
    # Test that the __enter__ method returns the GerryDB instance
    db = GerryDB(key="key", host="example.com")

    entered_db = db.__enter__()
    assert entered_db == db

    ret = db.__exit__(None, None, None)
    assert ret is False

    fake_exception = Exception("Fake exception")
    ret = db.__exit__(fake_exception, None, None)
    assert ret is False
