"""Fixtures for repository tests."""
import pytest


@pytest.fixture
def pop_column_meta():
    """Example metadata for a population column."""
    return {
        "path": "total_pop",
        "description": "2020 Census total population",
        "source_url": "https://www.census.gov/",
        "column_kind": "count",
        "column_type": "int",
        "aliases": ["totpop", "p001001", "p0001001"],
    }


@pytest.fixture
def vap_column_meta():
    """Example metadata for a voting-age population column."""
    return {
        "path": "total_vap",
        "description": "2020 Census total voting-age population (VAP)",
        "source_url": "https://www.census.gov/",
        "column_kind": "count",
        "column_type": "float",
        "aliases": ["totvap", "p003001", "p0003001"],
    }
