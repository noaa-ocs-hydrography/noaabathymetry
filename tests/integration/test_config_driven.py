"""Parametrized integration tests across all remote data sources.

Verifies that DB schemas, column naming conventions, and round-trip
operations are consistent with each source's configuration.
"""

import os

import pytest

from nbs.bluetopo._internal.config import (
    get_config,
    get_catalog_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_vrt_built_flags,
    get_utm_file_columns,
)
from nbs.bluetopo._internal.db import connect as connect_to_survey_registry


REMOTE_SOURCES = ["bluetopo", "modeling", "bag", "s102v21", "s102v22", "s102v30"]


# ---------------------------------------------------------------------------
# DB schema consistency
# ---------------------------------------------------------------------------


class TestDbSchemaConsistency:
    @pytest.mark.parametrize("source", REMOTE_SOURCES)
    def test_catalog_table_matches_config(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        catalog_table = cfg["catalog_table"]
        cursor.execute(f"SELECT name FROM pragma_table_info('{catalog_table}')")
        db_cols = {row[0] for row in cursor.fetchall()}
        expected = set(get_catalog_fields(cfg).keys())
        assert expected.issubset(db_cols), f"{source}: catalog missing {expected - db_cols}"
        conn.close()

    @pytest.mark.parametrize("source", REMOTE_SOURCES)
    def test_tiles_table_matches_config(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('tiles')")
        db_cols = {row[0] for row in cursor.fetchall()}
        expected = set(get_tiles_fields(cfg).keys())
        assert expected.issubset(db_cols), f"{source}: tiles missing {expected - db_cols}"
        conn.close()

    @pytest.mark.parametrize("source", REMOTE_SOURCES)
    def test_vrt_utm_matches_config(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('vrt_utm')")
        db_cols = {row[0] for row in cursor.fetchall()}
        expected = set(get_vrt_utm_fields(cfg).keys())
        assert expected.issubset(db_cols), f"{source}: vrt_utm missing {expected - db_cols}"
        conn.close()


# ---------------------------------------------------------------------------
# Column naming conventions
# ---------------------------------------------------------------------------


class TestColumnNaming:
    @pytest.mark.parametrize("source", REMOTE_SOURCES)
    def test_utm_built_flags_count(self, source):
        cfg = get_config(source)
        built_flags = get_vrt_built_flags(cfg)
        if cfg["subdatasets"]:
            # One per subdataset + built_combined
            assert len(built_flags) == len(cfg["subdatasets"]) + 1
            assert "built_combined" in built_flags
        else:
            assert len(built_flags) == 1
            assert "built" in built_flags


