"""Parametrized integration tests across all remote data sources.

Verifies that DB schemas, column naming conventions, and round-trip
operations are consistent with each source's configuration.
"""

import os

import pytest

from nbs.bluetopo.core.datasource import (
    get_config,
    get_catalog_fields,
    get_vrt_subregion_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_built_flags,
    get_vrt_file_columns,
    get_utm_file_columns,
    _subdataset_suffixes,
)
from nbs.bluetopo.core.build_vrt import (
    connect_to_survey_registry,
    select_unbuilt_subregions,
    update_subregion,
    missing_subregions,
)


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
    def test_vrt_subregion_matches_config(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('vrt_subregion')")
        db_cols = {row[0] for row in cursor.fetchall()}
        expected = set(get_vrt_subregion_fields(cfg).keys())
        assert expected.issubset(db_cols), f"{source}: vrt_subregion missing {expected - db_cols}"
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
    def test_subregion_suffix_convention(self, source):
        cfg = get_config(source)
        suffixes = _subdataset_suffixes(cfg)
        fields = get_vrt_subregion_fields(cfg)

        if suffixes == [None]:
            # Single dataset: no suffix in column names
            for col in fields:
                if col not in ("region", "utm", "built"):
                    assert "_subdataset" not in col, f"{source}: unexpected subdataset suffix in {col}"
        else:
            # Multi-subdataset: all resolution columns must have suffix
            for col in fields:
                if col.startswith("res_") or col.startswith("complete"):
                    assert any(s in col for s in suffixes), \
                        f"{source}: column {col} missing subdataset suffix"

    @pytest.mark.parametrize("source", REMOTE_SOURCES)
    def test_utm_built_flags_count(self, source):
        cfg = get_config(source)
        utm_fields = get_vrt_utm_fields(cfg)
        built_cols = [k for k in utm_fields if "built" in k]
        if cfg["subdatasets"]:
            # One per subdataset + built_combined
            expected = len(cfg["subdatasets"]) + 1
            assert len(built_cols) == expected
        else:
            assert len(built_cols) == 1
            assert "built" in built_cols


# ---------------------------------------------------------------------------
# Round-trip: insert -> update -> select_unbuilt -> missing -> select_unbuilt
# ---------------------------------------------------------------------------


class TestRoundTrip:
    @pytest.mark.parametrize("source", REMOTE_SOURCES)
    def test_subregion_lifecycle(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()

        # Insert an unbuilt subregion
        built_flags = get_built_flags(cfg)
        sr_data = {"region": "R1", "utm": "19"}
        for f in built_flags:
            sr_data[f] = 0
        cols = ", ".join(sr_data.keys())
        placeholders = ", ".join(["?"] * len(sr_data))
        cursor.execute(
            f"INSERT INTO vrt_subregion({cols}) VALUES({placeholders})",
            list(sr_data.values()),
        )
        conn.commit()

        # Should be unbuilt
        unbuilt = select_unbuilt_subregions(conn, cfg)
        assert len(unbuilt) == 1

        # Update with built=1
        vrt_cols = get_vrt_file_columns(cfg)
        fields = {"region": "R1"}
        for col in vrt_cols:
            if "complete" in col and col.endswith("_vrt"):
                # Create a fake VRT file
                vrt_path = os.path.join(str(tmp_path), f"R1_{col}.vrt")
                with open(vrt_path, "w") as f_:
                    f_.write("<VRT/>")
                fields[col] = os.path.relpath(vrt_path, str(tmp_path))
            else:
                fields[col] = None
        update_subregion(conn, fields, cfg)

        # Should now be empty
        unbuilt = select_unbuilt_subregions(conn, cfg)
        assert len(unbuilt) == 0

        # Delete VRT files to simulate missing
        for col in vrt_cols:
            if fields.get(col) and "complete" in col:
                fpath = os.path.join(str(tmp_path), fields[col])
                if os.path.isfile(fpath):
                    os.remove(fpath)

        # Also insert a UTM record so missing_subregions can reset it
        utm_data = {"utm": "19"}
        utm_fields = get_vrt_utm_fields(cfg)
        for k in utm_fields:
            if k != "utm":
                if "built" in k:
                    utm_data[k] = 1
                else:
                    utm_data[k] = None
        utm_cols_str = ", ".join(utm_data.keys())
        utm_ph = ", ".join(["?"] * len(utm_data))
        cursor.execute(
            f"INSERT OR REPLACE INTO vrt_utm({utm_cols_str}) VALUES({utm_ph})",
            list(utm_data.values()),
        )
        conn.commit()

        # missing_subregions should reset
        count = missing_subregions(str(tmp_path), conn, cfg)
        assert count == 1

        # Should be unbuilt again
        unbuilt = select_unbuilt_subregions(conn, cfg)
        assert len(unbuilt) == 1
