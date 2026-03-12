"""Tests for database functions in build_vrt.py (SQLite only, no GDAL VRT creation)."""

import os
import sqlite3

import pytest

from nbs.bluetopo.core.datasource import (
    get_config,
    get_catalog_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_built_flags,
    get_utm_file_columns,
)
from nbs.bluetopo.core.build_vrt import (
    connect_to_survey_registry,
    select_tiles_by_utm,
    select_unbuilt_utms,
    update_utm,
    missing_utms,
)


# ---------------------------------------------------------------------------
# connect_to_survey_registry
# ---------------------------------------------------------------------------


class TestConnectToSurveyRegistry:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "bag", "s102v21", "s102v22", "s102v30", "hsd"])
    def test_creates_all_tables(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        assert cfg["catalog_table"] in tables
        assert "tiles" in tables
        assert "vrt_utm" in tables
        conn.close()

    @pytest.mark.parametrize("source", ["bluetopo", "bag", "s102v22", "s102v30"])
    def test_schema_matches_helpers(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()

        # Check catalog table columns
        catalog_table = cfg["catalog_table"]
        cursor.execute(f"SELECT name FROM pragma_table_info('{catalog_table}')")
        catalog_cols = {row[0] for row in cursor.fetchall()}
        expected_catalog = set(get_catalog_fields(cfg).keys())
        assert expected_catalog.issubset(catalog_cols)

        # Check tiles table columns
        cursor.execute("SELECT name FROM pragma_table_info('tiles')")
        tiles_cols = {row[0] for row in cursor.fetchall()}
        expected_tiles = set(get_tiles_fields(cfg).keys())
        assert expected_tiles.issubset(tiles_cols)

        # Check vrt_utm columns
        cursor.execute("SELECT name FROM pragma_table_info('vrt_utm')")
        utm_cols = {row[0] for row in cursor.fetchall()}
        expected_utm = set(get_vrt_utm_fields(cfg).keys())
        assert expected_utm.issubset(utm_cols)
        conn.close()

    def test_idempotent_connect(self, tmp_path):
        cfg = get_config("bluetopo")
        conn1 = connect_to_survey_registry(str(tmp_path), cfg)
        conn1.close()
        # Second call should not error
        conn2 = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn2.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('tiles')")
        cols = [row[0] for row in cursor.fetchall()]
        # No duplicates
        assert len(cols) == len(set(cols))
        conn2.close()

    def test_row_factory_set(self, tmp_path):
        cfg = get_config("bluetopo")
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        assert conn.row_factory == sqlite3.Row
        conn.close()

    def test_db_file_created(self, tmp_path):
        cfg = get_config("bluetopo")
        connect_to_survey_registry(str(tmp_path), cfg)
        assert os.path.isfile(os.path.join(str(tmp_path), "bluetopo_registry.db"))


# ---------------------------------------------------------------------------
# select_tiles_by_utm
# ---------------------------------------------------------------------------


class TestSelectTilesByUtm:
    def test_returns_tiles_for_utm(self, registry_db, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        tif = make_geotiff("tile1.tif")
        rat = make_geotiff("tile1.tif.aux.xml")
        rel_tif = os.path.relpath(tif, str(tmp_path))
        rel_rat = os.path.relpath(rat, str(tmp_path))
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "subregion": "R1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": rel_rat},
        ])
        result = select_tiles_by_utm(project_dir, conn, "19", cfg)
        assert len(result) == 1
        assert result[0]["tilename"] == "T1"

    def test_excludes_missing_files(self, registry_db, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        tif = make_geotiff("exists.tif")
        rat = make_geotiff("exists.tif.aux.xml")
        rel_tif = os.path.relpath(tif, str(tmp_path))
        rel_rat = os.path.relpath(rat, str(tmp_path))
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "subregion": "R1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": rel_rat},
            {"tilename": "T2", "subregion": "R1", "utm": "19",
             "resolution": "4m", "geotiff_disk": "missing.tif", "rat_disk": "missing.aux"},
        ])
        result = select_tiles_by_utm(project_dir, conn, "19", cfg)
        assert len(result) == 1
        assert result[0]["tilename"] == "T1"

    def test_sorts_coarse_to_fine(self, registry_db, tmp_path):
        cfg = get_config("bluetopo")
        # Create real files for each tile
        for name in ["t2m.tif", "t2m.tif.aux.xml",
                      "t8m.tif", "t8m.tif.aux.xml",
                      "t16m.tif", "t16m.tif.aux.xml"]:
            open(os.path.join(str(tmp_path), name), "w").close()
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T2", "subregion": "R1", "utm": "19",
             "resolution": "2m", "geotiff_disk": "t2m.tif", "rat_disk": "t2m.tif.aux.xml"},
            {"tilename": "T8", "subregion": "R1", "utm": "19",
             "resolution": "8m", "geotiff_disk": "t8m.tif", "rat_disk": "t8m.tif.aux.xml"},
            {"tilename": "T16", "subregion": "R1", "utm": "19",
             "resolution": "16m", "geotiff_disk": "t16m.tif", "rat_disk": "t16m.tif.aux.xml"},
        ])
        result = select_tiles_by_utm(project_dir, conn, "19", cfg)
        assert len(result) == 3
        resolutions = [r["resolution"] for r in result]
        assert resolutions == ["16m", "8m", "2m"]

    def test_empty_utm(self, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        result = select_tiles_by_utm(project_dir, conn, "99", cfg)
        assert result == []


# ---------------------------------------------------------------------------
# select_unbuilt_utms
# ---------------------------------------------------------------------------


class TestSelectUnbuilt:
    def test_unbuilt_utms(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[
            {"utm": "19", "built": 0},
            {"utm": "20", "built": 1},
        ])
        result = select_unbuilt_utms(conn, cfg)
        assert len(result) == 1
        assert result[0]["utm"] == "19"

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset_unbuilt_utm_combined(self, registry_db, source):
        cfg = get_config(source)
        conn, _ = registry_db(cfg, utms=[
            {"utm": "19", "built_subdataset1": 1, "built_subdataset2": 1, "built_combined": 0},
        ])
        result = select_unbuilt_utms(conn, cfg)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# update_utm
# ---------------------------------------------------------------------------


class TestUpdateUtm:
    def test_sets_built_flags(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[
            {"utm": "19", "built": 0},
        ])
        fields = {"utm": "19", "utm_vrt": "path/utm.vrt", "utm_ovr": "path/utm.ovr"}
        update_utm(conn, fields, cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built"] == 1
        assert row["utm_vrt"] == "path/utm.vrt"

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset_sets_combined(self, registry_db, source):
        cfg = get_config(source)
        conn, _ = registry_db(cfg, utms=[
            {"utm": "19", "built_subdataset1": 0, "built_subdataset2": 0, "built_combined": 0},
        ])
        fields = {
            "utm": "19",
            "utm_subdataset1_vrt": "p1.vrt",
            "utm_subdataset1_ovr": "p1.ovr",
            "utm_subdataset2_vrt": "p2.vrt",
            "utm_subdataset2_ovr": "p2.ovr",
            "utm_combined_vrt": "combined.vrt",
        }
        update_utm(conn, fields, cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built_subdataset1"] == 1
        assert row["built_subdataset2"] == 1
        assert row["built_combined"] == 1


# ---------------------------------------------------------------------------
# missing_utms
# ---------------------------------------------------------------------------


class TestMissingUtms:
    def test_resets_when_vrt_missing(self, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg, utms=[
            {"utm": "19", "built": 1, "utm_vrt": "missing.vrt", "utm_ovr": "missing.ovr"},
        ])
        count = missing_utms(project_dir, conn, cfg)
        assert count == 1
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built"] == 0
        assert row["utm_vrt"] is None

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset_resets(self, registry_db, source):
        cfg = get_config(source)
        conn, project_dir = registry_db(cfg, utms=[
            {"utm": "19", "built_subdataset1": 1, "built_subdataset2": 1,
             "built_combined": 1, "utm_subdataset1_vrt": "missing.vrt",
             "utm_subdataset1_ovr": "missing.ovr",
             "utm_subdataset2_vrt": "missing.vrt",
             "utm_subdataset2_ovr": "missing.ovr",
             "utm_combined_vrt": "missing.vrt"},
        ])
        count = missing_utms(project_dir, conn, cfg)
        assert count == 1
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built_subdataset1"] == 0
        assert row["built_combined"] == 0

    def test_no_reset_when_files_exist(self, registry_db, tmp_path):
        cfg = get_config("bluetopo")
        vrt_path = os.path.join(str(tmp_path), "utm19.vrt")
        ovr_path = vrt_path + ".ovr"
        with open(vrt_path, "w") as f:
            f.write("<VRT/>")
        with open(ovr_path, "w") as f:
            f.write("ovr")
        rel_vrt = os.path.relpath(vrt_path, str(tmp_path))
        rel_ovr = os.path.relpath(ovr_path, str(tmp_path))
        conn, project_dir = registry_db(cfg, utms=[
            {"utm": "19", "built": 1, "utm_vrt": rel_vrt, "utm_ovr": rel_ovr},
        ])
        count = missing_utms(project_dir, conn, cfg)
        assert count == 0


# ---------------------------------------------------------------------------
# Additional edge cases: update_utm
# ---------------------------------------------------------------------------


class TestUpdateEdge:
    def test_update_utm_preserves_none_cols(self, registry_db):
        """Columns not in fields dict remain None."""
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[
            {"utm": "19", "built": 0},
        ])
        fields = {"utm": "19", "utm_vrt": "path.vrt"}
        update_utm(conn, fields, cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["utm_vrt"] == "path.vrt"
        assert row["utm_ovr"] is None
        assert row["built"] == 1


# ---------------------------------------------------------------------------
# Idempotent schema evolution
# ---------------------------------------------------------------------------


class TestSchemaEvolution:
    def test_connect_twice_with_different_configs(self, tmp_path):
        """Connect with bluetopo, then bag -- both table sets exist."""
        cfg1 = get_config("bluetopo")
        project1 = str(tmp_path / "project")
        os.makedirs(project1, exist_ok=True)
        conn1 = connect_to_survey_registry(project1, cfg1)
        conn1.close()
        cfg2 = get_config("bag")
        project2 = str(tmp_path / "project2")
        os.makedirs(project2, exist_ok=True)
        conn2 = connect_to_survey_registry(project2, cfg2)
        cursor = conn2.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        assert "catalog" in tables
        conn2.close()
