"""Tests for database functions in fetch_tiles.py (SQLite only)."""

import os

import pytest

from nbs.bluetopo.core.datasource import (
    get_config,
    get_disk_fields,
    get_utm_file_columns,
    get_vrt_utm_fields,
)
from nbs.bluetopo.core.fetch_tiles import (
    insert_new,
    update_records,
    all_db_tiles,
    sweep_files,
)
from nbs.bluetopo.core.build_vrt import connect_to_survey_registry


# ---------------------------------------------------------------------------
# insert_new
# ---------------------------------------------------------------------------


class TestInsertNew:
    def test_bluetopo_inserts(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1"},
            {"tile": "T2", "Delivered_Date": "2024-01-02",
             "GeoTIFF_Link": "link2", "RAT_Link": "rat2"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 2
        result = all_db_tiles(conn)
        assert len(result) == 2

    def test_bluetopo_filters_missing_date(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": None,
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_bluetopo_filters_missing_link(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": None, "RAT_Link": "rat1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_bag_inserts(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": "link1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_bag_filters_no_issuance(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": None, "BAG": "link1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_bag_filters_no_bag_field(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": None},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_s102v22_inserts(self, registry_db):
        cfg = get_config("s102v22")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V22": "link1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_s102v30_inserts(self, registry_db):
        cfg = get_config("s102v30")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V30": "link1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_s102v30_filters_none_string(self, registry_db):
        """S102V30 with 'None' string in link field."""
        cfg = get_config("s102v30")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V30": "None"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_idempotent(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1"},
        ]
        insert_new(conn, tiles, cfg)
        # Insert same tiles again
        insert_new(conn, tiles, cfg)
        result = all_db_tiles(conn)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# update_records
# ---------------------------------------------------------------------------


class TestUpdateRecords:
    def test_dual_file_updates(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "geotiff_disk": "BlueTopo/UTM19/T1.tif",
                "rat_disk": "BlueTopo/UTM19/T1.tif.aux.xml",
                "subregion": "R1",
                "utm": "19",
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)

        tiles = all_db_tiles(conn)
        assert tiles[0]["geotiff_disk"] == "BlueTopo/UTM19/T1.tif"
        assert tiles[0]["geotiff_verified"] == "True"

    def test_single_file_updates(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "file_disk": "BAG/Data/T1.bag",
                "subregion": "R1",
                "utm": "19",
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)

        tiles = all_db_tiles(conn)
        assert tiles[0]["file_disk"] == "BAG/Data/T1.bag"
        assert tiles[0]["file_verified"] == "True"

    def test_utm_upserted(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "geotiff_disk": "path.tif",
                "rat_disk": "path.aux",
                "subregion": "R1",
                "utm": "19",
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built"] == 0

    def test_no_records_when_no_successes(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
        ])
        download_dict = {"T1": {"tile": "T1", "geotiff_disk": "x", "rat_disk": "x",
                                "subregion": "R1", "utm": "19"}}
        update_records(conn, download_dict, [], cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm")
        assert cursor.fetchone() is None


# ---------------------------------------------------------------------------
# all_db_tiles
# ---------------------------------------------------------------------------


class TestAllDbTiles:
    def test_returns_all_tiles(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1"},
            {"tilename": "T2"},
        ])
        result = all_db_tiles(conn)
        assert len(result) == 2
        assert all(isinstance(r, dict) for r in result)

    def test_empty_table(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        result = all_db_tiles(conn)
        assert result == []


# ---------------------------------------------------------------------------
# sweep_files
# ---------------------------------------------------------------------------


class TestSweepFiles:
    def test_deletes_tile_with_missing_file(self, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": "missing.tif", "rat_disk": "missing.aux"},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 1
        assert all_db_tiles(conn) == []

    def test_leaves_existing_tiles(self, registry_db, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        tif = make_geotiff("tile1.tif")
        rat = make_geotiff("tile1.tif.aux.xml")
        rel_tif = os.path.relpath(tif, str(tmp_path))
        rel_rat = os.path.relpath(rat, str(tmp_path))
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": rel_rat},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 0
        assert len(all_db_tiles(conn)) == 1

    def test_cascades_to_utm_deletion(self, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": "missing.tif", "rat_disk": "missing.aux"},
        ], utms=[
            {"utm": "19", "built": 0},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert uu >= 1
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        assert cursor.fetchone() is None

    def test_single_file_schema(self, registry_db):
        cfg = get_config("bag")
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "file_disk": "missing.bag"},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 1


# ---------------------------------------------------------------------------
# insert_new edge cases
# ---------------------------------------------------------------------------


class TestInsertNewEdge:
    def test_pmn_none_string_filter(self, registry_db):
        """PMN filter excludes tiles where filter field is literal 'None' string."""
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": "None"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_pmn_none_string_case_insensitive(self, registry_db):
        """PMN filter excludes 'none' regardless of case."""
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": "none"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_mixed_valid_and_invalid_tiles(self, registry_db):
        """Mix of valid and filtered-out tiles."""
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1"},
            {"tile": "T2", "Delivered_Date": None,
             "GeoTIFF_Link": "link2", "RAT_Link": "rat2"},
            {"tile": "T3", "Delivered_Date": "2024-01-03",
             "GeoTIFF_Link": None, "RAT_Link": "rat3"},
            {"tile": "T4", "Delivered_Date": "2024-01-04",
             "GeoTIFF_Link": "link4", "RAT_Link": "rat4"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 2
        result = all_db_tiles(conn)
        names = {t["tilename"] for t in result}
        assert names == {"T1", "T4"}

    def test_s102v21_inserts(self, registry_db):
        cfg = get_config("s102v21")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V21": "link1"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_s102v21_filters_no_link(self, registry_db):
        cfg = get_config("s102v21")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V21": None},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_s102v22_filters_none_string(self, registry_db):
        """S102V22 with 'None' string in link field."""
        cfg = get_config("s102v22")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V22": "None"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_empty_tile_list(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        count = insert_new(conn, [], cfg)
        assert count == 0

    def test_bluetopo_filters_missing_rat_link(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": None},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_pmn_mixed_sources_in_navigation(self, registry_db):
        """BAG filter only counts tiles with BAG field, not S102V22."""
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": "link1",
             "S102V22": None},
            {"TILE_ID": "T2", "ISSUANCE": "2024-01-02", "BAG": None,
             "S102V22": "link2"},
        ]
        count = insert_new(conn, tiles, cfg)
        assert count == 1


# ---------------------------------------------------------------------------
# update_records edge cases
# ---------------------------------------------------------------------------


class TestUpdateRecordsEdge:
    def test_s102v30_multi_subdataset_utm(self, registry_db):
        """S102V30 update_records creates UTM with combined built flag."""
        cfg = get_config("s102v30")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "file_disk": "S102V30/Data/T1.h5",
                "subregion": "R1",
                "utm": "19",
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built_subdataset1"] == 0
        assert row["built_subdataset2"] == 0
        assert row["built_combined"] == 0

    def test_multi_subdataset_utm(self, registry_db):
        """S102V22 update_records creates UTM with combined built flag."""
        cfg = get_config("s102v22")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "file_disk": "S102V22/Data/T1.h5",
                "subregion": "R1",
                "utm": "19",
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19'")
        row = dict(cursor.fetchone())
        assert row["built_subdataset1"] == 0
        assert row["built_subdataset2"] == 0
        assert row["built_combined"] == 0

    def test_multiple_tiles_different_utms(self, registry_db):
        """Tiles in different UTMs -> separate UTM records."""
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1", "resolution": "2m"},
            {"tilename": "T2", "utm": "20", "subregion": "R2", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {"tile": "T1", "geotiff_disk": "a.tif", "rat_disk": "a.aux",
                    "subregion": "R1", "utm": "19"},
            "T2": {"tile": "T2", "geotiff_disk": "b.tif", "rat_disk": "b.aux",
                    "subregion": "R2", "utm": "20"},
        }
        update_records(conn, download_dict, ["T1", "T2"], cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM vrt_utm")
        assert cursor.fetchone()[0] == 2


# ---------------------------------------------------------------------------
# sweep_files edge cases
# ---------------------------------------------------------------------------


class TestSweepFilesEdge:
    def test_null_disk_fields_skipped(self, registry_db):
        """Tiles with None disk fields are skipped (no missing file check)."""
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": None, "rat_disk": None},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 0
        assert len(all_db_tiles(conn)) == 1

    def test_dual_file_partial_missing(self, registry_db, make_geotiff, tmp_path):
        """One file exists, other is missing -> tile deleted, existing file removed."""
        cfg = get_config("bluetopo")
        tif = make_geotiff("tile1.tif")
        rel_tif = os.path.relpath(tif, str(tmp_path))
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": "missing.aux"},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 1
        # The existing geotiff should be cleaned up
        assert not os.path.isfile(tif)

    def test_multiple_tiles_some_missing(self, registry_db, make_geotiff, tmp_path):
        """Multiple tiles -- only missing ones swept."""
        cfg = get_config("bluetopo")
        tif = make_geotiff("good.tif")
        rat = make_geotiff("good.aux.xml")
        rel_tif = os.path.relpath(tif, str(tmp_path))
        rel_rat = os.path.relpath(rat, str(tmp_path))
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": rel_rat},
            {"tilename": "T2", "utm": "19", "subregion": "R1",
             "resolution": "4m", "geotiff_disk": "gone.tif", "rat_disk": "gone.aux"},
        ])
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 1
        remaining = all_db_tiles(conn)
        assert len(remaining) == 1
        assert remaining[0]["tilename"] == "T1"

    def test_utm_vrt_files_cleaned(self, registry_db, tmp_path):
        """UTM VRT files removed when UTM is deleted."""
        cfg = get_config("bluetopo")
        utm_vrt = os.path.join(str(tmp_path), "utm19.vrt")
        with open(utm_vrt, "w") as f:
            f.write("<VRT/>")
        rel_utm_vrt = os.path.relpath(utm_vrt, str(tmp_path))
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "subregion": "R1",
             "resolution": "2m", "geotiff_disk": "missing.tif", "rat_disk": "missing.aux"},
        ], utms=[
            {"utm": "19", "built": 1, "utm_vrt": rel_utm_vrt},
        ])
        sweep_files(conn, project_dir, cfg)
        assert not os.path.isfile(utm_vrt)

    def test_no_tiles_returns_zeros(self, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        ut, uu = sweep_files(conn, project_dir, cfg)
        assert ut == 0
        assert uu == 0
