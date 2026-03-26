"""Tests for database functions in fetch_tiles.py (SQLite only)."""

import os

import pytest

from nbs.bluetopo._internal.config import (
    get_config,
    get_disk_fields,
    get_utm_file_columns,
    get_vrt_utm_fields,
)
from nbs.bluetopo._internal.download import (
    insert_new,
    update_records,
    all_db_tiles,
)
from nbs.bluetopo._internal.db import connect as connect_to_survey_registry


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
        count, _ = insert_new(conn, tiles, cfg)
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
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_bluetopo_filters_missing_link(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": None, "RAT_Link": "rat1"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_bag_inserts(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": "link1"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_bag_filters_no_issuance(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": None, "BAG": "link1"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_bag_filters_no_bag_field(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": None},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_s102v22_inserts(self, registry_db):
        cfg = get_config("s102v22")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V22": "link1"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_s102v30_inserts(self, registry_db):
        cfg = get_config("s102v30")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V30": "link1"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_s102v30_filters_none_string(self, registry_db):
        """S102V30 with 'None' string in link field."""
        cfg = get_config("s102v30")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V30": "None"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
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
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "utm": "19",
                "files": [
                    {"name": "geotiff", "disk": "BlueTopo/UTM19/T1.tif",
                     "source": "s3://bucket/T1.tif", "dest": "T1.tif", "checksum": "abc"},
                    {"name": "rat", "disk": "BlueTopo/UTM19/T1.tif.aux.xml",
                     "source": "s3://bucket/T1.aux", "dest": "T1.aux", "checksum": "def"},
                ],
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)

        tiles = all_db_tiles(conn)
        assert tiles[0]["geotiff_disk"] == "BlueTopo/UTM19/T1.tif"
        assert tiles[0]["geotiff_verified"] == 1

    def test_single_file_updates(self, registry_db):
        cfg = get_config("bag")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "utm": "19",
                "files": [
                    {"name": "file", "disk": "BAG/Data/T1.bag",
                     "source": "s3://bucket/T1.bag", "dest": "T1.bag", "checksum": "abc"},
                ],
            },
        }
        update_records(conn, download_dict, ["T1"], cfg)

        tiles = all_db_tiles(conn)
        assert tiles[0]["file_disk"] == "BAG/Data/T1.bag"
        assert tiles[0]["file_verified"] == 1

    def test_utm_upserted(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "utm": "19",
                "files": [
                    {"name": "geotiff", "disk": "path.tif",
                     "source": "s3://bucket/T1.tif", "dest": "T1.tif", "checksum": "abc"},
                    {"name": "rat", "disk": "path.aux",
                     "source": "s3://bucket/T1.aux", "dest": "T1.aux", "checksum": "def"},
                ],
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
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        download_dict = {"T1": {"tile": "T1", "utm": "19", "files": [
            {"name": "geotiff", "disk": "x",
             "source": "s3://bucket/x", "dest": "x", "checksum": "x"},
            {"name": "rat", "disk": "x",
             "source": "s3://bucket/x", "dest": "x", "checksum": "x"},
        ]}}
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
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_pmn_none_string_case_insensitive(self, registry_db):
        """PMN filter excludes 'none' regardless of case."""
        cfg = get_config("bag")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "BAG": "none"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
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
        count, _ = insert_new(conn, tiles, cfg)
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
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 1

    def test_s102v21_filters_no_link(self, registry_db):
        cfg = get_config("s102v21")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V21": None},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_s102v22_filters_none_string(self, registry_db):
        """S102V22 with 'None' string in link field."""
        cfg = get_config("s102v22")
        conn, _ = registry_db(cfg)
        tiles = [
            {"TILE_ID": "T1", "ISSUANCE": "2024-01-01", "S102V22": "None"},
        ]
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 0

    def test_empty_tile_list(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        count, _ = insert_new(conn, [], cfg)
        assert count == 0

    def test_bluetopo_filters_missing_rat_link(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": None},
        ]
        count, _ = insert_new(conn, tiles, cfg)
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
        count, _ = insert_new(conn, tiles, cfg)
        assert count == 1


# ---------------------------------------------------------------------------
# update_records edge cases
# ---------------------------------------------------------------------------


class TestUpdateRecordsEdge:
    def test_s102v30_multi_subdataset_utm(self, registry_db):
        """S102V30 update_records creates UTM with combined built flag."""
        cfg = get_config("s102v30")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "utm": "19",
                "files": [
                    {"name": "file", "disk": "S102V30/Data/T1.h5",
                     "source": "s3://bucket/T1.h5", "dest": "T1.h5", "checksum": "abc"},
                ],
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
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {
                "tile": "T1",
                "utm": "19",
                "files": [
                    {"name": "file", "disk": "S102V22/Data/T1.h5",
                     "source": "s3://bucket/T1.h5", "dest": "T1.h5", "checksum": "abc"},
                ],
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
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
            {"tilename": "T2", "utm": "20", "resolution": "2m"},
        ])
        download_dict = {
            "T1": {"tile": "T1", "utm": "19", "files": [
                {"name": "geotiff", "disk": "a.tif",
                 "source": "s3://bucket/a.tif", "dest": "a.tif", "checksum": "abc"},
                {"name": "rat", "disk": "a.aux",
                 "source": "s3://bucket/a.aux", "dest": "a.aux", "checksum": "def"},
            ]},
            "T2": {"tile": "T2", "utm": "20", "files": [
                {"name": "geotiff", "disk": "b.tif",
                 "source": "s3://bucket/b.tif", "dest": "b.tif", "checksum": "ghi"},
                {"name": "rat", "disk": "b.aux",
                 "source": "s3://bucket/b.aux", "dest": "b.aux", "checksum": "jkl"},
            ]},
        }
        update_records(conn, download_dict, ["T1", "T2"], cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM vrt_utm")
        assert cursor.fetchone()[0] == 2

    def test_resets_parameterized_partitions(self, registry_db):
        """Downloading tiles resets built flags in parameterized partitions."""
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        # Seed a default row and a parameterized row, both marked built
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO vrt_utm(utm, params_key, built) VALUES(?, ?, ?)",
            ("19", "", 1))
        cursor.execute(
            "INSERT INTO vrt_utm(utm, params_key, built) VALUES(?, ?, ?)",
            ("19", "_4m", 1))
        conn.commit()

        download_dict = {
            "T1": {"tile": "T1", "utm": "19", "files": [
                {"name": "geotiff", "disk": "path.tif",
                 "source": "s3://bucket/T1.tif", "dest": "T1.tif", "checksum": "abc"},
                {"name": "rat", "disk": "path.aux",
                 "source": "s3://bucket/T1.aux", "dest": "T1.aux", "checksum": "def"},
            ]},
        }
        update_records(conn, download_dict, ["T1"], cfg)

        cursor.execute("SELECT built FROM vrt_utm WHERE utm = '19' AND params_key = ''")
        assert cursor.fetchone()[0] == 0
        cursor.execute("SELECT built FROM vrt_utm WHERE utm = '19' AND params_key = '_4m'")
        assert cursor.fetchone()[0] == 0

    def test_resets_multi_subdataset_parameterized_partitions(self, registry_db):
        """Downloading tiles resets all built flags in parameterized partitions for subdataset sources."""
        cfg = get_config("s102v22")
        conn, _ = registry_db(cfg, tiles=[
            {"tilename": "T1", "utm": "19", "resolution": "2m"},
        ])
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO vrt_utm(utm, params_key, built_subdataset1, built_subdataset2, built_combined) "
            "VALUES(?, ?, ?, ?, ?)",
            ("19", "_4m", 1, 1, 1))
        conn.commit()

        download_dict = {
            "T1": {"tile": "T1", "utm": "19", "files": [
                {"name": "file", "disk": "S102V22/Data/T1.h5",
                 "source": "s3://bucket/T1.h5", "dest": "T1.h5", "checksum": "abc"},
            ]},
        }
        update_records(conn, download_dict, ["T1"], cfg)

        cursor.execute("SELECT * FROM vrt_utm WHERE utm = '19' AND params_key = '_4m'")
        row = dict(cursor.fetchone())
        assert row["built_subdataset1"] == 0
        assert row["built_subdataset2"] == 0
        assert row["built_combined"] == 0


