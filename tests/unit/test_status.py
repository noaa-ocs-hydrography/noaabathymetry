"""Tests for status_tiles and StatusResult (no network, no S3)."""

import os
import sqlite3

import pytest

from nbs.noaabathymetry._internal.status import (
    StatusResult,
    _tile_info,
    _tile_files_exist,
    _log_grouped,
    _log_table,
)
from nbs.noaabathymetry._internal.config import get_config
from nbs.noaabathymetry._internal.db import connect as connect_to_survey_registry


# ---------------------------------------------------------------------------
# StatusResult dataclass
# ---------------------------------------------------------------------------


class TestStatusResult:
    def test_defaults(self):
        r = StatusResult()
        assert r.up_to_date == []
        assert r.updates_available == []
        assert r.missing_from_disk == []
        assert r.removed_from_scheme == []
        assert r.total_tracked == 0

    def test_fields(self):
        r = StatusResult(
            up_to_date=[{"tile": "T1"}],
            updates_available=[{"tile": "T2"}],
            missing_from_disk=[{"tile": "T3"}],
            removed_from_scheme=[{"tile": "T4"}],
            total_tracked=4,
        )
        assert len(r.up_to_date) == 1
        assert len(r.updates_available) == 1
        assert len(r.missing_from_disk) == 1
        assert len(r.removed_from_scheme) == 1
        assert r.total_tracked == 4


# ---------------------------------------------------------------------------
# _tile_info
# ---------------------------------------------------------------------------


class TestTileInfo:
    def test_basic(self):
        tile = {"tilename": "T1", "utm": "18", "resolution": "4m", "delivered_date": "2024-06-15"}
        info = _tile_info(tile)
        assert info["tile"] == "T1"
        assert info["utm"] == "18"
        assert info["resolution"] == "4m"
        assert info["local_datetime"] == "2024-06-15"

    def test_null_utm(self):
        tile = {"tilename": "T1", "utm": None, "resolution": "4m", "delivered_date": None}
        info = _tile_info(tile)
        assert info["utm"] == "Unknown"
        assert info["local_datetime"] is None

    def test_null_resolution(self):
        tile = {"tilename": "T1", "utm": "18", "resolution": None, "delivered_date": "2024-01-01"}
        info = _tile_info(tile)
        assert info["resolution"] == "Unknown"


# ---------------------------------------------------------------------------
# _tile_files_exist
# ---------------------------------------------------------------------------


class TestTileFilesExist:
    def test_files_present_and_verified(self, tmp_path):
        cfg = get_config("bluetopo")
        (tmp_path / "tile.tiff").touch()
        (tmp_path / "tile.tiff.aux.xml").touch()
        tile = {
            "geotiff_disk": "tile.tiff",
            "geotiff_verified": 1,
            "rat_disk": "tile.tiff.aux.xml",
            "rat_verified": 1,
        }
        assert _tile_files_exist(tile, str(tmp_path), cfg) is True

    def test_file_missing(self, tmp_path):
        cfg = get_config("bluetopo")
        tile = {
            "geotiff_disk": "missing.tiff",
            "geotiff_verified": 1,
            "rat_disk": "missing.aux.xml",
            "rat_verified": 1,
        }
        assert _tile_files_exist(tile, str(tmp_path), cfg) is False

    def test_not_verified(self, tmp_path):
        cfg = get_config("bluetopo")
        (tmp_path / "tile.tiff").touch()
        (tmp_path / "tile.tiff.aux.xml").touch()
        tile = {
            "geotiff_disk": "tile.tiff",
            "geotiff_verified": 0,
            "rat_disk": "tile.tiff.aux.xml",
            "rat_verified": 1,
        }
        assert _tile_files_exist(tile, str(tmp_path), cfg) is False

    def test_null_disk_path(self, tmp_path):
        cfg = get_config("bluetopo")
        tile = {
            "geotiff_disk": None,
            "geotiff_verified": 0,
            "rat_disk": None,
            "rat_verified": 0,
        }
        assert _tile_files_exist(tile, str(tmp_path), cfg) is False

    def test_single_slot_source(self, tmp_path):
        cfg = get_config("bag")
        (tmp_path / "tile.bag").touch()
        tile = {
            "file_disk": "tile.bag",
            "file_verified": 1,
        }
        assert _tile_files_exist(tile, str(tmp_path), cfg) is True


# ---------------------------------------------------------------------------
# _log_grouped (non-verbose output)
# ---------------------------------------------------------------------------


class TestLogGrouped:
    def test_groups_by_utm_and_resolution(self, caplog):
        tiles = [
            {"tile": "T1", "utm": "18", "resolution": "4m"},
            {"tile": "T2", "utm": "18", "resolution": "4m"},
            {"tile": "T3", "utm": "19", "resolution": "8m"},
        ]
        _log_grouped("Updates available", tiles)
        output = caplog.text
        assert "18:" in output
        assert "4m:  2 tiles" in output
        assert "19:" in output
        assert "8m:  1 tile" in output

    def test_singular_tile(self, caplog):
        tiles = [{"tile": "T1", "utm": "18", "resolution": "4m"}]
        _log_grouped("Test", tiles)
        assert "1 tile" in caplog.text
        assert "1 tiles" not in caplog.text

    def test_section_header(self, caplog):
        _log_grouped("Missing from disk", [{"tile": "T1", "utm": "18", "resolution": "4m"}])
        assert "Missing from disk" in caplog.text


# ---------------------------------------------------------------------------
# _log_table (verbose output)
# ---------------------------------------------------------------------------


class TestLogTable:
    def test_updates_with_remote(self, caplog):
        tiles = [
            {"tile": "BlueTopo_BC25L4NW", "utm": "18", "resolution": "4m",
             "local_datetime": "2024-03-01", "remote_datetime": "2024-06-15"},
        ]
        _log_table("Updates available", tiles, include_remote=True)
        output = caplog.text
        assert "BlueTopo_BC25L4NW" in output
        assert "2024-03-01" in output
        assert "2024-06-15" in output
        assert "Local datetime" in output
        assert "Remote datetime" in output

    def test_missing_without_remote(self, caplog):
        tiles = [
            {"tile": "BlueTopo_BC25M4NW", "utm": "18", "resolution": "8m",
             "local_datetime": "2024-03-01"},
        ]
        _log_table("Missing from disk", tiles, include_remote=False)
        output = caplog.text
        assert "BlueTopo_BC25M4NW" in output
        assert "Local datetime" in output
        assert "Remote datetime" not in output

    def test_null_datetime_shows_none(self, caplog):
        tiles = [
            {"tile": "T1", "utm": "18", "resolution": "4m",
             "local_datetime": None, "remote_datetime": "2024-06-15"},
        ]
        _log_table("Test", tiles, include_remote=True)
        assert "None" in caplog.text

    def test_unknown_utm(self, caplog):
        tiles = [
            {"tile": "T1", "utm": "Unknown", "resolution": "Unknown",
             "local_datetime": None},
        ]
        _log_table("Test", tiles)
        assert "Unknown" in caplog.text
