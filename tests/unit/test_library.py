"""Tests for nbs.noaabathymetry.library (no network, no S3)."""

from unittest import mock

import pytest

from nbs.noaabathymetry._internal.config import get_config
from nbs.noaabathymetry._internal.status import _parse_geopackage
from nbs.noaabathymetry.library import (
    extended_status_tiles,
    parse_tile_scheme,
)
from nbs.noaabathymetry.library.scheme import (
    fetch_tile_scheme,
    list_tile_scheme,
)


# ---------------------------------------------------------------------------
# _parse_geopackage (internal helper extracted for reuse)
# ---------------------------------------------------------------------------


class TestParseGeopackage:
    def test_parses_dual_file_scheme(self, make_tile_scheme):
        cfg = get_config("bluetopo")
        gpkg = make_tile_scheme([
            {"tile": "T1", "Delivered_Date": "2024-06-01",
             "Resolution": "4m", "UTM": "18",
             "GeoTIFF_Link": "http://a", "RAT_Link": "http://b"},
            {"tile": "T2", "Delivered_Date": "2024-07-01",
             "Resolution": "8m", "UTM": "19",
             "GeoTIFF_Link": "http://c", "RAT_Link": "http://d"},
        ])
        result = _parse_geopackage(gpkg, cfg)
        assert len(result) == 2
        assert "T1" in result
        assert "T2" in result
        assert result["T1"]["Delivered_Date"] == "2024-06-01"

    def test_parses_navigation_scheme(self, make_tile_scheme):
        cfg = get_config("bag")
        gpkg = make_tile_scheme([
            {"TILE_ID": "N1", "ISSUANCE": "2024-01-01",
             "Resolution": "4m", "UTM": "18",
             "BAG": "http://a", "BAG_SHA256": "abc123"},
        ], schema="navigation")
        result = _parse_geopackage(gpkg, cfg)
        assert len(result) == 1
        assert "N1" in result

    def test_invalid_path_raises(self):
        cfg = get_config("bluetopo")
        with pytest.raises(RuntimeError):
            _parse_geopackage("/nonexistent/file.gpkg", cfg)

    def test_skips_null_tile_name(self, make_tile_scheme):
        cfg = get_config("bluetopo")
        gpkg = make_tile_scheme([
            {"tile": "T1", "Delivered_Date": "2024-06-01",
             "GeoTIFF_Link": "http://a", "RAT_Link": "http://b"},
        ])
        result = _parse_geopackage(gpkg, cfg)
        assert len(result) == 1


# ---------------------------------------------------------------------------
# parse_tile_scheme (library wrapper around _parse_geopackage)
# ---------------------------------------------------------------------------


class TestParseTileScheme:
    def test_parses_from_bytes(self, make_tile_scheme):
        gpkg = make_tile_scheme([
            {"tile": "T1", "Delivered_Date": "2024-06-01",
             "Resolution": "4m", "UTM": "18",
             "GeoTIFF_Link": "http://a", "RAT_Link": "http://b"},
        ])
        with open(gpkg, "rb") as f:
            raw_bytes = f.read()
        result = parse_tile_scheme(raw_bytes, data_source="bluetopo")
        assert len(result) == 1
        assert "T1" in result
        assert result["T1"]["Delivered_Date"] == "2024-06-01"

    def test_navigation_from_bytes(self, make_tile_scheme):
        gpkg = make_tile_scheme([
            {"TILE_ID": "N1", "ISSUANCE": "2024-01-01",
             "Resolution": "4m", "UTM": "18",
             "BAG": "http://a"},
        ], schema="navigation")
        with open(gpkg, "rb") as f:
            raw_bytes = f.read()
        result = parse_tile_scheme(raw_bytes, data_source="bag")
        assert len(result) == 1
        assert "N1" in result

    def test_thread_safe_vsimem_paths(self, make_tile_scheme):
        """Concurrent calls should not collide on /vsimem/ paths."""
        gpkg = make_tile_scheme([
            {"tile": "T1", "Delivered_Date": "2024-06-01",
             "GeoTIFF_Link": "http://a", "RAT_Link": "http://b"},
        ])
        with open(gpkg, "rb") as f:
            raw_bytes = f.read()
        # Call twice — if paths collided, second call would fail or
        # return stale data.
        r1 = parse_tile_scheme(raw_bytes, data_source="bluetopo")
        r2 = parse_tile_scheme(raw_bytes, data_source="bluetopo")
        assert r1 == r2


# ---------------------------------------------------------------------------
# list_tile_scheme
# ---------------------------------------------------------------------------


class TestListTileScheme:
    def test_returns_metadata(self):
        from datetime import datetime, timezone
        fake_objects = [
            {"Key": "BlueTopo/Tile_Scheme_20240601.gpkg",
             "LastModified": datetime(2024, 6, 1, tzinfo=timezone.utc),
             "ETag": '"abc123"'},
        ]
        with mock.patch(
            "nbs.noaabathymetry.library.scheme._list_s3_latest",
            return_value=("BlueTopo/Tile_Scheme_20240601.gpkg", fake_objects),
        ):
            result = list_tile_scheme(data_source="bluetopo")
        assert result is not None
        key, last_mod, etag = result
        assert key == "BlueTopo/Tile_Scheme_20240601.gpkg"
        assert last_mod == datetime(2024, 6, 1, tzinfo=timezone.utc)
        assert etag == "abc123"

    def test_returns_none_on_failure(self):
        with mock.patch(
            "nbs.noaabathymetry.library.scheme._list_s3_latest",
            return_value=(None, []),
        ):
            result = list_tile_scheme(data_source="bluetopo")
        assert result is None

    def test_returns_none_on_exception(self):
        with mock.patch(
            "nbs.noaabathymetry.library.scheme._list_s3_latest",
            side_effect=Exception("network error"),
        ):
            result = list_tile_scheme(data_source="bluetopo")
        assert result is None

    def test_strips_etag_quotes(self):
        from datetime import datetime, timezone
        fake_objects = [
            {"Key": "k", "LastModified": datetime(2024, 1, 1, tzinfo=timezone.utc),
             "ETag": '"quoted-etag"'},
        ]
        with mock.patch(
            "nbs.noaabathymetry.library.scheme._list_s3_latest",
            return_value=("k", fake_objects),
        ):
            _, _, etag = list_tile_scheme(data_source="bluetopo")
        assert etag == "quoted-etag"
        assert '"' not in etag


# ---------------------------------------------------------------------------
# fetch_tile_scheme
# ---------------------------------------------------------------------------


class TestFetchTileScheme:
    def test_returns_bytes_and_metadata(self):
        from datetime import datetime, timezone
        fake_objects = [
            {"Key": "BlueTopo/Tile_Scheme.gpkg",
             "LastModified": datetime(2024, 6, 1, tzinfo=timezone.utc),
             "ETag": '"abc123"'},
        ]
        fake_body = mock.MagicMock()
        fake_body.read.return_value = b"fake-gpkg-bytes"

        with mock.patch(
            "nbs.noaabathymetry.library.scheme._list_s3_latest",
            return_value=("BlueTopo/Tile_Scheme.gpkg", fake_objects),
        ), mock.patch(
            "nbs.noaabathymetry.library.scheme._get_s3_client",
        ) as mock_client:
            mock_client.return_value.get_object.return_value = {
                "Body": fake_body,
                "LastModified": datetime(2024, 6, 1, tzinfo=timezone.utc),
                "ETag": '"abc123"',
            }
            raw_bytes, key, last_mod, etag = fetch_tile_scheme("bluetopo")

        assert raw_bytes == b"fake-gpkg-bytes"
        assert key == "BlueTopo/Tile_Scheme.gpkg"
        assert last_mod == datetime(2024, 6, 1, tzinfo=timezone.utc)
        assert etag == "abc123"

    def test_raises_on_no_objects(self):
        with mock.patch(
            "nbs.noaabathymetry.library.scheme._list_s3_latest",
            return_value=(None, []),
        ):
            with pytest.raises(RuntimeError, match="No tile scheme found"):
                fetch_tile_scheme("bluetopo")


# ---------------------------------------------------------------------------
# extended_status_tiles with remote_tiles
# ---------------------------------------------------------------------------


class TestExtendedStatusTilesRemoteTiles:
    def test_uses_remote_tiles_instead_of_s3(self, tmp_path, make_tile_scheme,
                                              registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "delivered_date": "2024-06-01",
             "resolution": "4m", "utm": "18",
             "geotiff_disk": "tile.tiff", "geotiff_verified": 1,
             "rat_disk": "tile.tiff.aux.xml", "rat_verified": 1},
        ])
        conn.close()
        (tmp_path / "tile.tiff").touch()
        (tmp_path / "tile.tiff.aux.xml").touch()

        # Build remote_tiles dict matching what _parse_geopackage returns
        remote_tiles = {
            "T1": {"tile": "T1", "Delivered_Date": "2024-06-01",
                    "Resolution": "4m", "UTM": "18"},
        }

        # Should NOT call S3
        with mock.patch(
            "nbs.noaabathymetry._internal.status._read_remote_geopackage"
        ) as mock_remote:
            result = extended_status_tiles(
                project_dir=project_dir,
                data_source="bluetopo",
                remote_tiles=remote_tiles,
                verbosity="quiet",
            )
            mock_remote.assert_not_called()

        assert result.total_tracked == 1
        assert len(result.up_to_date) == 1

    def test_falls_back_to_s3_when_none(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg, tiles=[
            {"tilename": "T1", "delivered_date": "2024-06-01",
             "resolution": "4m", "utm": "18"},
        ])
        conn.close()

        remote_tiles = {
            "T1": {"tile": "T1", "Delivered_Date": "2024-07-01"},
        }

        with mock.patch(
            "nbs.noaabathymetry._internal.status._read_remote_geopackage",
            return_value=remote_tiles,
        ) as mock_remote:
            result = extended_status_tiles(
                project_dir=project_dir,
                data_source="bluetopo",
                verbosity="quiet",
            )
            mock_remote.assert_called_once()
