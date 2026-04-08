"""Tests for geospatial functions in fetch_tiles.py (GDAL/OGR)."""

import logging
import os

import pytest
from osgeo import gdal, ogr, osr

from nbs.noaabathymetry._internal.config import get_config
from nbs.noaabathymetry._internal.db import connect as connect_to_survey_registry
from nbs.noaabathymetry._internal.spatial import (
    get_tile_list,
    parse_geometry_input,
    transform_layer,
)
from nbs.noaabathymetry._internal.download import upsert_tiles, all_db_tiles


# ---------------------------------------------------------------------------
# get_tile_list
# ---------------------------------------------------------------------------


class TestGetTileList:
    def test_intersection_dual_file(self, make_polygon, make_tile_scheme):
        """BlueTopo-style tile scheme: intersecting polygon finds nearby tile."""
        poly = make_polygon(lon=-76.0, lat=37.0, size=0.02)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "https://bucket.s3.amazonaws.com/BlueTopo/T1/T1.tif",
             "RAT_Link": "https://bucket.s3.amazonaws.com/BlueTopo/T1/T1.tif.aux.xml",
             "UTM": "18", "Resolution": "2m",
             "GeoTIFF_SHA256_Checksum": "abc123", "RAT_SHA256_Checksum": "def456",
             "lon": -76.005, "lat": 37.005},
            {"tile": "T2", "Delivered_Date": "2024-01-02",
             "GeoTIFF_Link": "https://bucket.s3.amazonaws.com/BlueTopo/T2/T2.tif",
             "RAT_Link": "https://bucket.s3.amazonaws.com/BlueTopo/T2/T2.tif.aux.xml",
             "UTM": "18", "Resolution": "4m",
             "GeoTIFF_SHA256_Checksum": "abc789", "RAT_SHA256_Checksum": "def012",
             "lon": -80.0, "lat": 30.0},  # Far away
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        result = get_tile_list(poly, scheme)
        assert result is not None
        tile_names = [f.get("tile") for f in result]
        assert "T1" in tile_names
        assert "T2" not in tile_names

    def test_intersection_navigation(self, make_polygon, make_tile_scheme):
        """Navigation-style tile scheme: intersecting polygon finds nearby tile."""
        poly = make_polygon(lon=-76.0, lat=37.0, size=0.02)
        tiles = [
            {"TILE_ID": "US4VA01N", "REGION": "MidAtlantic", "SUBREGION": "Norfolk",
             "ISSUANCE": "2024-01-01",
             "BAG": "https://bucket.s3.amazonaws.com/BAG/US4VA01N.bag",
             "BAG_SHA256": "abc123",
             "Resolution": "2m", "UTM": "18",
             "lon": -76.005, "lat": 37.005},
            {"TILE_ID": "US4SC1EV", "REGION": "Southeast", "SUBREGION": "Wilmington",
             "ISSUANCE": "2024-01-02",
             "BAG": "https://bucket.s3.amazonaws.com/BAG/US4SC1EV.bag",
             "BAG_SHA256": "def456",
             "Resolution": "4m", "UTM": "17",
             "lon": -80.0, "lat": 30.0},
        ]
        scheme = make_tile_scheme(tiles, schema="navigation")
        result = get_tile_list(poly, scheme)
        assert result is not None
        tile_ids = [f.get("TILE_ID") for f in result]
        assert "US4VA01N" in tile_ids
        assert "US4SC1EV" not in tile_ids

    def test_no_intersection_returns_empty(self, make_polygon, make_tile_scheme):
        poly = make_polygon(lon=0.0, lat=0.0, size=0.01)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.0, "lat": 37.0},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        result = get_tile_list(poly, scheme)
        assert result == []

    def test_invalid_file_returns_none(self, tmp_path):
        bad_file = str(tmp_path / "nonexistent.shp")
        # GDAL exceptions are enabled globally, so ogr.Open raises RuntimeError
        # instead of returning None.
        try:
            result = get_tile_list(bad_file, bad_file)
            assert result is None
        except RuntimeError:
            pass  # Expected when gdal.UseExceptions() is active

    def test_different_crs_reprojected(self, make_tile_scheme, tmp_path):
        # Create polygon in UTM zone 18N that covers (-76, 37)
        # (-76, 37) in UTM 18N is approximately (411023, 4095340)
        poly_path = str(tmp_path / "utm_polygon.geojson")
        drv = ogr.GetDriverByName("GeoJSON")
        ds = drv.CreateDataSource(poly_path)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(32618)  # UTM 18N
        lyr = ds.CreateLayer("polygon", srs, ogr.wkbPolygon)
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint_2D(410000, 4094000)
        ring.AddPoint_2D(413000, 4094000)
        ring.AddPoint_2D(413000, 4097000)
        ring.AddPoint_2D(410000, 4097000)
        ring.AddPoint_2D(410000, 4094000)
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetGeometry(poly)
        lyr.CreateFeature(feat)
        ds = None

        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        result = get_tile_list(poly_path, scheme)
        assert result is not None
        assert len(result) >= 1


# ---------------------------------------------------------------------------
# transform_layer
# ---------------------------------------------------------------------------


class TestTransformLayer:
    def test_transforms_geometry(self):
        # Create a layer in EPSG:4326
        driver = ogr.GetDriverByName("MEMORY")
        src_ds = driver.CreateDataSource("src")
        src_srs = osr.SpatialReference()
        src_srs.ImportFromEPSG(4326)
        src_lyr = src_ds.CreateLayer("src", src_srs, ogr.wkbPoint)
        feat = ogr.Feature(src_lyr.GetLayerDefn())
        pt = ogr.Geometry(ogr.wkbPoint)
        pt.AddPoint_2D(-76.0, 37.0)
        feat.SetGeometry(pt)
        src_lyr.CreateFeature(feat)

        # Transform to UTM 18N
        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(32618)
        result = transform_layer(src_lyr, dst_srs)
        result_lyr = result.GetLayer(0)
        result_feat = result_lyr.GetNextFeature()
        geom = result_feat.GetGeometryRef()
        # UTM coords should be in hundreds of thousands range
        assert geom.GetX() > 100000
        assert geom.GetY() > 1000000

    def test_multiple_features(self):
        driver = ogr.GetDriverByName("MEMORY")
        src_ds = driver.CreateDataSource("src")
        src_srs = osr.SpatialReference()
        src_srs.ImportFromEPSG(4326)
        src_lyr = src_ds.CreateLayer("src", src_srs, ogr.wkbPoint)
        for lon, lat in [(-76.0, 37.0), (-77.0, 38.0)]:
            feat = ogr.Feature(src_lyr.GetLayerDefn())
            pt = ogr.Geometry(ogr.wkbPoint)
            pt.AddPoint_2D(lon, lat)
            feat.SetGeometry(pt)
            src_lyr.CreateFeature(feat)

        dst_srs = osr.SpatialReference()
        dst_srs.ImportFromEPSG(32618)
        result = transform_layer(src_lyr, dst_srs)
        result_lyr = result.GetLayer(0)
        count = result_lyr.GetFeatureCount()
        assert count == 2


# ---------------------------------------------------------------------------
# get_tile_list edge cases
# ---------------------------------------------------------------------------


class TestGetTileListEdge:
    def test_returns_all_fields(self, make_polygon, make_tile_scheme):
        """Returned dicts have all tile-scheme fields."""
        poly = make_polygon(lon=-76.0, lat=37.0, size=0.02)
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "GeoTIFF_SHA256_Checksum": "abc", "RAT_SHA256_Checksum": "def",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        result = get_tile_list(poly, scheme)
        assert len(result) == 1
        fields = result[0]
        assert "tile" in fields
        assert "Delivered_Date" in fields
        assert "GeoTIFF_Link" in fields
        assert "UTM" in fields

    def test_navigation_returns_all_fields(self, make_polygon, make_tile_scheme):
        """Navigation-style returns all 14 fields."""
        poly = make_polygon(lon=-76.0, lat=37.0, size=0.02)
        tiles = [
            {"TILE_ID": "US4VA01N", "REGION": "MidAtlantic", "SUBREGION": "Norfolk",
             "ISSUANCE": "2024-01-01",
             "BAG": "link1", "S102V21": None, "S102V22": None, "S102V30": None,
             "BAG_SHA256": "abc", "S102V21_SHA256": None, "S102V22_SHA256": None,
             "S102V30_SHA256": None,
             "Resolution": "2m", "UTM": "18",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="navigation")
        result = get_tile_list(poly, scheme)
        assert len(result) == 1
        assert "TILE_ID" in result[0]
        assert "BAG" in result[0]
        assert "S102V22" in result[0]

    def test_large_polygon_finds_multiple(self, make_polygon, make_tile_scheme):
        """Large polygon intersects multiple tiles."""
        poly = make_polygon(lon=-76.5, lat=37.5, size=1.0)
        # Place all tiles within the polygon bounds (-76.5 to -75.5, 36.5 to 37.5)
        tiles = [
            {"tile": f"T{i}", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": f"link{i}", "RAT_Link": f"rat{i}",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.4 + i * 0.3, "lat": 37.2}
            for i in range(3)
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        result = get_tile_list(poly, scheme)
        assert len(result) == 3


# ---------------------------------------------------------------------------
# upsert_tiles
# ---------------------------------------------------------------------------


class TestUpsertTiles:
    def _make_dual_file_tilescheme(self, tmp_path, tiles):
        """Create a BlueTopo-style gpkg tile scheme for upsert testing."""
        path = str(tmp_path / "UpsertTest_Tile_Scheme.gpkg")
        drv = ogr.GetDriverByName("GPKG")
        ds = drv.CreateDataSource(path)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        lyr = ds.CreateLayer("UpsertTest_Tile_Scheme", srs, ogr.wkbMultiPolygon)
        for field_name in ["tile", "GeoTIFF_Link", "RAT_Link", "Delivered_Date",
                           "Resolution", "UTM", "GeoTIFF_SHA256_Checksum",
                           "RAT_SHA256_Checksum"]:
            lyr.CreateField(ogr.FieldDefn(field_name, ogr.OFTString))
        defn = lyr.GetLayerDefn()
        for t in tiles:
            feat = ogr.Feature(defn)
            lon = t.get("lon", -76.3)
            lat = t.get("lat", 37.0)
            # Size 0.2 ensures tile fits within a single 1.2-degree global region tile
            size = 0.2
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint_2D(lon, lat)
            ring.AddPoint_2D(lon + size, lat)
            ring.AddPoint_2D(lon + size, lat - size)
            ring.AddPoint_2D(lon, lat - size)
            ring.AddPoint_2D(lon, lat)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            multipoly = ogr.Geometry(ogr.wkbMultiPolygon)
            multipoly.AddGeometry(poly)
            feat.SetGeometry(multipoly)
            for key, val in t.items():
                if key in ("lon", "lat"):
                    continue
                idx = defn.GetFieldIndex(key)
                if idx >= 0 and val is not None:
                    feat.SetField(key, str(val))
            lyr.CreateFeature(feat)
        ds = None
        return path

    def test_newer_date_triggers_upsert(self, tmp_path):
        """Tilescheme with newer delivery date updates the DB tile."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename, delivered_date) VALUES(?, ?)",
            ("T1", "2024-01-01"),
        )
        conn.commit()

        gpkg = self._make_dual_file_tilescheme(tmp_path, [
            {"tile": "T1", "Delivered_Date": "2024-06-15",
             "GeoTIFF_Link": "newlink.tif", "RAT_Link": "newrat.aux",
             "Resolution": "2m", "UTM": "18",
             "GeoTIFF_SHA256_Checksum": "abc", "RAT_SHA256_Checksum": "def"},
        ])

        upsert_tiles(conn, project_dir, gpkg, cfg)

        tiles = all_db_tiles(conn)
        assert len(tiles) == 1
        assert tiles[0]["delivered_date"] == "2024-06-15"
        assert tiles[0]["geotiff_link"] == "newlink.tif"
        # disk/verified fields should be cleared
        assert tiles[0]["geotiff_disk"] is None
        assert tiles[0]["geotiff_verified"] is None

    def test_same_date_no_update(self, tmp_path):
        """Same delivery date should not update the tile."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename, delivered_date, geotiff_link) VALUES(?, ?, ?)",
            ("T1", "2024-01-01", "oldlink.tif"),
        )
        conn.commit()

        gpkg = self._make_dual_file_tilescheme(tmp_path, [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "newlink.tif", "RAT_Link": "newrat.aux",
             "Resolution": "2m", "UTM": "18"},
        ])

        upsert_tiles(conn, project_dir, gpkg, cfg)

        tiles = all_db_tiles(conn)
        assert len(tiles) == 1
        # Link should remain unchanged
        assert tiles[0]["geotiff_link"] == "oldlink.tif"

    def test_tile_removed_from_tilescheme(self, tmp_path, caplog):
        """Tile in DB but not in tilescheme logs warning."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename, delivered_date) VALUES(?, ?)",
            ("REMOVED_TILE", "2024-01-01"),
        )
        conn.commit()

        # Empty tilescheme (tile not present)
        gpkg = self._make_dual_file_tilescheme(tmp_path, [])

        upsert_tiles(conn, project_dir, gpkg, cfg)

        assert any("removed" in r.message.lower() for r in caplog.records)

    def test_null_delivered_date_in_tilescheme(self, tmp_path, caplog):
        """Tile in tilescheme with null delivery date logs warning."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename, delivered_date) VALUES(?, ?)",
            ("T1", "2024-01-01"),
        )
        conn.commit()

        gpkg = self._make_dual_file_tilescheme(tmp_path, [
            {"tile": "T1", "Delivered_Date": None,
             "GeoTIFF_Link": "link.tif", "RAT_Link": "rat.aux",
             "Resolution": "2m", "UTM": "18"},
        ])

        upsert_tiles(conn, project_dir, gpkg, cfg)

        assert any("removal of delivered date" in r.message.lower()
                    for r in caplog.records)

    def test_duplicate_tilename_raises(self, tmp_path):
        """Duplicate tilename in tilescheme raises ValueError."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename, delivered_date) VALUES(?, ?)",
            ("T1", "2024-01-01"),
        )
        conn.commit()

        gpkg = self._make_dual_file_tilescheme(tmp_path, [
            {"tile": "T1", "Delivered_Date": "2024-06-15",
             "GeoTIFF_Link": "link1.tif", "RAT_Link": "rat1.aux",
             "Resolution": "2m", "UTM": "18"},
            {"tile": "T1", "Delivered_Date": "2024-06-15",
             "GeoTIFF_Link": "link2.tif", "RAT_Link": "rat2.aux",
             "Resolution": "2m", "UTM": "18"},
        ])

        with pytest.raises(ValueError, match="More than one tilename"):
            upsert_tiles(conn, project_dir, gpkg, cfg)

    def test_null_db_date_triggers_upsert(self, tmp_path):
        """Tile in DB with null delivered_date -> always upserted."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename) VALUES(?)",
            ("T1",),
        )
        conn.commit()

        gpkg = self._make_dual_file_tilescheme(tmp_path, [
            {"tile": "T1", "Delivered_Date": "2024-06-15",
             "GeoTIFF_Link": "link.tif", "RAT_Link": "rat.aux",
             "Resolution": "2m", "UTM": "18",
             "GeoTIFF_SHA256_Checksum": "abc", "RAT_SHA256_Checksum": "def"},
        ])

        upsert_tiles(conn, project_dir, gpkg, cfg)

        tiles = all_db_tiles(conn)
        assert len(tiles) == 1
        assert tiles[0]["delivered_date"] == "2024-06-15"
        assert tiles[0]["geotiff_link"] == "link.tif"

    def test_no_tiles_in_db_noop(self, tmp_path):
        """No tiles in DB -> upsert does nothing."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        gpkg = self._make_dual_file_tilescheme(tmp_path, [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link.tif", "RAT_Link": "rat.aux",
             "Resolution": "2m", "UTM": "18"},
        ])

        upsert_tiles(conn, project_dir, gpkg, cfg)

        tiles = all_db_tiles(conn)
        assert len(tiles) == 0


# ---------------------------------------------------------------------------
# parse_geometry_input
# ---------------------------------------------------------------------------


class TestParseGeometryInput:
    def test_bbox_returns_datasource(self):
        ds = parse_geometry_input("-76.1,36.9,-75.9,37.1")
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1
        srs = lyr.GetSpatialRef()
        assert srs is not None
        assert srs.GetAuthorityCode(None) == "4326"

    def test_bbox_invalid_order_raises(self):
        with pytest.raises(ValueError, match="xmin"):
            parse_geometry_input("-75.9,36.9,-76.1,37.1")

    def test_bbox_not_numbers_falls_through(self):
        with pytest.raises(ValueError, match="not a recognized"):
            parse_geometry_input("a,b,c,d")

    def test_wkt_polygon(self):
        wkt = "POLYGON((-76.1 36.9, -75.9 36.9, -75.9 37.1, -76.1 37.1, -76.1 36.9))"
        ds = parse_geometry_input(wkt)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_wkt_multipolygon(self):
        wkt = ("MULTIPOLYGON(((-76.1 36.9, -75.9 36.9, -75.9 37.1, -76.1 37.1, -76.1 36.9)),"
               "((-80.0 30.0, -79.8 30.0, -79.8 30.2, -80.0 30.2, -80.0 30.0)))")
        ds = parse_geometry_input(wkt)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_wkt_case_insensitive(self):
        wkt = "polygon((-76.1 36.9, -75.9 36.9, -75.9 37.1, -76.1 37.1, -76.1 36.9))"
        ds = parse_geometry_input(wkt)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_wkt_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid WKT"):
            parse_geometry_input("POLYGON((not valid wkt))")

    def test_geojson_polygon(self):
        geojson = '{"type":"Polygon","coordinates":[[[-76.1,36.9],[-75.9,36.9],[-75.9,37.1],[-76.1,37.1],[-76.1,36.9]]]}'
        ds = parse_geometry_input(geojson)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_geojson_feature(self):
        geojson = ('{"type":"Feature","geometry":{"type":"Polygon",'
                   '"coordinates":[[[-76.1,36.9],[-75.9,36.9],[-75.9,37.1],[-76.1,37.1],[-76.1,36.9]]]},'
                   '"properties":{}}')
        ds = parse_geometry_input(geojson)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_geojson_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid GeoJSON"):
            parse_geometry_input('{"type":"Polygon","coordinates":"bad"}')

    def test_geojson_with_whitespace(self):
        geojson = '  {"type":"Polygon","coordinates":[[[-76.1,36.9],[-75.9,36.9],[-75.9,37.1],[-76.1,37.1],[-76.1,36.9]]]}  '
        ds = parse_geometry_input(geojson)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_file_path(self, make_polygon):
        poly_path = make_polygon(lon=-76.0, lat=37.0, size=0.02)
        ds = parse_geometry_input(poly_path)
        assert ds is not None
        lyr = ds.GetLayer(0)
        assert lyr.GetFeatureCount() == 1

    def test_file_not_found_raises(self):
        with pytest.raises(ValueError, match="not a recognized"):
            parse_geometry_input("/nonexistent/path/to/file.shp")


# ---------------------------------------------------------------------------
# get_tile_list with DataSource inputs
# ---------------------------------------------------------------------------


class TestGetTileListWithDataSource:
    def test_bbox_intersection(self, make_tile_scheme):
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        ds = parse_geometry_input("-76.1,36.9,-75.9,37.1")
        result = get_tile_list(ds, scheme)
        assert result is not None
        assert len(result) >= 1
        assert any(f.get("tile") == "T1" for f in result)

    def test_wkt_intersection(self, make_tile_scheme):
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        wkt = "POLYGON((-76.1 36.9, -75.9 36.9, -75.9 37.1, -76.1 37.1, -76.1 36.9))"
        ds = parse_geometry_input(wkt)
        result = get_tile_list(ds, scheme)
        assert result is not None
        assert len(result) >= 1
        assert any(f.get("tile") == "T1" for f in result)

    def test_geojson_intersection(self, make_tile_scheme):
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        geojson = '{"type":"Polygon","coordinates":[[[-76.1,36.9],[-75.9,36.9],[-75.9,37.1],[-76.1,37.1],[-76.1,36.9]]]}'
        ds = parse_geometry_input(geojson)
        result = get_tile_list(ds, scheme)
        assert result is not None
        assert len(result) >= 1
        assert any(f.get("tile") == "T1" for f in result)

    def test_bbox_no_intersection(self, make_tile_scheme):
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        ds = parse_geometry_input("10.0,50.0,11.0,51.0")
        result = get_tile_list(ds, scheme)
        assert result == []


# ---------------------------------------------------------------------------
# Bug fix tests
# ---------------------------------------------------------------------------


class TestBugFixes:
    def test_null_crs_raises(self, make_tile_scheme):
        """Layer with no SRS raises ValueError."""
        tiles = [
            {"tile": "T1", "Delivered_Date": "2024-01-01",
             "GeoTIFF_Link": "link1", "RAT_Link": "rat1",
             "UTM": "18", "Resolution": "2m",
             "lon": -76.005, "lat": 37.005},
        ]
        scheme = make_tile_scheme(tiles, schema="dual_file")
        # Create a DataSource with no SRS
        driver = ogr.GetDriverByName("MEMORY")
        ds = driver.CreateDataSource("no_crs")
        lyr = ds.CreateLayer("no_crs", None, ogr.wkbPolygon)
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint_2D(-76.1, 36.9)
        ring.AddPoint_2D(-75.9, 36.9)
        ring.AddPoint_2D(-75.9, 37.1)
        ring.AddPoint_2D(-76.1, 37.1)
        ring.AddPoint_2D(-76.1, 36.9)
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetGeometry(poly)
        lyr.CreateFeature(feat)

        with pytest.raises(ValueError, match="no CRS defined"):
            get_tile_list(ds, scheme)
