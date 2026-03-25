"""Tests for RAT aggregation logic in build_vrt.py (requires GDAL)."""

import logging
import os

import pytest
from osgeo import gdal

from nbs.bluetopo._internal.config import get_config, get_local_config, KNOWN_RAT_FIELDS
from nbs.bluetopo._internal.vrt import add_vrt_rat, create_vrt

# Minimal RAT fields for testing (subset of BlueTopo)
MINI_RAT_FIELDS = {
    "value": [int, gdal.GFU_MinMax],
    "count": [int, gdal.GFU_PixelCount],
    "source_survey_id": [str, gdal.GFU_Generic],
    "coverage": [int, gdal.GFU_Generic],
}


def _make_bt_cfg():
    """Return a BlueTopo config with mini RAT fields for simpler testing."""
    cfg = get_config("bluetopo")
    cfg["rat_fields"] = MINI_RAT_FIELDS
    return cfg


class TestAddVrtRat:
    def test_noop_when_no_rat(self):
        cfg = get_config("bag")
        # Should return immediately without error
        add_vrt_rat([], "/dummy", "dummy.vrt", cfg)

    def test_direct_method_writes_rat(self, make_geotiff, tmp_path):
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)
        rat_entries = [
            [1, 100, "SURVEY_A", 80],
            [2, 200, "SURVEY_B", 90],
        ]
        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel_tif = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": rel_tif},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False, ["Elevation", "Uncertainty", "Contributor"])

        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat is not None
        assert rat.GetRowCount() == 2
        assert rat.GetColumnCount() == len(MINI_RAT_FIELDS)
        ds = None

    def test_survey_dedup_sums_counts(self, make_geotiff, tmp_path):
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)

        # Two tiles with same survey ID
        rat_entries1 = [[1, 100, "SURVEY_A", 80]]
        rat_entries2 = [[1, 150, "SURVEY_A", 80]]

        tif1 = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries1, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        tif2 = make_geotiff(
            "tile2.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries2, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel1 = os.path.relpath(tif1, project_dir)
        rel2 = os.path.relpath(tif2, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel1, "rat_disk": rel1},
            {"tilename": "T2", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel2, "rat_disk": rel2},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif1, tif2], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetRowCount() == 1  # Deduped
        # Count should be summed: 100 + 150 = 250
        assert rat.GetValueAsInt(0, 1) == 250
        ds = None

    def test_pixel_count_cap(self, make_geotiff, tmp_path):
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)
        max_int = 2147483647

        rat_entries1 = [[1, max_int, "SURVEY_A", 80]]
        rat_entries2 = [[1, 100, "SURVEY_A", 80]]

        tif1 = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries1, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        tif2 = make_geotiff(
            "tile2.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries2, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel1 = os.path.relpath(tif1, project_dir)
        rel2 = os.path.relpath(tif2, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel1, "rat_disk": rel1},
            {"tilename": "T2", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel2, "rat_disk": rel2},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif1, tif2], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetValueAsInt(0, 1) == max_int
        ds = None

    def test_column_types(self, make_geotiff, tmp_path):
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)
        rat_entries = [[1, 100, "SURVEY_A", 80]]
        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel, "rat_disk": rel},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetTypeOfCol(0) == gdal.GFT_Integer  # value
        assert rat.GetTypeOfCol(1) == gdal.GFT_Integer  # count
        assert rat.GetTypeOfCol(2) == gdal.GFT_String   # source_survey_id
        assert rat.GetTypeOfCol(3) == gdal.GFT_Integer   # coverage
        ds = None

    def test_missing_tile_files_raises(self, make_geotiff, tmp_path):
        """Tiles with missing files raise FileNotFoundError."""
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": "missing.tif", "rat_disk": "missing.aux"},
        ]
        dummy = make_geotiff("dummy.tif", bands=3, width=4, height=4)
        vrt_path = os.path.join(project_dir, "utm19.vrt")
        create_vrt([dummy], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])

        with pytest.raises(FileNotFoundError, match="T1"):
            add_vrt_rat(tiles, project_dir, vrt_path, cfg)


# ---------------------------------------------------------------------------
# RAT zero fields
# ---------------------------------------------------------------------------


class TestRatZeroFields:
    def test_zero_fields_forced(self, make_geotiff, tmp_path):
        """Fields in rat_zero_fields are forced to 0 during aggregation."""
        cfg = get_config("bluetopo")
        rat_fields = {
            "value": [int, gdal.GFU_MinMax],
            "count": [int, gdal.GFU_PixelCount],
            "source_survey_id": [str, gdal.GFU_Generic],
            "coverage": [int, gdal.GFU_Generic],
        }
        cfg["rat_fields"] = rat_fields
        cfg["rat_zero_fields"] = ["coverage"]

        project_dir = str(tmp_path)
        rat_entries = [
            [1, 100, "SURVEY_A", 80],
        ]
        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries, rat_fields=rat_fields, rat_band=3,
        )
        rel_tif = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel_tif, "rat_disk": rel_tif},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetRowCount() == 1
        # "coverage" (col 3) should be forced to 0
        assert rat.GetValueAsInt(0, 3) == 0
        ds = None


# ---------------------------------------------------------------------------
# RAT with multiple surveys
# ---------------------------------------------------------------------------


class TestRatMultipleSurveys:
    def test_different_surveys_not_deduped(self, make_geotiff, tmp_path):
        """Different survey IDs across tiles are kept separate."""
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)

        rat_entries1 = [[1, 100, "SURVEY_A", 80]]
        rat_entries2 = [[2, 200, "SURVEY_B", 90]]

        tif1 = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries1, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        tif2 = make_geotiff(
            "tile2.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries2, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel1 = os.path.relpath(tif1, project_dir)
        rel2 = os.path.relpath(tif2, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel1, "rat_disk": rel1},
            {"tilename": "T2", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel2, "rat_disk": rel2},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif1, tif2], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetRowCount() == 2
        ds = None

    def test_no_rat_on_file_skipped_gracefully(self, make_geotiff, tmp_path):
        """Tile GeoTIFF without a RAT -> GetDefaultRAT() returns None.

        The tile should be skipped gracefully, producing an empty RAT.
        """
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)

        # Create a GeoTIFF WITHOUT a RAT (rat_entries not passed)
        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
        )
        rel = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel, "rat_disk": rel},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])

        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetRowCount() == 0
        ds = None

    def test_no_tiles(self, make_geotiff, tmp_path):
        """Empty tiles list -> RAT has 0 rows."""
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)
        dummy = make_geotiff("dummy.tif", bands=3, width=4, height=4)
        vrt_path = str(tmp_path / "utm99.vrt")
        create_vrt([dummy], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])

        add_vrt_rat([], project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetRowCount() == 0
        ds = None


# ---------------------------------------------------------------------------
# HSD RAT fields (superset of BlueTopo mini)
# ---------------------------------------------------------------------------


HSD_RAT_FIELDS = {
    "value": [int, gdal.GFU_MinMax],
    "count": [int, gdal.GFU_PixelCount],
    "source_survey_id": [str, gdal.GFU_Generic],
    "coverage": [int, gdal.GFU_Generic],
    "catzoc": [int, gdal.GFU_Generic],
}


# ---------------------------------------------------------------------------
# Dynamic field detection
# ---------------------------------------------------------------------------


class TestDynamicFieldDetection:
    def test_mixed_schemas_uses_common_subset(self, make_geotiff, tmp_path, caplog):
        """Tile1 has 4 fields (BT), tile2 has 5 fields (HSD) -> VRT RAT uses 4 common fields."""
        cfg = _make_bt_cfg()
        # Give config the full HSD superset so it must be trimmed
        cfg["rat_fields"] = dict(HSD_RAT_FIELDS)
        project_dir = str(tmp_path)

        bt_entries = [[1, 100, "SURVEY_A", 80]]
        hsd_entries = [[2, 200, "SURVEY_B", 90, 3]]

        tif1 = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=bt_entries, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        tif2 = make_geotiff(
            "tile2.tif", bands=3, width=16, height=16,
            rat_entries=hsd_entries, rat_fields=HSD_RAT_FIELDS, rat_band=3,
        )
        rel1 = os.path.relpath(tif1, project_dir)
        rel2 = os.path.relpath(tif2, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel1, "rat_disk": rel1},
            {"tilename": "T2", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel2, "rat_disk": rel2},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif1, tif2], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetColumnCount() == 4  # common subset
        assert rat.GetRowCount() == 2
        ds = None

        assert any("catzoc" in r.message for r in caplog.records)
        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_master_config_with_bluetopo_tiles_trims_to_match(self, make_geotiff, tmp_path):
        """Master config (5 fields) with BlueTopo tiles (4 mini fields) -> trims to 4."""
        cfg = get_local_config("TestSource")
        cfg["rat_fields"] = dict(HSD_RAT_FIELDS)  # 5 fields
        project_dir = str(tmp_path)

        bt_entries = [[1, 100, "SURVEY_A", 80]]
        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=bt_entries, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel, "rat_disk": rel},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetColumnCount() == 4  # trimmed to what tile has
        assert rat.GetRowCount() == 1
        ds = None

    def test_exact_config_match_no_trimming(self, make_geotiff, tmp_path, caplog):
        """Config fields exactly match tile fields -> no trimming, no warning."""
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)

        rat_entries = [[1, 100, "SURVEY_A", 80]]
        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries, rat_fields=MINI_RAT_FIELDS, rat_band=3,
        )
        rel = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel, "rat_disk": rel},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetColumnCount() == 4
        ds = None

        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_unknown_extra_fields_in_tile_ignored(self, make_geotiff, tmp_path):
        """Tile has config fields + extra unknown fields -> unknown fields ignored."""
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)

        # Tile has MINI_RAT_FIELDS + "custom_field"
        extended_fields = dict(MINI_RAT_FIELDS)
        extended_fields["custom_field"] = [str, gdal.GFU_Generic]
        rat_entries = [[1, 100, "SURVEY_A", 80, "custom_val"]]

        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries, rat_fields=extended_fields, rat_band=3,
        )
        rel = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel, "rat_disk": rel},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetColumnCount() == 4  # only config fields
        assert rat.GetRowCount() == 1
        ds = None

    def test_minimal_value_count_only(self, make_geotiff, tmp_path):
        """Tile with only value+count (2 of 4 config fields) -> VRT RAT has 2 columns."""
        cfg = _make_bt_cfg()
        project_dir = str(tmp_path)

        minimal_fields = {
            "value": [int, gdal.GFU_MinMax],
            "count": [int, gdal.GFU_PixelCount],
        }
        rat_entries = [[1, 100]]

        tif = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=rat_entries, rat_fields=minimal_fields, rat_band=3,
        )
        rel = os.path.relpath(tif, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel, "rat_disk": rel},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetColumnCount() == 2
        assert rat.GetRowCount() == 1
        ds = None

    def test_dedup_still_works_with_col_map(self, make_geotiff, tmp_path):
        """Dedup by value (col 0) and count summing (col 1) works with col_map."""
        cfg = _make_bt_cfg()
        cfg["rat_fields"] = dict(HSD_RAT_FIELDS)  # 5 fields in config
        project_dir = str(tmp_path)

        # Both tiles have HSD fields, same survey ID -> dedup + sum
        hsd_entries1 = [[1, 100, "SURVEY_A", 80, 3]]
        hsd_entries2 = [[1, 150, "SURVEY_A", 80, 3]]

        tif1 = make_geotiff(
            "tile1.tif", bands=3, width=16, height=16,
            rat_entries=hsd_entries1, rat_fields=HSD_RAT_FIELDS, rat_band=3,
        )
        tif2 = make_geotiff(
            "tile2.tif", bands=3, width=16, height=16,
            rat_entries=hsd_entries2, rat_fields=HSD_RAT_FIELDS, rat_band=3,
        )
        rel1 = os.path.relpath(tif1, project_dir)
        rel2 = os.path.relpath(tif2, project_dir)
        tiles = [
            {"tilename": "T1", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel1, "rat_disk": rel1},
            {"tilename": "T2", "utm": "19",
             "resolution": "2m", "geotiff_disk": rel2, "rat_disk": rel2},
        ]

        vrt_path = str(tmp_path / "utm19.vrt")
        create_vrt([tif1, tif2], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "Contributor"])
        add_vrt_rat(tiles, project_dir, vrt_path, cfg)

        ds = gdal.Open(vrt_path, 0)
        band = ds.GetRasterBand(3)
        rat = band.GetDefaultRAT()
        assert rat.GetRowCount() == 1  # deduped
        assert rat.GetValueAsInt(0, 1) == 250  # 100 + 150
        ds = None
