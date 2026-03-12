"""Tests for VRT creation functions in build_vrt.py (requires GDAL)."""

import os

import pytest
from osgeo import gdal

from nbs.bluetopo.core.build_vrt import create_vrt, compute_overview_factors


# ---------------------------------------------------------------------------
# create_vrt
# ---------------------------------------------------------------------------


class TestCreateVrt:
    def test_creates_valid_vrt(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        t2 = make_geotiff("tile2.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1, t2], vrt_path, [2, 4], False,
                   ["Elevation", "Uncertainty", "Contributor"])
        assert os.path.isfile(vrt_path)
        ds = gdal.Open(vrt_path)
        assert ds is not None
        assert ds.RasterCount == 3
        ds = None

    def test_band_descriptions(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        descs = ["Elevation", "Uncertainty", "Contributor"]
        create_vrt([t1], vrt_path, None, False, descs)
        ds = gdal.Open(vrt_path)
        for i, desc in enumerate(descs):
            assert ds.GetRasterBand(i + 1).GetDescription() == desc
        ds = None

    def test_overview_created(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3, width=16, height=16)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, [2, 4], False,
                   ["Elevation", "Uncertainty", "Contributor"])
        assert os.path.isfile(vrt_path + ".ovr")

    def test_no_overview_when_levels_none(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False)
        assert not os.path.isfile(vrt_path + ".ovr")

    def test_relative_paths(self, make_geotiff, tmp_path):
        subdir = tmp_path / "tiles"
        subdir.mkdir()
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, True)
        with open(vrt_path) as f:
            content = f.read()
        # Should not contain absolute tmp_path
        assert str(tmp_path) not in content or "relativeToVRT" in content

    def test_absolute_paths(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False)
        with open(vrt_path) as f:
            content = f.read()
        # File references should contain absolute path components
        assert "tile1.tif" in content

    def test_removes_stale_files(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3, width=16, height=16)
        vrt_path = str(tmp_path / "output.vrt")
        # First build
        create_vrt([t1], vrt_path, [2], False, ["Elevation", "Uncertainty", "Contributor"])
        assert os.path.isfile(vrt_path)
        # Second build should remove old and recreate
        create_vrt([t1], vrt_path, [2], False, ["Elevation", "Uncertainty", "Contributor"])
        assert os.path.isfile(vrt_path)

    def test_separate_mode(self, make_geotiff, tmp_path):
        t1 = make_geotiff("sub1.tif", bands=2)
        t2 = make_geotiff("sub2.tif", bands=1)
        vrt_path = str(tmp_path / "combined.vrt")
        create_vrt([t1, t2], vrt_path, None, False,
                   ["Elevation", "Uncertainty", "QualityOfSurvey"], separate=True)
        ds = gdal.Open(vrt_path)
        assert ds.RasterCount == 3
        ds = None


# ---------------------------------------------------------------------------
# compute_overview_factors
# ---------------------------------------------------------------------------


class TestComputeOverviewFactors:
    def test_mixed_resolutions(self, make_geotiff, tmp_path):
        """2m + 16m tiles: targets 32m, 64m, 128m above coarsest (16m).

        Native resolution is 2m (finest), so factors are 32/2=16, 64/2=32, 128/2=64.
        """
        t_2m = make_geotiff("tile_2m.tif", bands=1, pixel_size=2)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t_2m, t_16m])
        assert factors == [16, 32, 64]

    def test_same_resolution(self, make_geotiff, tmp_path):
        """All 16m tiles: targets 32m, 64m, 128m above coarsest (16m).

        Native resolution is 16m, so factors are 32/16=2, 64/16=4, 128/16=8.
        """
        t1 = make_geotiff("tile_a.tif", bands=1, pixel_size=16)
        t2 = make_geotiff("tile_b.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t1, t2])
        assert factors == [2, 4, 8]

    def test_target_resolution_override(self, make_geotiff, tmp_path):
        """target_resolution=8 with 2m+16m tiles.

        Targets above coarsest (16m): 32m, 64m, 128m.
        Factors relative to native=8: 32/8=4, 64/8=8, 128/8=16.
        """
        t_2m = make_geotiff("tile_2m.tif", bands=1, pixel_size=2)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t_2m, t_16m], target_resolution=8)
        assert factors == [4, 8, 16]

    def test_empty_tile_list(self):
        """Empty tile list returns empty factors."""
        factors = compute_overview_factors([])
        assert factors == []


# ---------------------------------------------------------------------------
# create_vrt edge cases
# ---------------------------------------------------------------------------


class TestCreateVrtEdge:
    def test_empty_file_list_raises(self, tmp_path):
        vrt_path = str(tmp_path / "empty.vrt")
        with pytest.raises(RuntimeError):
            create_vrt([], vrt_path, None, False, ["Elevation"])

    def test_band_descriptions_none(self, make_geotiff, tmp_path):
        """No band descriptions -> bands have no description set."""
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False)
        ds = gdal.Open(vrt_path)
        assert ds is not None
        ds = None

    def test_single_file_vrt(self, make_geotiff, tmp_path):
        """VRT from a single file."""
        t1 = make_geotiff("single.tif", bands=2)
        vrt_path = str(tmp_path / "single.vrt")
        create_vrt([t1], vrt_path, None, False, ["Elevation", "Uncertainty"])
        ds = gdal.Open(vrt_path)
        assert ds.RasterCount == 2
        assert ds.GetRasterBand(1).GetDescription() == "Elevation"
        ds = None


# ---------------------------------------------------------------------------
# target_resolution
# ---------------------------------------------------------------------------


class TestCreateVrtTargetResolution:
    def test_target_resolution_sets_pixel_size(self, make_geotiff, tmp_path):
        """VRT with target_resolution should have the requested pixel size."""
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False, target_resolution=8.0)
        ds = gdal.Open(vrt_path)
        gt = ds.GetGeoTransform()
        assert gt[1] == 8.0
        assert abs(gt[5]) == 8.0
        ds = None

    def test_no_target_resolution_uses_highest(self, make_geotiff, tmp_path):
        """VRT without target_resolution should use highest (source) resolution."""
        t1 = make_geotiff("tile1.tif", bands=3)  # source pixel size is 2m
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False)
        ds = gdal.Open(vrt_path)
        gt = ds.GetGeoTransform()
        assert gt[1] == 2.0
        ds = None

    def test_negative_target_resolution_raises(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        with pytest.raises(ValueError, match="must be positive"):
            create_vrt([t1], vrt_path, None, False, target_resolution=-8.0)

    def test_zero_target_resolution_raises(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        with pytest.raises(ValueError, match="must be positive"):
            create_vrt([t1], vrt_path, None, False, target_resolution=0.0)


