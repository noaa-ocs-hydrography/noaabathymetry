"""Tests for VRT creation functions in build_vrt.py (requires GDAL)."""

import os

import pytest
from osgeo import gdal

from nbs.bluetopo._internal.config import get_config
from nbs.bluetopo._internal.vrt import create_vrt, compute_overview_factors, select_tiles_by_utm


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
    LEVELS = [8, 16, 32, 64, 128]

    def test_mixed_resolutions(self, make_geotiff, tmp_path):
        """2m + 16m tiles: targets above coarsest (16m) = 32, 64, 128.

        Factors relative to native 2m: 32/2=16, 64/2=32, 128/2=64.
        """
        t_2m = make_geotiff("tile_2m.tif", bands=1, pixel_size=2)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t_2m, t_16m],
                                          overview_levels=self.LEVELS)
        assert factors == [16, 32, 64]

    def test_mixed_4m_16m(self, make_geotiff, tmp_path):
        """4m + 16m tiles: targets above coarsest (16m) = 32, 64, 128.

        Factors relative to native 4m: 32/4=8, 64/4=16, 128/4=32.
        """
        t_4m = make_geotiff("tile_4m.tif", bands=1, pixel_size=4)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t_4m, t_16m],
                                          overview_levels=self.LEVELS)
        assert factors == [8, 16, 32]

    def test_all_4m_tiles(self, make_geotiff, tmp_path):
        """All 4m tiles: coarsest is 4m, targets above = 8, 16, 32, 64, 128.

        Factors relative to 4m: 2, 4, 8, 16, 32.
        """
        t1 = make_geotiff("tile_a.tif", bands=1, pixel_size=4)
        t2 = make_geotiff("tile_b.tif", bands=1, pixel_size=4)
        factors = compute_overview_factors([t1, t2],
                                          overview_levels=self.LEVELS)
        assert factors == [2, 4, 8, 16, 32]

    def test_same_resolution_16m(self, make_geotiff, tmp_path):
        """All 16m tiles: targets above coarsest (16m) = 32, 64, 128.

        Factors relative to 16m: 2, 4, 8.
        """
        t1 = make_geotiff("tile_a.tif", bands=1, pixel_size=16)
        t2 = make_geotiff("tile_b.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t1, t2],
                                          overview_levels=self.LEVELS)
        assert factors == [2, 4, 8]

    def test_vrt_resolution_target_override(self, make_geotiff, tmp_path):
        """vrt_resolution_target=8 with 2m+16m tiles.

        Targets above coarsest (16m): 32, 64, 128.
        Factors relative to native=8: 32/8=4, 64/8=8, 128/8=16.
        """
        t_2m = make_geotiff("tile_2m.tif", bands=1, pixel_size=2)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors([t_2m, t_16m], vrt_resolution_target=8,
                                          overview_levels=self.LEVELS)
        assert factors == [4, 8, 16]

    def test_empty_tile_list(self):
        """Empty tile list returns empty factors."""
        factors = compute_overview_factors([], overview_levels=self.LEVELS)
        assert factors == []

    def test_none_overview_levels_raises(self):
        """overview_levels=None raises ValueError."""
        with pytest.raises(ValueError, match="overview_levels must be provided"):
            compute_overview_factors([])

    def test_filtered(self, make_geotiff, tmp_path):
        """filter_coarsest=True with 4m+16m: targets above 16m = [32,64,128].

        Factors relative to 4m: 32/4=8, 64/4=16, 128/4=32.
        """
        t_4m = make_geotiff("tile_4m.tif", bands=1, pixel_size=4)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors(
            [t_4m, t_16m],
            overview_levels=self.LEVELS,
            filter_coarsest=True,
        )
        assert factors == [8, 16, 32]

    def test_unfiltered(self, make_geotiff, tmp_path):
        """filter_coarsest=False with 4m+16m: all levels are candidates.

        Factors relative to 4m: 8/4=2, 16/4=4, 32/4=8, 64/4=16, 128/4=32.
        """
        t_4m = make_geotiff("tile_4m.tif", bands=1, pixel_size=4)
        t_16m = make_geotiff("tile_16m.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors(
            [t_4m, t_16m],
            overview_levels=self.LEVELS,
            filter_coarsest=False,
        )
        assert factors == [2, 4, 8, 16, 32]

    def test_filtered_single_res(self, make_geotiff, tmp_path):
        """filter_coarsest=True with all 4m: targets above 4m = [8,16,32,64,128].

        Factors relative to 4m: 2, 4, 8, 16, 32.
        """
        t1 = make_geotiff("tile_a.tif", bands=1, pixel_size=4)
        t2 = make_geotiff("tile_b.tif", bands=1, pixel_size=4)
        factors = compute_overview_factors(
            [t1, t2],
            overview_levels=self.LEVELS,
            filter_coarsest=True,
        )
        assert factors == [2, 4, 8, 16, 32]

    def test_filtered_only_16m(self, make_geotiff, tmp_path):
        """filter_coarsest=True with all 16m: targets above 16m = [32,64,128].

        Factors relative to 16m: 2, 4, 8.
        """
        t1 = make_geotiff("tile_a.tif", bands=1, pixel_size=16)
        t2 = make_geotiff("tile_b.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors(
            [t1, t2],
            overview_levels=self.LEVELS,
            filter_coarsest=True,
        )
        assert factors == [2, 4, 8]

    def test_unfiltered_only_16m(self, make_geotiff, tmp_path):
        """filter_coarsest=False with all 16m: all levels, but factor < 2 filtered.

        Factors relative to 16m: 8/16<1(skip), 16/16=1(skip), 32/16=2, 64/16=4, 128/16=8.
        """
        t1 = make_geotiff("tile_a.tif", bands=1, pixel_size=16)
        t2 = make_geotiff("tile_b.tif", bands=1, pixel_size=16)
        factors = compute_overview_factors(
            [t1, t2],
            overview_levels=self.LEVELS,
            filter_coarsest=False,
        )
        assert factors == [2, 4, 8]


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
# vrt_resolution_target
# ---------------------------------------------------------------------------


class TestCreateVrtResolutionTarget:
    def test_vrt_resolution_target_sets_pixel_size(self, make_geotiff, tmp_path):
        """VRT with vrt_resolution_target should have the requested pixel size."""
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False, vrt_resolution_target=8.0)
        ds = gdal.Open(vrt_path)
        gt = ds.GetGeoTransform()
        assert gt[1] == 8.0
        assert abs(gt[5]) == 8.0
        ds = None

    def test_no_vrt_resolution_target_uses_highest(self, make_geotiff, tmp_path):
        """VRT without vrt_resolution_target should use highest (source) resolution."""
        t1 = make_geotiff("tile1.tif", bands=3)  # source pixel size is 2m
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False)
        ds = gdal.Open(vrt_path)
        gt = ds.GetGeoTransform()
        assert gt[1] == 2.0
        ds = None

    def test_negative_vrt_resolution_target_raises(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        with pytest.raises(ValueError, match="must be a positive number"):
            create_vrt([t1], vrt_path, None, False, vrt_resolution_target=-8.0)

    def test_zero_vrt_resolution_target_raises(self, make_geotiff, tmp_path):
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        with pytest.raises(ValueError, match="must be a positive number"):
            create_vrt([t1], vrt_path, None, False, vrt_resolution_target=0.0)

    def test_nonstandard_vrt_resolution_target(self, make_geotiff, tmp_path):
        """Non-standard resolution like 5m should work."""
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False, vrt_resolution_target=5.0)
        ds = gdal.Open(vrt_path)
        gt = ds.GetGeoTransform()
        assert gt[1] == 5.0
        ds = None

    def test_fractional_vrt_resolution_target(self, make_geotiff, tmp_path):
        """Fractional resolution like 0.5m should work."""
        t1 = make_geotiff("tile1.tif", bands=3)
        vrt_path = str(tmp_path / "output.vrt")
        create_vrt([t1], vrt_path, None, False, vrt_resolution_target=0.5)
        ds = gdal.Open(vrt_path)
        gt = ds.GetGeoTransform()
        assert gt[1] == 0.5
        ds = None


# ---------------------------------------------------------------------------
# select_tiles_by_utm with resolution filter
# ---------------------------------------------------------------------------


class TestSelectTilesByUtmResolution:
    def _setup(self, registry_db, make_geotiff, tmp_path):
        """Create a DB with tiles at different resolutions and files on disk."""
        cfg = get_config("bluetopo")
        tiles = [
            {
                "tilename": "tile_4m", "delivered_date": "2024-01-01",
                "resolution": "4m", "utm": "19",
                "geotiff_disk": "BlueTopo/tile_4m.tif",
                "rat_disk": "BlueTopo/tile_4m.tif.aux.xml",
                "geotiff_link": "s3://link/tile_4m.tif",
                "rat_link": "s3://link/tile_4m.tif.aux.xml",
                "geotiff_sha256_checksum": "abc", "rat_sha256_checksum": "abc",
                "geotiff_verified": 1, "rat_verified": 1,
            },
            {
                "tilename": "tile_8m", "delivered_date": "2024-01-01",
                "resolution": "8m", "utm": "19",
                "geotiff_disk": "BlueTopo/tile_8m.tif",
                "rat_disk": "BlueTopo/tile_8m.tif.aux.xml",
                "geotiff_link": "s3://link/tile_8m.tif",
                "rat_link": "s3://link/tile_8m.tif.aux.xml",
                "geotiff_sha256_checksum": "abc", "rat_sha256_checksum": "abc",
                "geotiff_verified": 1, "rat_verified": 1,
            },
            {
                "tilename": "tile_16m", "delivered_date": "2024-01-01",
                "resolution": "16m", "utm": "19",
                "geotiff_disk": "BlueTopo/tile_16m.tif",
                "rat_disk": "BlueTopo/tile_16m.tif.aux.xml",
                "geotiff_link": "s3://link/tile_16m.tif",
                "rat_link": "s3://link/tile_16m.tif.aux.xml",
                "geotiff_sha256_checksum": "abc", "rat_sha256_checksum": "abc",
                "geotiff_verified": 1, "rat_verified": 1,
            },
        ]
        conn, project_dir = registry_db(cfg, tiles=tiles,
                                        utms=[{"utm": "19", "built": 0}])
        # Create files on disk
        bt_dir = os.path.join(project_dir, "BlueTopo")
        os.makedirs(bt_dir, exist_ok=True)
        for t in tiles:
            make_geotiff(os.path.join("BlueTopo", os.path.basename(t["geotiff_disk"])), bands=3)
            # Create the RAT aux file
            rat_path = os.path.join(project_dir, t["rat_disk"])
            with open(rat_path, "w") as f:
                f.write("<PAMDataset/>")
        return conn, project_dir, cfg

    def test_filter_to_subset(self, registry_db, make_geotiff, tmp_path):
        conn, project_dir, cfg = self._setup(registry_db, make_geotiff, tmp_path)
        result = select_tiles_by_utm(project_dir, conn, "19", cfg, tile_resolution_filter=[4, 8])
        names = [t["tilename"] for t in result]
        assert "tile_4m" in names
        assert "tile_8m" in names
        assert "tile_16m" not in names

    def test_filter_no_matches(self, registry_db, make_geotiff, tmp_path):
        conn, project_dir, cfg = self._setup(registry_db, make_geotiff, tmp_path)
        result = select_tiles_by_utm(project_dir, conn, "19", cfg, tile_resolution_filter=[32])
        assert result == []

    def test_no_filter_returns_all(self, registry_db, make_geotiff, tmp_path):
        conn, project_dir, cfg = self._setup(registry_db, make_geotiff, tmp_path)
        result = select_tiles_by_utm(project_dir, conn, "19", cfg, tile_resolution_filter=None)
        assert len(result) == 3


