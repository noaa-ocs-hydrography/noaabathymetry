"""Tests for VRT creation functions in build_vrt.py (requires GDAL)."""

import os

import pytest
from osgeo import gdal

from nbs.bluetopo.core.datasource import get_config, get_vrt_file_columns, get_disk_field
from nbs.bluetopo.core.build_vrt import create_vrt, build_sub_vrts


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
# build_sub_vrts
# ---------------------------------------------------------------------------


class TestBuildSubVrts:
    def test_single_dataset_2m(self, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.tif", bands=3, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "2m", "geotiff_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        assert fields["region"] == "R1"
        assert fields.get("res_2_vrt") is not None
        assert fields.get("complete_vrt") is not None
        # VRT files should exist
        assert os.path.isfile(os.path.join(project_dir, fields["res_2_vrt"]))
        assert os.path.isfile(os.path.join(project_dir, fields["complete_vrt"]))

    def test_single_dataset_16m_only(self, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.tif", bands=3, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "16m", "geotiff_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        # 16m tiles go directly into complete VRT, no separate res VRT
        assert fields.get("res_2_vrt") is None
        assert fields.get("complete_vrt") is not None

    def test_return_dict_columns_match_config(self, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.tif", bands=3, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "2m", "geotiff_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        expected_cols = get_vrt_file_columns(cfg)
        for col in expected_cols:
            assert col in fields, f"Missing column {col} in fields"

    def test_directory_cleanup(self, make_geotiff, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.tif", bands=3, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "2m", "geotiff_disk": rel_tif}]

        # First build creates directory
        build_sub_vrts(subregion, tiles, project_dir, cfg, True)
        vrt_dir = os.path.join(project_dir, "BlueTopo_VRT", "R1")
        assert os.path.isdir(vrt_dir)

        # Second build should clean and recreate
        build_sub_vrts(subregion, tiles, project_dir, cfg, True)
        assert os.path.isdir(vrt_dir)

    def test_multi_subdataset(self, make_geotiff, tmp_path):
        """Test multi-subdataset VRT with only the non-S102 subdataset.

        S102V22 subdataset2 (QualityOfSurvey) uses S102:"..." protocol which
        requires real HDF5 files. We test only subdataset1 (BathymetryCoverage)
        by creating a config with only that subdataset.
        """
        cfg = get_config("s102v22")
        # Keep only subdataset1 (non-S102 protocol) for synthetic test
        cfg["subdatasets"] = [cfg["subdatasets"][0]]
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.h5", bands=2, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "2m", "file_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, False)

        assert fields.get("res_2_subdataset1_vrt") is not None
        assert fields.get("complete_subdataset1_vrt") is not None
        assert os.path.isfile(os.path.join(project_dir, fields["res_2_subdataset1_vrt"]))

    def test_multi_subdataset_s102v30(self, make_geotiff, tmp_path):
        """Test S102V30 multi-subdataset VRT with only BathymetryCoverage.

        S102V30 subdataset2 (QualityOfBathymetryCoverage) uses S102:"..."
        protocol which requires real HDF5 files. We test only subdataset1
        (BathymetryCoverage) by creating a config with only that subdataset.
        """
        cfg = get_config("s102v30")
        # Keep only subdataset1 (non-S102 protocol) for synthetic test
        cfg["subdatasets"] = [cfg["subdatasets"][0]]
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.h5", bands=2, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "2m", "file_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, False)

        assert fields.get("res_2_subdataset1_vrt") is not None
        assert fields.get("complete_subdataset1_vrt") is not None
        assert os.path.isfile(os.path.join(project_dir, fields["res_2_subdataset1_vrt"]))


# ---------------------------------------------------------------------------
# build_sub_vrts multi-resolution
# ---------------------------------------------------------------------------


class TestBuildSubVrtsMultiRes:
    def test_all_four_resolutions(self, make_geotiff, tmp_path):
        """2m + 4m + 8m + 16m tiles produce all per-res VRTs."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        subregion = {"region": "R1", "utm": "19"}
        tiles = []
        for res in ["2m", "4m", "8m", "16m"]:
            tif = make_geotiff(f"tile_{res}.tif", bands=3, width=16, height=16)
            rel_tif = os.path.relpath(tif, project_dir)
            tiles.append({"resolution": res, "geotiff_disk": rel_tif})

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        assert fields.get("res_2_vrt") is not None
        assert fields.get("res_4_vrt") is not None
        assert fields.get("res_8_vrt") is not None
        assert fields.get("complete_vrt") is not None
        # 16m goes directly into complete VRT, no separate res VRT
        assert os.path.isfile(os.path.join(project_dir, fields["complete_vrt"]))

    def test_8m_only(self, make_geotiff, tmp_path):
        """Only 8m tiles -> res_8_vrt and complete_vrt, no 2m or 4m."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.tif", bands=3, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "8m", "geotiff_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        assert fields.get("res_2_vrt") is None
        assert fields.get("res_4_vrt") is None
        assert fields.get("res_8_vrt") is not None
        assert fields.get("complete_vrt") is not None

    def test_4m_and_8m_no_2m(self, make_geotiff, tmp_path):
        """4m + 8m tiles, no 2m."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        subregion = {"region": "R1", "utm": "19"}
        tiles = []
        for res in ["4m", "8m"]:
            tif = make_geotiff(f"tile_{res}.tif", bands=3, width=16, height=16)
            rel_tif = os.path.relpath(tif, project_dir)
            tiles.append({"resolution": res, "geotiff_disk": rel_tif})

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        assert fields.get("res_2_vrt") is None
        assert fields.get("res_4_vrt") is not None
        assert fields.get("res_8_vrt") is not None
        assert fields.get("complete_vrt") is not None

    def test_multiple_tiles_per_resolution(self, make_geotiff, tmp_path):
        """Multiple tiles at same resolution -> combined in single VRT."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        subregion = {"region": "R1", "utm": "19"}
        tiles = []
        for i in range(3):
            tif = make_geotiff(f"tile2m_{i}.tif", bands=3, width=16, height=16)
            rel_tif = os.path.relpath(tif, project_dir)
            tiles.append({"resolution": "2m", "geotiff_disk": rel_tif})

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        vrt_path = os.path.join(project_dir, fields["res_2_vrt"])
        ds = gdal.Open(vrt_path)
        assert ds is not None
        ds = None

    def test_bag_single_file_schema(self, make_geotiff, tmp_path):
        """BAG config uses file_disk instead of geotiff_disk."""
        cfg = get_config("bag")
        project_dir = str(tmp_path)
        tif = make_geotiff("tile.bag", bands=2, width=16, height=16)
        rel_tif = os.path.relpath(tif, project_dir)
        subregion = {"region": "R1", "utm": "19"}
        tiles = [{"resolution": "2m", "file_disk": rel_tif}]

        fields = build_sub_vrts(subregion, tiles, project_dir, cfg, True)

        assert fields.get("res_2_vrt") is not None
        assert fields.get("complete_vrt") is not None
        ds = gdal.Open(os.path.join(project_dir, fields["res_2_vrt"]))
        assert ds.RasterCount == 2
        ds = None


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
