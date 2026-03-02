"""Full pipeline integration tests using synthetic data (no network).

Creates synthetic GeoTIFFs, populates a registry DB, and runs the
build_vrt pipeline to verify end-to-end VRT creation.
"""

import os

import pytest
from osgeo import gdal

from nbs.bluetopo.core.datasource import (
    get_config,
    get_built_flags,
    get_vrt_file_columns,
    get_utm_file_columns,
)
from nbs.bluetopo.core.build_vrt import (
    connect_to_survey_registry,
    build_sub_vrts,
    create_vrt,
    add_vrt_rat,
    update_subregion,
    update_utm,
    select_unbuilt_subregions,
    select_unbuilt_utms,
    select_tiles_by_subregion,
    select_subregions_by_utm,
    missing_subregions,
)

# Minimal RAT fields for pipeline testing
MINI_RAT_FIELDS = {
    "value": [int, gdal.GFU_MinMax],
    "count": [int, gdal.GFU_PixelCount],
    "source_survey_id": [str, gdal.GFU_Generic],
    "coverage": [int, gdal.GFU_Generic],
}


class TestBluetopoPipeline:
    """End-to-end pipeline test for BlueTopo config with synthetic data."""

    @pytest.fixture
    def pipeline_env(self, make_geotiff, tmp_path):
        """Set up a complete pipeline environment with synthetic tiles."""
        cfg = get_config("bluetopo")
        cfg["rat_fields"] = MINI_RAT_FIELDS
        project_dir = str(tmp_path)

        # Create synthetic tiles in proper directory structure
        tile_dir = os.path.join(project_dir, "BlueTopo", "UTM19")
        os.makedirs(tile_dir, exist_ok=True)

        rat_entries = [
            [1, 100, "SURVEY_A", 80],
            [2, 200, "SURVEY_B", 90],
        ]

        tiles_info = []
        for i, res in enumerate(["2m", "4m"]):
            tif_name = f"tile_{res}_{i}.tif"
            tif = make_geotiff(
                tif_name, bands=3, width=16, height=16,
                rat_entries=rat_entries, rat_fields=MINI_RAT_FIELDS, rat_band=3,
            )
            # Copy to proper directory (include PAM sidecar for RAT)
            import shutil
            dest = os.path.join(tile_dir, tif_name)
            shutil.copy(tif, dest)
            pam = tif + ".aux.xml"
            if os.path.exists(pam):
                shutil.copy(pam, dest + ".aux.xml")
            rel = os.path.relpath(dest, project_dir)
            tiles_info.append({
                "tilename": f"T{i}",
                "subregion": "R1",
                "utm": "19",
                "resolution": res,
                "geotiff_disk": rel,
                "rat_disk": rel,  # Reuse tif as rat for testing
            })

        # Create registry DB
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        for tile in tiles_info:
            cols = ", ".join(tile.keys())
            ph = ", ".join(["?"] * len(tile))
            cursor.execute(f"INSERT INTO tiles({cols}) VALUES({ph})",
                           list(tile.values()))

        # Insert unbuilt subregion and UTM
        cursor.execute(
            "INSERT INTO vrt_subregion(region, utm, built) VALUES(?, ?, ?)",
            ("R1", "19", 0),
        )
        cursor.execute(
            "INSERT INTO vrt_utm(utm, built) VALUES(?, ?)",
            ("19", 0),
        )
        conn.commit()

        return conn, project_dir, cfg, tiles_info

    def test_subregion_vrts_created(self, pipeline_env):
        conn, project_dir, cfg, tiles_info = pipeline_env

        unbuilt = select_unbuilt_subregions(conn, cfg)
        assert len(unbuilt) == 1

        sr = unbuilt[0]
        sr_tiles = select_tiles_by_subregion(project_dir, conn, sr["region"], cfg)
        assert len(sr_tiles) == 2

        fields = build_sub_vrts(sr, sr_tiles, project_dir, cfg, True)
        assert fields["region"] == "R1"
        assert fields.get("res_2_vrt") is not None
        assert fields.get("res_4_vrt") is not None
        assert fields.get("complete_vrt") is not None

        # Verify VRT files exist
        assert os.path.isfile(os.path.join(project_dir, fields["res_2_vrt"]))
        assert os.path.isfile(os.path.join(project_dir, fields["complete_vrt"]))

    def test_subregion_vrt_band_descriptions(self, pipeline_env):
        conn, project_dir, cfg, tiles_info = pipeline_env

        unbuilt = select_unbuilt_subregions(conn, cfg)
        sr = unbuilt[0]
        sr_tiles = select_tiles_by_subregion(project_dir, conn, sr["region"], cfg)
        fields = build_sub_vrts(sr, sr_tiles, project_dir, cfg, True)

        # Check band descriptions on 2m VRT
        vrt_path = os.path.join(project_dir, fields["res_2_vrt"])
        ds = gdal.Open(vrt_path)
        assert ds.RasterCount == 3
        assert ds.GetRasterBand(1).GetDescription() == "Elevation"
        assert ds.GetRasterBand(2).GetDescription() == "Uncertainty"
        assert ds.GetRasterBand(3).GetDescription() == "Contributor"
        ds = None

    def test_update_sets_built_flags(self, pipeline_env):
        conn, project_dir, cfg, tiles_info = pipeline_env

        unbuilt = select_unbuilt_subregions(conn, cfg)
        sr = unbuilt[0]
        sr_tiles = select_tiles_by_subregion(project_dir, conn, sr["region"], cfg)
        fields = build_sub_vrts(sr, sr_tiles, project_dir, cfg, True)
        update_subregion(conn, fields, cfg)

        # Verify built flag
        unbuilt = select_unbuilt_subregions(conn, cfg)
        assert len(unbuilt) == 0

    def test_utm_vrt_created(self, pipeline_env):
        conn, project_dir, cfg, tiles_info = pipeline_env

        # Build subregion first
        unbuilt_sr = select_unbuilt_subregions(conn, cfg)
        sr = unbuilt_sr[0]
        sr_tiles = select_tiles_by_subregion(project_dir, conn, sr["region"], cfg)
        fields = build_sub_vrts(sr, sr_tiles, project_dir, cfg, True)
        update_subregion(conn, fields, cfg)

        # Build UTM
        unbuilt_utms = select_unbuilt_utms(conn, cfg)
        assert len(unbuilt_utms) == 1

        utm = unbuilt_utms[0]
        subregions = select_subregions_by_utm(project_dir, conn, utm["utm"], cfg)
        vrt_list = [os.path.join(project_dir, s["complete_vrt"]) for s in subregions]

        vrt_dir = os.path.join(project_dir, "BlueTopo_VRT")
        os.makedirs(vrt_dir, exist_ok=True)
        rel_path = os.path.join("BlueTopo_VRT", "BlueTopo_Fetched_UTM19.vrt")
        utm_vrt = os.path.join(project_dir, rel_path)
        create_vrt(vrt_list, utm_vrt, [32, 64], True, cfg["band_descriptions"])

        assert os.path.isfile(utm_vrt)
        assert os.path.isfile(utm_vrt + ".ovr")

    def test_rat_attached_to_utm_vrt(self, pipeline_env):
        conn, project_dir, cfg, tiles_info = pipeline_env

        # Build subregion
        unbuilt_sr = select_unbuilt_subregions(conn, cfg)
        sr = unbuilt_sr[0]
        sr_tiles = select_tiles_by_subregion(project_dir, conn, sr["region"], cfg)
        fields = build_sub_vrts(sr, sr_tiles, project_dir, cfg, True)
        update_subregion(conn, fields, cfg)

        # Build UTM VRT
        subregions = select_subregions_by_utm(project_dir, conn, "19", cfg)
        vrt_list = [os.path.join(project_dir, s["complete_vrt"]) for s in subregions]
        vrt_dir = os.path.join(project_dir, "BlueTopo_VRT")
        os.makedirs(vrt_dir, exist_ok=True)
        utm_vrt = os.path.join(project_dir, "BlueTopo_VRT", "BlueTopo_Fetched_UTM19.vrt")
        create_vrt(vrt_list, utm_vrt, [32, 64], True, cfg["band_descriptions"])

        # Add RAT
        add_vrt_rat(conn, "19", project_dir, utm_vrt, cfg)

        ds = gdal.Open(utm_vrt, 0)
        band = ds.GetRasterBand(cfg["rat_band"])
        rat = band.GetDefaultRAT()
        assert rat is not None
        assert rat.GetRowCount() > 0
        # Check field names
        field_names = [rat.GetNameOfCol(i) for i in range(rat.GetColumnCount())]
        for expected_field in cfg["rat_fields"]:
            assert expected_field in field_names
        ds = None

    def test_rebuild_after_deletion(self, pipeline_env):
        conn, project_dir, cfg, tiles_info = pipeline_env

        # Build subregion
        unbuilt_sr = select_unbuilt_subregions(conn, cfg)
        sr = unbuilt_sr[0]
        sr_tiles = select_tiles_by_subregion(project_dir, conn, sr["region"], cfg)
        fields = build_sub_vrts(sr, sr_tiles, project_dir, cfg, True)
        update_subregion(conn, fields, cfg)

        # Verify built
        assert len(select_unbuilt_subregions(conn, cfg)) == 0

        # Delete the complete VRT
        complete_path = os.path.join(project_dir, fields["complete_vrt"])
        os.remove(complete_path)

        # missing_subregions should detect and reset
        count = missing_subregions(project_dir, conn, cfg)
        assert count == 1

        # Should be unbuilt again
        assert len(select_unbuilt_subregions(conn, cfg)) == 1


class TestNoTilesPipeline:
    """Pipeline behavior when no tiles exist."""

    def test_empty_subregion_skipped(self, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO vrt_subregion(region, utm, built) VALUES(?, ?, ?)",
            ("R1", "19", 0),
        )
        conn.commit()

        sr_tiles = select_tiles_by_subregion(project_dir, conn, "R1", cfg)
        assert len(sr_tiles) == 0
