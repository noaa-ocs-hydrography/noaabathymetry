"""Tests verifying database schema creation and field population after build.

Covers:
- Schema creation with exact column sets for every data source
- Metadata table structure and population
- Field population after mosaic_tiles for single-dataset and multi-subdataset sources
- built_hillshade flag behavior (on/off, default, second-pass)
- Reproject mode field population (BlueTopo only)
- mosaic_resolution_target effect on mosaic_resolution field
- Sequential vs parallel execution producing identical DB state
"""

import os
import shutil
import sqlite3

import pytest
from osgeo import gdal

from nbs.noaabathymetry._internal.config import (
    get_config,
    get_all_reset_flags,
    get_catalog_fields,
    get_tiles_fields,
    get_utm_file_columns,
    get_mosaic_built_flags,
    get_mosaic_fields,
)
from nbs.noaabathymetry._internal.builder import mosaic_tiles
from nbs.noaabathymetry._internal.db import (
    INTERNAL_VERSION,
    check_internal_version,
    connect as connect_to_survey_registry,
)
from nbs.noaabathymetry._internal.mosaic import update_utm, select_unbuilt_utms

MINI_RAT_FIELDS = {
    "value": [int, gdal.GFU_MinMax],
    "count": [int, gdal.GFU_PixelCount],
    "source_survey_id": [str, gdal.GFU_Generic],
    "coverage": [int, gdal.GFU_Generic],
}

ALL_SOURCES = [
    "bluetopo", "modeling", "bag", "s102v21", "s102v22", "s102v30", "hsd",
]


def _skip_if_driver_missing(cfg):
    for d in cfg.get("required_gdal_drivers", []):
        if gdal.GetDriverByName(d) is None:
            pytest.skip(f"GDAL driver '{d}' not available")


def _db_path(project_dir, cfg):
    return os.path.join(project_dir, f"{cfg['canonical_name'].lower()}_registry.db")


def _query_utm_row(project_dir, cfg, utm="19", params_key=""):
    conn = sqlite3.connect(_db_path(project_dir, cfg))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM mosaic_utm WHERE utm = ? AND params_key = ?",
        (utm, params_key),
    )
    row = cur.fetchone()
    result = dict(row) if row else None
    conn.close()
    return result


def _setup_project(make_geotiff, tmp_path, source="bluetopo",
                   resolutions=None, utm="19", sub_dir=None):
    """Create a project directory with tiles and DB ready for mosaic_tiles()."""
    cfg = get_config(source)
    _skip_if_driver_missing(cfg)

    project_dir = str(tmp_path / "project")
    os.makedirs(project_dir)
    data_source = cfg["canonical_name"]
    if sub_dir is None:
        sub_dir = data_source
    tile_dir = os.path.join(project_dir, sub_dir, f"UTM{utm}")
    os.makedirs(tile_dir)

    if resolutions is None:
        resolutions = [("2m", 2), ("4m", 4)]

    conn = connect_to_survey_registry(project_dir, cfg)
    check_internal_version(conn)
    cursor = conn.cursor()

    for i, (res_label, px_size) in enumerate(resolutions):
        tif_name = f"tile_{res_label}_{i}.tif"
        bands = len(cfg["band_descriptions"]) if cfg["band_descriptions"] else 2
        src = make_geotiff(
            tif_name, bands=bands, width=16, height=16,
            utm_zone=int(utm), pixel_size=px_size,
            rat_entries=[[i + 1, 100, f"SURVEY_{i}", 80]],
            rat_fields=MINI_RAT_FIELDS, rat_band=min(3, bands),
        )
        dest = os.path.join(tile_dir, tif_name)
        shutil.copy(src, dest)
        rel = os.path.relpath(dest, project_dir)

        disk_fields = {s["name"] for s in cfg["file_slots"]}
        tile_rec = {"tilename": f"T{i}", "utm": utm, "resolution": res_label}
        for slot in cfg["file_slots"]:
            tile_rec[f"{slot['name']}_disk"] = rel
        cols = ", ".join(tile_rec.keys())
        ph = ", ".join(["?"] * len(tile_rec))
        cursor.execute(
            f"INSERT INTO tiles({cols}) VALUES({ph})",
            list(tile_rec.values()),
        )

    built_flags = get_mosaic_built_flags(cfg)
    flag_cols = ", ".join(built_flags + ["built_hillshade"])
    flag_vals = ", ".join(["0"] * (len(built_flags) + 1))
    cursor.execute(
        f"INSERT INTO mosaic_utm(utm, params_key, {flag_cols}) "
        f"VALUES(?, '', {flag_vals})",
        (utm,),
    )
    conn.commit()
    conn.close()
    return project_dir, cfg


# ---------------------------------------------------------------------------
# Schema creation
# ---------------------------------------------------------------------------


class TestSchemaExact:
    """Verify every expected column exists for all sources."""

    @pytest.mark.parametrize("source", ALL_SOURCES)
    def test_mosaic_utm_columns_exact(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('mosaic_utm')")
        actual = {row[0] for row in cursor.fetchall()}
        expected = set(get_mosaic_fields(cfg).keys())
        assert expected == actual, f"Missing: {expected - actual}, Extra: {actual - expected}"
        conn.close()

    @pytest.mark.parametrize("source", ALL_SOURCES)
    def test_tiles_columns_exact(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('tiles')")
        actual = {row[0] for row in cursor.fetchall()}
        expected = set(get_tiles_fields(cfg).keys())
        assert expected == actual, f"Missing: {expected - actual}, Extra: {actual - expected}"
        conn.close()

    @pytest.mark.parametrize("source", ALL_SOURCES)
    def test_catalog_columns_exact(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        catalog_table = cfg["catalog_table"]
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM pragma_table_info('{catalog_table}')")
        actual = {row[0] for row in cursor.fetchall()}
        expected = set(get_catalog_fields(cfg).keys())
        assert expected == actual, f"Missing: {expected - actual}, Extra: {actual - expected}"
        conn.close()

    @pytest.mark.parametrize("source", ALL_SOURCES)
    def test_metadata_table_columns(self, tmp_path, source):
        cfg = get_config(source)
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM pragma_table_info('metadata')")
        actual = {row[0] for row in cursor.fetchall()}
        assert actual == {"id", "internal_version", "data_source", "initialized"}
        conn.close()


class TestMetadataPopulation:
    def test_metadata_initialized_on_first_connect(self, tmp_path):
        cfg = get_config("bluetopo")
        conn = connect_to_survey_registry(str(tmp_path), cfg)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM metadata WHERE id = 1")
        row = dict(cursor.fetchone())
        assert row["initialized"] is not None
        assert row["data_source"] is not None
        conn.close()

    def test_internal_version_set_after_build(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo")
        conn = sqlite3.connect(_db_path(project_dir, cfg))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT internal_version FROM metadata WHERE id = 1")
        row = cur.fetchone()
        assert row["internal_version"] == INTERNAL_VERSION
        conn.close()


# ---------------------------------------------------------------------------
# Built-flag columns
# ---------------------------------------------------------------------------


class TestBuiltFlagHelpers:
    @pytest.mark.parametrize("source", ALL_SOURCES)
    def test_all_reset_flags_includes_hillshade(self, source):
        cfg = get_config(source)
        flags = get_all_reset_flags(cfg)
        assert "built_hillshade" in flags

    @pytest.mark.parametrize("source", ALL_SOURCES)
    def test_mosaic_built_flags_excludes_hillshade(self, source):
        cfg = get_config(source)
        flags = get_mosaic_built_flags(cfg)
        assert "built_hillshade" not in flags


# ---------------------------------------------------------------------------
# update_utm: built_hillshade behaviour
# ---------------------------------------------------------------------------


class TestUpdateUtmHillshade:
    def test_hillshade_true_sets_flag_1(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[{"utm": "19", "built": 0, "built_hillshade": 0}])
        fields = {"utm": "19", "utm_mosaic": "p.vrt", "built_hillshade": 1}
        update_utm(conn, fields, cfg)
        row = dict(conn.cursor().execute(
            "SELECT * FROM mosaic_utm WHERE utm = '19'").fetchone())
        assert row["built_hillshade"] == 1
        assert row["built"] == 1

    def test_hillshade_false_sets_flag_0(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[{"utm": "19", "built": 0, "built_hillshade": 0}])
        fields = {"utm": "19", "utm_mosaic": "p.vrt", "built_hillshade": 0}
        update_utm(conn, fields, cfg)
        row = dict(conn.cursor().execute(
            "SELECT * FROM mosaic_utm WHERE utm = '19'").fetchone())
        assert row["built_hillshade"] == 0
        assert row["built"] == 1

    def test_missing_hillshade_defaults_to_0(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[{"utm": "19", "built": 0}])
        fields = {"utm": "19", "utm_mosaic": "p.vrt"}
        update_utm(conn, fields, cfg)
        row = dict(conn.cursor().execute(
            "SELECT * FROM mosaic_utm WHERE utm = '19'").fetchone())
        assert row["built_hillshade"] == 0
        assert row["built"] == 1

    def test_hillshade_does_not_affect_unbuilt_selection(self, registry_db):
        """A zone with built=1 and built_hillshade=0 is NOT unbuilt."""
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[
            {"utm": "19", "built": 1, "built_hillshade": 0},
        ])
        assert select_unbuilt_utms(conn, cfg) == []

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset_hillshade_flag(self, registry_db, source):
        cfg = get_config(source)
        conn, _ = registry_db(cfg, utms=[{
            "utm": "19",
            "built_subdataset1": 0, "built_subdataset2": 0,
            "built_combined": 0, "built_hillshade": 0,
        }])
        fields = {
            "utm": "19",
            "utm_subdataset1_mosaic": "s1.vrt", "utm_subdataset1_ovr": None,
            "utm_subdataset2_mosaic": "s2.vrt", "utm_subdataset2_ovr": None,
            "utm_combined_mosaic": "c.vrt",
            "built_hillshade": 1,
        }
        update_utm(conn, fields, cfg)
        row = dict(conn.cursor().execute(
            "SELECT * FROM mosaic_utm WHERE utm = '19'").fetchone())
        assert row["built_subdataset1"] == 1
        assert row["built_subdataset2"] == 1
        assert row["built_combined"] == 1
        assert row["built_hillshade"] == 1


# ---------------------------------------------------------------------------
# Full pipeline: mosaic_tiles field population
# ---------------------------------------------------------------------------


class TestBuildPopulatesSingleDataset:
    """Verify all mosaic_utm fields after mosaic_tiles for a single-dataset source."""

    @pytest.fixture
    def built_row(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path, "bluetopo")
        mosaic_tiles(project_dir, "bluetopo")
        return _query_utm_row(project_dir, cfg), project_dir, cfg

    def test_mosaic_path_populated(self, built_row):
        row, project_dir, _ = built_row
        assert row["utm_mosaic"] is not None
        assert os.path.isfile(os.path.join(project_dir, row["utm_mosaic"]))

    def test_mosaic_disk_file_size_positive(self, built_row):
        row, _, _ = built_row
        assert row["utm_mosaic_disk_file_size"] is not None
        assert row["utm_mosaic_disk_file_size"] > 0

    def test_tile_count(self, built_row):
        row, _, _ = built_row
        assert row["tile_count"] == 2

    def test_tile_count_per_resolution(self, built_row):
        row, _, _ = built_row
        assert row["tiles_2m"] == 1
        assert row["tiles_4m"] == 1
        assert row["tiles_8m"] == 0
        assert row["tiles_16m"] == 0
        assert row["tiles_32m"] == 0
        assert row["tiles_64m"] == 0

    def test_mosaic_resolution_is_native(self, built_row):
        row, _, _ = built_row
        assert row["mosaic_resolution"] == 2.0

    def test_built_timestamp_is_iso(self, built_row):
        row, _, _ = built_row
        assert row["built_timestamp"] is not None
        # Should be parseable as ISO
        assert "T" in row["built_timestamp"]

    def test_build_duration_positive(self, built_row):
        row, _, _ = built_row
        assert row["build_duration_seconds"] is not None
        assert row["build_duration_seconds"] > 0

    def test_built_flag_is_1(self, built_row):
        row, _, _ = built_row
        assert row["built"] == 1

    def test_built_hillshade_is_0_when_off(self, built_row):
        row, _, _ = built_row
        assert row["built_hillshade"] == 0

    def test_hillshade_null_when_off(self, built_row):
        row, _, _ = built_row
        assert row["hillshade"] is None
        assert row["hillshade_disk_file_size"] is None

    def test_overview_metadata(self, built_row):
        row, _, _ = built_row
        assert row["overview_count"] is not None
        assert isinstance(row["overview_count"], int)
        assert row["tile_count_plus_overviews"] is not None


class TestBuildWithHillshade:
    def test_hillshade_fields_populated(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        result = mosaic_tiles(project_dir, "bluetopo", hillshade=True)
        row = _query_utm_row(project_dir, cfg)
        assert row["built_hillshade"] == 1
        assert row["hillshade"] is not None
        assert os.path.isfile(os.path.join(project_dir, row["hillshade"]))
        assert row["hillshade_disk_file_size"] is not None
        assert row["hillshade_disk_file_size"] > 0
        # First-pass hillshade should also appear in result.hillshades
        assert len(result.hillshades) == 1
        assert result.hillshades[0]["utm"] == "19"
        assert os.path.isabs(result.hillshades[0]["hillshade"])
        assert os.path.isfile(result.hillshades[0]["hillshade"])


class TestBuildWithResolutionTarget:
    def test_mosaic_resolution_matches_target(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path,
                                          resolutions=[("2m", 2), ("4m", 4)])
        mosaic_tiles(project_dir, "bluetopo", mosaic_resolution_target=4.0)
        row = _query_utm_row(project_dir, cfg, params_key="_tr4m")
        assert row is not None
        assert row["mosaic_resolution"] == 4.0

    def test_native_resolution_without_target(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path,
                                          resolutions=[("2m", 2), ("4m", 4)])
        mosaic_tiles(project_dir, "bluetopo")
        row = _query_utm_row(project_dir, cfg)
        assert row["mosaic_resolution"] == 2.0


class TestBuildReproject:
    """Reproject mode (BlueTopo only) populates extra fields."""

    def test_reproject_fields(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", reproject=True)
        row = _query_utm_row(project_dir, cfg, params_key="_3857")
        assert row is not None
        # Output is a GeoTIFF, not a VRT
        assert row["utm_mosaic"] is not None
        assert row["utm_mosaic"].endswith(".tif")
        assert os.path.isfile(os.path.join(project_dir, row["utm_mosaic"]))
        assert row["utm_mosaic_disk_file_size"] > 0
        assert row["built"] == 1

    def test_reproject_tile_counts(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", reproject=True)
        row = _query_utm_row(project_dir, cfg, params_key="_3857")
        assert row["tile_count"] == 2
        assert row["tiles_2m"] == 1
        assert row["tiles_4m"] == 1

    def test_reproject_metadata(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", reproject=True)
        row = _query_utm_row(project_dir, cfg, params_key="_3857")
        assert row["built_timestamp"] is not None
        assert row["build_duration_seconds"] > 0
        assert row["mosaic_resolution"] == 2.0
        assert row["built_hillshade"] == 0

    def test_reproject_with_hillshade(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", reproject=True, hillshade=True)
        row = _query_utm_row(project_dir, cfg, params_key="_3857")
        assert row["built_hillshade"] == 1
        assert row["hillshade"] is not None
        assert os.path.isfile(os.path.join(project_dir, row["hillshade"]))
        assert row["hillshade_disk_file_size"] > 0

    def test_reproject_with_resolution_target(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", reproject=True,
                  mosaic_resolution_target=4.0)
        row = _query_utm_row(project_dir, cfg, params_key="_tr4m_3857")
        assert row is not None
        assert row["mosaic_resolution"] == 4.0
        assert row["built"] == 1

    def test_reproject_build_result_paths_are_absolute(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        result = mosaic_tiles(project_dir, "bluetopo", reproject=True)
        assert len(result.built) == 1
        entry = result.built[0]
        # mosaic is always an absolute path
        assert entry["mosaic"] is not None
        assert os.path.isabs(entry["mosaic"])
        assert os.path.isfile(entry["mosaic"])
        # ovr is an absolute path when present
        if entry["ovr"] is not None:
            assert os.path.isabs(entry["ovr"])
            assert os.path.isfile(entry["ovr"])


# ---------------------------------------------------------------------------
# Second-pass hillshade
# ---------------------------------------------------------------------------


class TestSecondPassHillshade:
    """Build without hillshade first, then run with hillshade=True.

    The second run should not rebuild VRTs but should generate hillshade
    for the already-built zone via the second-pass mechanism.
    """

    def test_second_pass_generates_hillshade(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)

        # First build: no hillshade
        result1 = mosaic_tiles(project_dir, "bluetopo")
        assert len(result1.built) == 1
        row = _query_utm_row(project_dir, cfg)
        assert row["built"] == 1
        assert row["built_hillshade"] == 0
        assert row["hillshade"] is None

        # Second build: hillshade=True, but zone already built
        result2 = mosaic_tiles(project_dir, "bluetopo", hillshade=True)
        # Zone was already built so it's skipped (not rebuilt)
        assert len(result2.built) == 0

        # But hillshade should have been generated in the second pass
        row = _query_utm_row(project_dir, cfg)
        assert row["built_hillshade"] == 1
        assert row["hillshade"] is not None
        assert os.path.isfile(os.path.join(project_dir, row["hillshade"]))
        assert row["hillshade_disk_file_size"] > 0

        # MosaicResult.hillshades should capture the second-pass generation
        assert len(result2.hillshades) == 1
        assert result2.hillshades[0]["utm"] == "19"
        assert os.path.isabs(result2.hillshades[0]["hillshade"])
        assert os.path.isfile(result2.hillshades[0]["hillshade"])

    def test_second_pass_detects_missing_hillshade_file(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)

        # Build with hillshade
        mosaic_tiles(project_dir, "bluetopo", hillshade=True)
        row = _query_utm_row(project_dir, cfg)
        assert row["built_hillshade"] == 1
        hs_path = os.path.join(project_dir, row["hillshade"])
        assert os.path.isfile(hs_path)

        # Delete the hillshade file
        os.remove(hs_path)

        # Rebuild with hillshade — should detect and regenerate
        mosaic_tiles(project_dir, "bluetopo", hillshade=True)
        row = _query_utm_row(project_dir, cfg)
        assert row["built_hillshade"] == 1
        assert os.path.isfile(os.path.join(project_dir, row["hillshade"]))


# ---------------------------------------------------------------------------
# Sequential vs parallel
# ---------------------------------------------------------------------------


class TestSequentialVsParallel:
    """Verify sequential and parallel builds produce the same DB state."""

    def _get_comparable_fields(self, row):
        """Return fields that should be identical between seq/parallel runs."""
        exclude = {"built_timestamp", "build_duration_seconds"}
        return {k: v for k, v in row.items() if k not in exclude}

    def test_same_db_state(self, make_geotiff, tmp_path):
        # Sequential build
        seq_dir, cfg = _setup_project(
            make_geotiff, tmp_path / "seq", resolutions=[("2m", 2), ("4m", 4)])
        mosaic_tiles(seq_dir, "bluetopo", workers=1)
        seq_row = _query_utm_row(seq_dir, cfg)

        # Parallel build (workers=2, though only 1 UTM zone)
        par_dir, _ = _setup_project(
            make_geotiff, tmp_path / "par", resolutions=[("2m", 2), ("4m", 4)])
        mosaic_tiles(par_dir, "bluetopo", workers=2)
        par_row = _query_utm_row(par_dir, cfg)

        seq_cmp = self._get_comparable_fields(seq_row)
        par_cmp = self._get_comparable_fields(par_row)

        # Core fields should match
        for key in ("tile_count", "tiles_2m", "tiles_4m", "mosaic_resolution",
                     "overview_count", "built", "built_hillshade"):
            assert seq_cmp[key] == par_cmp[key], f"Mismatch on {key}"

        # Both should have non-null VRT paths
        assert seq_row["utm_mosaic"] is not None
        assert par_row["utm_mosaic"] is not None


# ---------------------------------------------------------------------------
# Multi-subdataset source (S102V22)
# ---------------------------------------------------------------------------


class TestBuildPopulatesMultiSubdataset:
    """Verify field population for S102V22 (multi-subdataset source)."""

    @pytest.fixture
    def s102v22_project(self, make_s102v22, tmp_path):
        source = "s102v22"
        cfg = get_config(source)
        _skip_if_driver_missing(cfg)

        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)
        data_source = cfg["canonical_name"]
        tile_dir = os.path.join(project_dir, data_source, "UTM19")
        os.makedirs(tile_dir)

        conn = connect_to_survey_registry(project_dir, cfg)
        check_internal_version(conn)
        cursor = conn.cursor()

        for i, (res_label, px_size) in enumerate([("2m", 2), ("4m", 4)]):
            h5_name = f"tile_{res_label}_{i}.h5"
            src = make_s102v22(h5_name, width=16, height=16, utm_zone=19)
            dest = os.path.join(tile_dir, h5_name)
            shutil.copy(src, dest)
            rel = os.path.relpath(dest, project_dir)
            cursor.execute(
                "INSERT INTO tiles(tilename, utm, resolution, file_disk) "
                "VALUES(?, ?, ?, ?)",
                (f"T{i}", "19", res_label, rel),
            )

        cursor.execute(
            "INSERT INTO mosaic_utm(utm, params_key, built_subdataset1, "
            "built_subdataset2, built_combined, built_hillshade) "
            "VALUES('19', '', 0, 0, 0, 0)"
        )
        conn.commit()
        conn.close()
        return project_dir, cfg

    def test_subdataset_mosaics_populated(self, s102v22_project):
        project_dir, cfg = s102v22_project
        mosaic_tiles(project_dir, "s102v22")
        row = _query_utm_row(project_dir, cfg)
        assert row["utm_subdataset1_mosaic"] is not None
        assert row["utm_subdataset2_mosaic"] is not None
        assert row["utm_combined_mosaic"] is not None

    def test_subdataset_built_flags(self, s102v22_project):
        project_dir, cfg = s102v22_project
        mosaic_tiles(project_dir, "s102v22")
        row = _query_utm_row(project_dir, cfg)
        assert row["built_subdataset1"] == 1
        assert row["built_subdataset2"] == 1
        assert row["built_combined"] == 1
        assert row["built_hillshade"] == 0

    def test_subdataset_disk_file_sizes(self, s102v22_project):
        project_dir, cfg = s102v22_project
        mosaic_tiles(project_dir, "s102v22")
        row = _query_utm_row(project_dir, cfg)
        assert row["utm_subdataset1_mosaic_disk_file_size"] is not None
        assert row["utm_subdataset1_mosaic_disk_file_size"] > 0
        assert row["utm_combined_mosaic_disk_file_size"] is not None
        assert row["utm_combined_mosaic_disk_file_size"] > 0

    def test_subdataset_metadata(self, s102v22_project):
        project_dir, cfg = s102v22_project
        mosaic_tiles(project_dir, "s102v22")
        row = _query_utm_row(project_dir, cfg)
        assert row["tile_count"] == 2
        assert row["mosaic_resolution"] == 2.0
        assert row["built_timestamp"] is not None
        assert row["build_duration_seconds"] > 0

    def test_subdataset_with_hillshade(self, s102v22_project):
        project_dir, cfg = s102v22_project
        mosaic_tiles(project_dir, "s102v22", hillshade=True)
        row = _query_utm_row(project_dir, cfg)
        assert row["built_hillshade"] == 1
        assert row["hillshade"] is not None
        assert os.path.isfile(os.path.join(project_dir, row["hillshade"]))


# ---------------------------------------------------------------------------
# Other single-dataset sources
# ---------------------------------------------------------------------------


class TestBuildPopulatesBag:
    """Verify BAG source populates DB correctly (no RAT, no subdatasets)."""

    @pytest.fixture
    def bag_project(self, make_bag, tmp_path):
        cfg = get_config("bag")
        _skip_if_driver_missing(cfg)

        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)
        tile_dir = os.path.join(project_dir, cfg["canonical_name"], "UTM19")
        os.makedirs(tile_dir)

        conn = connect_to_survey_registry(project_dir, cfg)
        check_internal_version(conn)
        cursor = conn.cursor()

        for i, (res_label, px_size) in enumerate([("2m", 2), ("4m", 4)]):
            bag_name = f"tile_{res_label}_{i}.bag"
            src = make_bag(bag_name, width=16, height=16, utm_zone=19)
            dest = os.path.join(tile_dir, bag_name)
            shutil.copy(src, dest)
            rel = os.path.relpath(dest, project_dir)
            cursor.execute(
                "INSERT INTO tiles(tilename, utm, resolution, file_disk) "
                "VALUES(?, ?, ?, ?)",
                (f"T{i}", "19", res_label, rel),
            )

        cursor.execute(
            "INSERT INTO mosaic_utm(utm, params_key, built, built_hillshade) "
            "VALUES('19', '', 0, 0)"
        )
        conn.commit()
        conn.close()
        return project_dir, cfg

    def test_bag_build_populates_fields(self, bag_project):
        project_dir, cfg = bag_project
        mosaic_tiles(project_dir, "bag")
        row = _query_utm_row(project_dir, cfg)
        assert row["built"] == 1
        assert row["utm_mosaic"] is not None
        assert row["tile_count"] == 2
        assert row["mosaic_resolution"] == 2.0
        assert row["built_hillshade"] == 0
        assert row["utm_mosaic_disk_file_size"] > 0
        assert row["build_duration_seconds"] > 0


# ---------------------------------------------------------------------------
# Tile resolution filter + params_key isolation
# ---------------------------------------------------------------------------


class TestTileResolutionFilter:
    def test_filter_creates_params_partition(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", tile_resolution_filter=[4])
        row = _query_utm_row(project_dir, cfg, params_key="_4m")
        assert row is not None
        assert row["built"] == 1
        assert row["tiles_4m"] == 1
        assert row["tiles_2m"] == 0
        assert row["tile_count"] == 1

    def test_filter_does_not_affect_default_partition(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        # Build default first
        mosaic_tiles(project_dir, "bluetopo")
        # Then build filtered
        mosaic_tiles(project_dir, "bluetopo", tile_resolution_filter=[4])
        default_row = _query_utm_row(project_dir, cfg)
        filtered_row = _query_utm_row(project_dir, cfg, params_key="_4m")
        # Default has both tiles, filtered has only 4m
        assert default_row["tile_count"] == 2
        assert filtered_row["tile_count"] == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_already_built_zone_skipped(self, make_geotiff, tmp_path):
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        r1 = mosaic_tiles(project_dir, "bluetopo")
        assert len(r1.built) == 1
        r2 = mosaic_tiles(project_dir, "bluetopo")
        assert len(r2.built) == 0
        assert "19" in r2.skipped

    def test_no_tiles_produces_empty_result(self, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)
        os.makedirs(os.path.join(project_dir, "BlueTopo"))
        conn = connect_to_survey_registry(project_dir, cfg)
        check_internal_version(conn)
        conn.cursor().execute(
            "INSERT INTO mosaic_utm(utm, params_key, built, built_hillshade) "
            "VALUES('19', '', 0, 0)")
        conn.commit()
        conn.close()
        result = mosaic_tiles(project_dir, "bluetopo")
        assert len(result.built) == 0

    def test_build_then_rebuild_preserves_hillshade_flag(self, make_geotiff, tmp_path):
        """Build with hillshade, then run without. built_hillshade should stay 1
        because the zone is skipped (already built)."""
        project_dir, cfg = _setup_project(make_geotiff, tmp_path)
        mosaic_tiles(project_dir, "bluetopo", hillshade=True)
        row = _query_utm_row(project_dir, cfg)
        assert row["built_hillshade"] == 1

        # Second run without hillshade — zone is skipped
        mosaic_tiles(project_dir, "bluetopo", hillshade=False)
        row = _query_utm_row(project_dir, cfg)
        # Flag preserved because zone wasn't rebuilt
        assert row["built_hillshade"] == 1
