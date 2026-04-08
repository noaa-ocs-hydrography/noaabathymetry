"""End-to-end pipeline tests that exercise fetch_tiles + mosaic_tiles.

Tests are split into two categories:

- **CI-safe** (no markers) — fully offline, run by default in CI/CD.
  Includes ``TestSyntheticLocal``.
- **Network-dependent** (``@pytest.mark.network``) — require S3 access.
  Excluded by default; run locally before committing with ``-m ""``.
  Long-running network tests also carry ``@pytest.mark.slow``.

Usage (-s shows download progress bars)::

    # CI default — runs offline tests only
    pytest

    # All tests (local dev)
    pytest -m "" -v -s

    # Network tests only
    pytest -m network -v -s

    # Synthetic local tests only (offline)
    pytest -v -s -k "TestSyntheticLocal"

    # Skip slow network tests
    pytest -m "not slow" -v -s
"""

import hashlib
import os
import shutil
import sqlite3

import pytest
from osgeo import gdal, ogr

from nbs.noaabathymetry._internal.config import (
    get_config,
    get_disk_field,
    get_disk_fields,
    get_mosaic_built_flags,
    get_utm_file_columns,
)
from nbs.noaabathymetry._internal.fetcher import fetch_tiles as fetch_main
from nbs.noaabathymetry._internal.db import connect as connect_to_survey_registry
from nbs.noaabathymetry._internal.builder import mosaic_tiles as build_main

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ALL_REMOTE_SOURCES = ["bluetopo", "modeling", "bag", "s102v21", "s102v22", "s102v30"]


# Which tile scheme each source uses (for polygon lookup)
_TILE_SCHEME_FAMILY = {
    "bluetopo": "bluetopo",
    "modeling": "bluetopo",
    "bag": "navigation",
    "s102v21": "navigation",
    "s102v22": "navigation",
    "s102v30": "navigation",
}

# Polygon definitions per tile-scheme family and scenario.
# Each entry: (lon, lat, width, height)
_POLYGONS = {
    "bluetopo": {
        "uniform_single_utm": (-74.95, 38.60, 0.20, 0.20),
        "mixed_res_single_utm": (-76.00, 36.50, 0.70, 0.30),
        "uniform_cross_utm": (-72.25, 41.10, 0.50, 0.50),
        "mixed_res_cross_utm": (-72.20, 41.00, 0.50, 0.50),
    },
    "navigation": {
        "uniform_single_utm": (-74.95, 39.20, 0.25, 0.25),
        "mixed_res_single_utm": (-76.80, 36.45, 1.50, 0.30),
        "uniform_cross_utm": (-72.25, 41.50, 0.50, 0.50),
        "mixed_res_cross_utm": (-72.30, 41.50, 0.50, 0.50),
    },
}

_SCENARIOS = [
    "uniform_single_utm",
    "mixed_res_single_utm",
    "uniform_cross_utm",
    "mixed_res_cross_utm",
]

# Build the full parametrize list: (source, scenario)
_REMOTE_PARAMS = [
    (src, scen) for src in ALL_REMOTE_SOURCES for scen in _SCENARIOS
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _skip_if_gdal_too_old(cfg):
    """Skip the test if GDAL is older than the source requires."""
    if int(gdal.VersionInfo()) < cfg["min_gdal_version"]:
        min_ver = cfg["min_gdal_version"]
        major = min_ver // 1_000_000
        minor = (min_ver % 1_000_000) // 10_000
        pytest.skip(f"GDAL >= {major}.{minor} required for {cfg['canonical_name']}")


def _skip_if_gdal_missing_drivers(cfg):
    """Skip the test if GDAL is missing required drivers for mosaic_tiles."""
    missing = [d for d in cfg.get("required_gdal_drivers", [])
               if gdal.GetDriverByName(d) is None]
    if missing:
        pytest.skip(f"GDAL missing driver(s) {', '.join(missing)} "
                    f"for {cfg['canonical_name']}")


def _get_polygon_args(source, scenario):
    """Return (lon, lat, width, height) for a source+scenario pair."""
    family = _TILE_SCHEME_FAMILY[source]
    return _POLYGONS[family][scenario]


def _expected_band_count(cfg):
    """Return the expected band count for a data source."""
    if cfg["subdatasets"]:
        return sum(len(sd["band_descriptions"]) for sd in cfg["subdatasets"])
    return len(cfg["band_descriptions"])


def _registry_db_path(project_dir, cfg):
    """Return the path to the registry DB."""
    return os.path.join(project_dir, f"{cfg['canonical_name'].lower()}_registry.db")


def _count_tiles_with_disk(project_dir, cfg):
    """Count tiles in the DB that have non-null disk paths."""
    db_path = _registry_db_path(project_dir, cfg)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    disk_field = get_disk_field(cfg)
    cursor.execute(f"SELECT COUNT(*) FROM tiles WHERE {disk_field} IS NOT NULL")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def _get_all_tiles(project_dir, cfg):
    """Return all tile records from the DB."""
    db_path = _registry_db_path(project_dir, cfg)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles")
    tiles = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return tiles


def _get_utm_zones(project_dir, cfg):
    """Return list of UTM zone strings from the DB."""
    db_path = _registry_db_path(project_dir, cfg)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT utm FROM tiles WHERE utm IS NOT NULL")
    zones = [row["utm"] for row in cursor.fetchall()]
    conn.close()
    return zones


# ---------------------------------------------------------------------------
# Assertion helpers
# ---------------------------------------------------------------------------


def assert_fetch_results(project_dir, cfg, successful, failed):
    """Verify post-fetch state."""
    if len(successful) == 0:
        pytest.skip("No tiles were downloaded (S3 may be temporarily unavailable)")

    # Registry DB exists
    assert os.path.isfile(_registry_db_path(project_dir, cfg))

    # Tiles table has records with disk paths
    count = _count_tiles_with_disk(project_dir, cfg)
    assert count >= 1, "Expected at least 1 tile with a disk path in the DB"

    # Downloaded files exist on disk
    tiles = _get_all_tiles(project_dir, cfg)
    disk_fields = get_disk_fields(cfg)
    for tile in tiles:
        for df in disk_fields:
            if tile[df]:
                assert os.path.isfile(os.path.join(project_dir, tile[df])), \
                    f"Tile file missing: {tile[df]}"


def assert_build_results(project_dir, cfg, scenario):
    """Verify post-build state."""
    data_source = cfg["canonical_name"]
    mosaic_dir = os.path.join(project_dir, f"{data_source}_Mosaic")
    assert os.path.isdir(mosaic_dir), f"Mosaic directory missing: {mosaic_dir}"

    # At least 1 UTM mosaic file
    utm_zones = _get_utm_zones(project_dir, cfg)
    assert len(utm_zones) >= 1, "Expected at least 1 UTM zone"

    mosaics_found = []
    for zone in utm_zones:
        vrt_path = os.path.join(mosaic_dir, f"{data_source}_Fetched_UTM{zone}.vrt")
        if os.path.isfile(vrt_path):
            mosaics_found.append(vrt_path)

    assert len(mosaics_found) >= 1, \
        f"Expected at least 1 UTM mosaic in {mosaic_dir}"

    # Cross-UTM scenarios should produce 2+ mosaic files
    if "cross_utm" in scenario:
        assert len(mosaics_found) >= 2, \
            f"Cross-UTM scenario expected 2+ UTM mosaics, got {len(mosaics_found)}"

    # Each mosaic openable by GDAL with correct band count
    expected_bands = _expected_band_count(cfg)
    for vrt_path in mosaics_found:
        ds = gdal.Open(vrt_path)
        assert ds is not None, f"GDAL cannot open mosaic: {vrt_path}"
        assert ds.RasterCount == expected_bands, \
            f"Expected {expected_bands} bands, got {ds.RasterCount} in {vrt_path}"
        ds = None

    # Sources with RAT: verify RAT present on UTM mosaic
    if cfg["has_rat"]:
        for vrt_path in mosaics_found:
            ds = gdal.Open(vrt_path, 0)
            band = ds.GetRasterBand(cfg["rat_band"])
            rat = band.GetDefaultRAT()
            assert rat is not None, f"RAT missing on {vrt_path}"
            assert rat.GetRowCount() > 0, f"RAT has 0 rows on {vrt_path}"
            ds = None


# ---------------------------------------------------------------------------
# Local source helpers
# ---------------------------------------------------------------------------


def setup_local_from_download(project_dir_1, cfg, tmp_path):
    """Create a local_dir from a completed remote download.

    Copies the tile scheme gpkg from project_dir_1 into a new local_dir,
    then modifies link columns via OGR to point to downloaded files'
    absolute paths.

    Returns (local_dir, project_dir_2).
    """
    data_source = cfg["canonical_name"]
    gpkg_fields = cfg["gpkg_fields"]

    # Locate the downloaded gpkg
    tess_dir = os.path.join(project_dir_1, f"{data_source}_Tessellation")
    gpkg_files = [f for f in os.listdir(tess_dir)
                  if f.endswith(".gpkg") and "Tile_Scheme" in f]
    assert len(gpkg_files) >= 1, f"No gpkg found in {tess_dir}"
    gpkg_file = sorted(gpkg_files, reverse=True)[0]

    # Create local_dir and copy gpkg
    local_dir = str(tmp_path / "local_source")
    os.makedirs(local_dir, exist_ok=True)
    local_gpkg = os.path.join(local_dir, f"{data_source}_Tile_Scheme.gpkg")
    shutil.copy(os.path.join(tess_dir, gpkg_file), local_gpkg)

    # Build lookup: tilename -> absolute paths of downloaded files per slot
    tiles = _get_all_tiles(project_dir_1, cfg)
    tile_paths = {}
    for tile in tiles:
        primary_disk = f"{cfg['file_slots'][0]['name']}_disk"
        if tile[primary_disk] is None:
            continue
        paths = {}
        for slot in cfg["file_slots"]:
            name = slot["name"]
            disk_col = f"{name}_disk"
            if tile[disk_col]:
                paths[name] = os.path.join(project_dir_1, tile[disk_col])
        tile_paths[tile["tilename"]] = paths

    # Modify gpkg link columns using OGR (avoids SpatiaLite trigger issues
    # that occur when updating a real gpkg via raw sqlite3)
    ds = ogr.Open(local_gpkg, 1)
    lyr = ds.GetLayer(0)

    tile_field = gpkg_fields["tile"]
    for feat in lyr:
        name = feat.GetField(tile_field)
        if name in tile_paths:
            for slot in cfg["file_slots"]:
                gpkg_link = slot["gpkg_link"]
                slot_name = slot["name"]
                if slot_name in tile_paths[name]:
                    feat.SetField(gpkg_link, tile_paths[name][slot_name])
            lyr.SetFeature(feat)

    ds = None

    project_dir_2 = str(tmp_path / "project_local")
    os.makedirs(project_dir_2, exist_ok=True)

    return local_dir, project_dir_2


def _select_tile_maker(source, make_geotiff, make_bag, make_s102v21, make_s102v22,
                       make_s102v30=None):
    """Return the fixture callable that creates the right file format."""
    makers = {
        "bag": make_bag,
        "s102v21": make_s102v21,
        "s102v22": make_s102v22,
    }
    if make_s102v30 is not None:
        makers["s102v30"] = make_s102v30
    return makers.get(source, make_geotiff)


def setup_synthetic_local(cfg, tmp_path, make_geotiff, make_tile_scheme,
                          make_bag, make_s102v21, make_s102v22,
                          make_s102v30=None):
    """Create a fully synthetic local directory (no S3 required).

    Each source uses its native file format: BAG for BAG, S102 HDF5 for
    S102V21/S102V22/S102V30, and GeoTIFF for BlueTopo/Modeling.

    Returns (local_dir, project_dir).
    """
    data_source = cfg["canonical_name"]
    gpkg_fields = cfg["gpkg_fields"]
    file_slots = cfg["file_slots"]
    source_lower = data_source.lower()
    make_tile = _select_tile_maker(
        source_lower, make_geotiff, make_bag, make_s102v21, make_s102v22,
        make_s102v30)

    # Determine if this source has multiple file slots (e.g. geotiff + rat)
    has_secondary_slots = len(file_slots) > 1

    local_dir = str(tmp_path / "synth_local")
    os.makedirs(local_dir, exist_ok=True)

    # Create 2 synthetic tiles in UTM 19
    tile_infos = []
    for i, res in enumerate(["4m", "8m"]):
        if source_lower in ("bag", "s102v21", "s102v22", "s102v30"):
            # Native HDF5 format -- fixture handles structure
            ext = ".bag" if source_lower == "bag" else ".h5"
            tile_name = f"tile_{res}_{i}{ext}"
            tile_path = make_tile(tile_name, width=16, height=16, utm_zone=19)
            secondary_path = None
        else:
            tif_name = f"tile_{res}_{i}.tif"

            # Build RAT data if source has_rat and method is "direct"
            rat_entries = None
            rat_fields = None
            rat_band_val = None
            if cfg["has_rat"] and cfg["rat_open_method"] == "direct":
                rat_fields = cfg["rat_fields"]
                rat_band_val = cfg["rat_band"]
                row = []
                for fname, (ftype, _) in rat_fields.items():
                    if ftype == int:
                        row.append(1)
                    elif ftype == float:
                        row.append(1.0)
                    else:
                        row.append("test")
                rat_entries = [row]

            bands = len(cfg["band_descriptions"]) if cfg["band_descriptions"] else 2

            tile_path = make_geotiff(
                tif_name, bands=bands, width=16, height=16, utm_zone=19,
                rat_entries=rat_entries, rat_fields=rat_fields,
                rat_band=rat_band_val,
            )

            # Also create secondary file for multi-slot sources (e.g. RAT aux)
            if has_secondary_slots:
                secondary_name = f"tile_{res}_{i}.tif.aux.xml"
                secondary_path = make_geotiff(
                    secondary_name, bands=bands, width=16, height=16, utm_zone=19,
                    rat_entries=rat_entries, rat_fields=rat_fields,
                    rat_band=rat_band_val,
                )
            else:
                secondary_path = None

        # Compute SHA-256 checksums per slot
        slot_paths = [tile_path]
        slot_shas = []
        with open(tile_path, "rb") as f:
            slot_shas.append(hashlib.sha256(f.read()).hexdigest())
        if has_secondary_slots and secondary_path:
            slot_paths.append(secondary_path)
            with open(secondary_path, "rb") as f:
                slot_shas.append(hashlib.sha256(f.read()).hexdigest())
        elif has_secondary_slots:
            slot_paths.append(None)
            slot_shas.append(None)

        tile_infos.append({
            "slot_paths": slot_paths,
            "slot_shas": slot_shas,
            "tile_id": f"SYNTH_{i:04d}",
            "resolution": res,
            "utm": "19",
        })

    # Build tile scheme gpkg -- use gpkg_fields and file_slots for all sources
    # Determine schema type for the make_tile_scheme fixture
    is_navigation = gpkg_fields["tile"] == "TILE_ID"
    schema = "navigation" if is_navigation else "dual_file"

    tiles_for_gpkg = []
    for info in tile_infos:
        tile_dict = {
            gpkg_fields["tile"]: info["tile_id"],
            gpkg_fields["delivered_date"]: "2025-01-01",
            gpkg_fields["resolution"]: info["resolution"],
            gpkg_fields["utm"]: info["utm"],
            "lon": -76.0 + 0.01 * tile_infos.index(info),
            "lat": 37.0,
        }
        # Navigation schema needs extra fields
        if is_navigation:
            tile_dict["REGION"] = "US"
            tile_dict["SUBREGION"] = "TEST"

        # Set link and checksum columns from file_slots
        for j, slot in enumerate(file_slots):
            gpkg_link = slot["gpkg_link"]
            gpkg_checksum = slot.get("gpkg_checksum")
            if j < len(info["slot_paths"]):
                tile_dict[gpkg_link] = info["slot_paths"][j]
            if gpkg_checksum and j < len(info["slot_shas"]):
                tile_dict[gpkg_checksum] = info["slot_shas"][j]
        tiles_for_gpkg.append(tile_dict)

    gpkg_path = make_tile_scheme(
        tiles_for_gpkg,
        name=f"{data_source}_Tile_Scheme.gpkg",
        schema=schema,
    )

    # Copy gpkg to local_dir
    shutil.copy(gpkg_path, os.path.join(local_dir, f"{data_source}_Tile_Scheme.gpkg"))

    project_dir = str(tmp_path / "synth_project")
    os.makedirs(project_dir, exist_ok=True)

    return local_dir, project_dir


# ===========================================================================
# A. Remote Pipeline Tests (20 tests)
# ===========================================================================


@pytest.mark.network
@pytest.mark.slow
class TestRemotePipeline:
    """20 tests: 5 sources x 4 polygon scenarios."""

    @pytest.mark.parametrize("source,scenario", _REMOTE_PARAMS,
                             ids=[f"{s}-{sc}" for s, sc in _REMOTE_PARAMS])
    def test_pipeline(self, source, scenario, tmp_path, make_polygon):
        cfg = get_config(source)
        _skip_if_gdal_too_old(cfg)
        data_source = cfg["canonical_name"]

        lon, lat, width, height = _get_polygon_args(source, scenario)
        polygon = make_polygon(lon=lon, lat=lat, width=width, height=height)

        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)

        # Fetch
        fetch_result = fetch_main(
            project_dir=project_dir,
            geometry=polygon,
            data_source=source,
        )
        assert_fetch_results(project_dir, cfg, fetch_result.downloaded, fetch_result.not_found + [f["tile"] for f in fetch_result.failed])

        # Build (requires GDAL drivers for BAG/S102 formats)
        _skip_if_gdal_missing_drivers(cfg)
        build_main(
            project_dir=project_dir,
            data_source=source,
        )
        assert_build_results(project_dir, cfg, scenario)


# ===========================================================================
# B. Download-Then-Local Tests (5 tests)
# ===========================================================================


@pytest.mark.network
@pytest.mark.slow
class TestDownloadThenLocal:
    """5 tests: download from S3, then reuse as local source."""

    @pytest.mark.parametrize("source", ALL_REMOTE_SOURCES)
    def test_download_then_local(self, source, tmp_path, make_polygon):
        cfg = get_config(source)
        _skip_if_gdal_too_old(cfg)

        # Use uniform_single_utm for the remote fetch
        lon, lat, width, height = _get_polygon_args(source, "uniform_single_utm")
        polygon = make_polygon(lon=lon, lat=lat, width=width, height=height)

        project_dir_1 = str(tmp_path / "project_remote")
        os.makedirs(project_dir_1, exist_ok=True)

        # Remote fetch
        fetch_result = fetch_main(
            project_dir=project_dir_1,
            geometry=polygon,
            data_source=source,
        )
        if len(fetch_result.downloaded) == 0:
            pytest.skip("No tiles downloaded from S3")

        # Setup local source from download
        local_dir, project_dir_2 = setup_local_from_download(
            project_dir_1, cfg, tmp_path,
        )

        # Local fetch — polygon needed to discover tiles via insert_new()
        fetch_result_local = fetch_main(
            project_dir=project_dir_2,
            geometry=polygon,
            data_source=local_dir,
        )

        # Local build (requires GDAL drivers for BAG/S102 formats)
        _skip_if_gdal_missing_drivers(cfg)
        build_main(
            project_dir=project_dir_2,
            data_source=local_dir,
        )

        # Verify tiles were copied
        local_cfg = cfg.copy()
        local_cfg["geom_prefix"] = None
        count = _count_tiles_with_disk(project_dir_2, local_cfg)
        assert count >= 1, "Expected at least 1 tile with disk path in local project"


# ===========================================================================
# B2. Synthetic Local Tests (5 tests)
# ===========================================================================


class TestSyntheticLocal:
    """5 fetch + build tests: create local source from scratch, no S3.

    Each source uses its native file format (BAG, S102 HDF5, or GeoTIFF)
    so GDAL can open them with the correct driver during mosaic build.
    """

    @pytest.mark.parametrize("source", ALL_REMOTE_SOURCES)
    def test_synthetic_local(self, source, tmp_path, make_geotiff,
                             make_tile_scheme, make_polygon, make_bag,
                             make_s102v21, make_s102v22, make_s102v30):
        cfg = get_config(source)
        _skip_if_gdal_too_old(cfg)

        local_dir, project_dir = setup_synthetic_local(
            cfg, tmp_path, make_geotiff, make_tile_scheme,
            make_bag, make_s102v21, make_s102v22, make_s102v30,
        )

        # Polygon covering synthetic tile locations (lon=-76.0, lat=37.0)
        polygon = make_polygon(lon=-76.01, lat=37.01, width=0.04, height=0.04)

        # Local fetch — polygon needed to discover tiles via insert_new()
        fetch_result = fetch_main(
            project_dir=project_dir,
            geometry=polygon,
            data_source=local_dir,
        )

        # Verify tiles were fetched
        from nbs.noaabathymetry._internal.config import get_local_config
        local_cfg = get_local_config(cfg["canonical_name"])
        count = _count_tiles_with_disk(project_dir, local_cfg)
        assert count >= 1, "Expected at least 1 tile with disk path after synthetic local"

        # Local build
        _skip_if_gdal_missing_drivers(cfg)
        build_main(
            project_dir=project_dir,
            data_source=local_dir,
        )


# ===========================================================================
# C. Delete Tile + Refetch Tests (2 tests)
# ===========================================================================


@pytest.mark.network
class TestDeleteTileRefetch:
    """2 tests: delete tile file, verify refetch."""

    @pytest.mark.parametrize("source", ["bluetopo", "bag"])
    def test_refetch_deleted_tile(self, source, tmp_path, make_polygon):
        cfg = get_config(source)
        _skip_if_gdal_too_old(cfg)

        lon, lat, width, height = _get_polygon_args(source, "uniform_single_utm")
        polygon = make_polygon(lon=lon, lat=lat, width=width, height=height)

        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)

        # Initial fetch
        fetch_result = fetch_main(
            project_dir=project_dir,
            geometry=polygon,
            data_source=source,
        )
        if len(fetch_result.downloaded) == 0:
            pytest.skip("No tiles downloaded from S3")

        # Find a tile with files on disk
        tiles = _get_all_tiles(project_dir, cfg)
        disk_fields = get_disk_fields(cfg)
        target_tile = None
        for tile in tiles:
            if all(tile.get(df) and os.path.isfile(os.path.join(project_dir, tile[df]))
                   for df in disk_fields):
                target_tile = tile
                break
        assert target_tile is not None, "No tile with files on disk found"

        # Delete the primary file
        primary_disk = get_disk_field(cfg)
        deleted_path = os.path.join(project_dir, target_tile[primary_disk])
        os.remove(deleted_path)
        assert not os.path.isfile(deleted_path)

        # Re-run fetch (no polygon — just re-download missing tiles)
        fetch_result_2 = fetch_main(
            project_dir=project_dir,
            data_source=source,
        )

        # Verify the file is restored
        assert os.path.isfile(deleted_path), \
            f"Tile file was not restored after refetch: {deleted_path}"


# ===========================================================================
# D. Delete Mosaic + Rebuild Tests (2 tests)
# ===========================================================================


@pytest.mark.network
@pytest.mark.slow
class TestDeleteMosaicRebuild:
    """3 tests: delete mosaic file, verify rebuild."""

    @pytest.mark.parametrize("source", ["bluetopo", "s102v22", "s102v30"])
    def test_rebuild_deleted_mosaic(self, source, tmp_path, make_polygon):
        cfg = get_config(source)
        _skip_if_gdal_too_old(cfg)
        _skip_if_gdal_missing_drivers(cfg)
        data_source = cfg["canonical_name"]

        lon, lat, width, height = _get_polygon_args(source, "uniform_single_utm")
        polygon = make_polygon(lon=lon, lat=lat, width=width, height=height)

        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)

        # Initial fetch + build
        fetch_result = fetch_main(
            project_dir=project_dir,
            geometry=polygon,
            data_source=source,
        )
        if len(fetch_result.downloaded) == 0:
            pytest.skip("No tiles downloaded from S3")

        build_main(
            project_dir=project_dir,
            data_source=source,
        )

        # Find a UTM mosaic file to delete
        mosaic_dir = os.path.join(project_dir, f"{data_source}_Mosaic")
        assert os.path.isdir(mosaic_dir), f"Mosaic directory missing: {mosaic_dir}"

        utm_zones = _get_utm_zones(project_dir, cfg)
        assert len(utm_zones) >= 1

        target_zone = utm_zones[0]
        vrt_path = os.path.join(mosaic_dir, f"{data_source}_Fetched_UTM{target_zone}.vrt")
        assert os.path.isfile(vrt_path), f"UTM mosaic not found: {vrt_path}"

        # Delete the mosaic (and ovr if present)
        os.remove(vrt_path)
        ovr_path = vrt_path + ".ovr"
        if os.path.isfile(ovr_path):
            os.remove(ovr_path)

        # Re-run build
        build_main(
            project_dir=project_dir,
            data_source=source,
        )

        # Verify mosaic is restored
        assert os.path.isfile(vrt_path), \
            f"UTM mosaic was not restored after rebuild: {vrt_path}"
