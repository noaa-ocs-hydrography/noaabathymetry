"""
build_vrt.py - Orchestrate GDAL Virtual Raster creation from downloaded tiles.

Thin orchestrator that coordinates:
1. Data source resolution
2. UTM zone discovery and missing VRT detection
3. Per-UTM VRT creation with adaptive overviews
4. RAT aggregation for sources that support it
"""

import concurrent.futures
import datetime
import glob
import logging
import os
import platform
import sqlite3
from dataclasses import dataclass, field

from osgeo import gdal

from nbs.bluetopo._internal.config import (
    get_vrt_built_flags,
    get_all_reset_flags,
    get_utm_file_columns,
    make_resolution_label,
    make_vrt_dir_name,
    make_params_key,
    parse_resolution,
    validate_vrt_resolution_target,
    resolve_data_source,
)

from nbs.bluetopo._internal.db import check_internal_version, connect
from nbs.bluetopo._internal.vrt import (
    add_vrt_rat,
    build_tile_paths,
    compute_overview_factors,
    configure_gdal_for_worker,
    create_vrt,
    ensure_params_rows,
    generate_hillshade,
    missing_utms,
    reproject_to_web_mercator,
    select_tiles_by_utm,
    select_unbuilt_utms,
    update_utm,
)

logger = logging.getLogger("bluetopo")


def _build_utm_zone(project_dir, cfg, data_source, utm, vrt_dir,
                     vrt_dir_name, params_key, relative_to_vrt,
                     vrt_resolution_target, tile_resolution_filter,
                     hillshade, total_workers=1):
    """Build one UTM zone VRT.  Designed to run in a worker process.

    Opens a read-only DB connection for tile selection and RAT
    aggregation (schema migration is already done by the main process).
    Returns a result dict for the main process to handle DB updates.
    """
    if total_workers > 1:
        configure_gdal_for_worker(total_workers)
    db_path = os.path.join(project_dir, f"{cfg['canonical_name'].lower()}_registry.db")
    worker_conn = sqlite3.connect(db_path)
    worker_conn.row_factory = sqlite3.Row
    zone_start = datetime.datetime.now()
    try:
        tiles = select_tiles_by_utm(project_dir, worker_conn, utm, cfg,
                                    tile_resolution_filter=tile_resolution_filter)
        if not tiles:
            return None

        tile_resolutions = {parse_resolution(t.get("resolution")) for t in tiles}
        tile_resolutions.discard(None)

        if cfg["subdatasets"]:
            # Multi-subdataset path (S102V22, S102V30)
            # Compute overview factors once — all subdatasets share the same grid resolution.
            factors = compute_overview_factors(
                resolutions=tile_resolutions,
                vrt_resolution_target=vrt_resolution_target,
                overview_levels=cfg.get("overview_levels"),
                filter_coarsest=cfg.get("overview_filter_coarsest", True),
            )
            sd_vrt_paths = []
            fields = {"utm": utm, "params_key": params_key}
            for sd_idx, sd in enumerate(cfg["subdatasets"]):
                suffix_label = f"_subdataset{sd_idx + 1}"
                tile_paths = build_tile_paths(tiles, project_dir, cfg, sd)
                if not tile_paths:
                    continue
                rel_path = os.path.join(vrt_dir_name,
                                        f"{data_source}_Fetched_UTM{utm}{sd['suffix']}{params_key}.vrt")
                utm_sd_vrt = os.path.join(project_dir, rel_path)
                create_vrt(tile_paths, utm_sd_vrt, factors or None, relative_to_vrt,
                           sd["band_descriptions"], vrt_resolution_target=vrt_resolution_target)
                sd_vrt_paths.append(utm_sd_vrt)
                fields[f"utm{suffix_label}_vrt"] = rel_path
                fields[f"utm{suffix_label}_vrt_disk_file_size"] = os.path.getsize(utm_sd_vrt)
                fields[f"utm{suffix_label}_ovr"] = None
                fields[f"utm{suffix_label}_ovr_disk_file_size"] = None
                ovr_abs = os.path.join(project_dir, rel_path + ".ovr")
                if os.path.isfile(ovr_abs):
                    fields[f"utm{suffix_label}_ovr"] = rel_path + ".ovr"
                    fields[f"utm{suffix_label}_ovr_disk_file_size"] = os.path.getsize(ovr_abs)
                elif factors:
                    raise RuntimeError(
                        f"Overview failed to create for utm{utm}. "
                        "Please try again. If error persists, please contact NBS.")

            rel_combined = os.path.join(vrt_dir_name,
                                        f"{data_source}_Fetched_UTM{utm}{params_key}.vrt")
            utm_combined_vrt = os.path.join(project_dir, rel_combined)
            combined_bands = []
            for sd in cfg["subdatasets"]:
                combined_bands.extend(sd["band_descriptions"])
            create_vrt(sd_vrt_paths, utm_combined_vrt, None, relative_to_vrt,
                       combined_bands, separate=True)
            fields["utm_combined_vrt"] = rel_combined
            fields["utm_combined_vrt_disk_file_size"] = os.path.getsize(utm_combined_vrt)

            # Build metadata
            native_res = vrt_resolution_target if vrt_resolution_target else (min(tile_resolutions) if tile_resolutions else None)
            if native_res is None:
                raise ValueError(
                    f"No parseable tile resolutions found for UTM {utm}. "
                    "Check that tiles have valid resolution metadata."
                )
            tile_count = len(tiles)
            ovw_count = len(factors) if factors else 0
            fields["tile_count"] = tile_count
            fields["tile_count_plus_overviews"] = tile_count * (1 + ovw_count)
            fields["vrt_resolution"] = native_res
            fields["overview_count"] = ovw_count
            fields["overview_resolutions"] = (
                ",".join(str(int(f * native_res)) for f in factors) if factors else None
            )
            fields["built_timestamp"] = datetime.datetime.now().isoformat()
            res_counts = {}
            for t in tiles:
                r = parse_resolution(t.get("resolution"))
                if r is not None:
                    res_counts[r] = res_counts.get(r, 0) + 1
            for res in (2, 4, 8, 16, 32, 64):
                fields[f"tiles_{res}m"] = res_counts.get(res, 0)

            if cfg["has_rat"]:
                add_vrt_rat(tiles, project_dir, utm_combined_vrt, cfg, utm=utm)

            result = {"utm": utm, "fields": fields,
                      "vrt": os.path.join(project_dir, rel_combined), "ovr": None}

            if hillshade:
                logger.info("[UTM%s] Generating hillshade...", utm)
                hs_path = utm_combined_vrt.replace(".vrt", "_hillshade.tif")
                generate_hillshade(utm_combined_vrt, hs_path)
                hs_rel = os.path.relpath(hs_path, project_dir)
                fields["hillshade"] = hs_rel
                fields["hillshade_disk_file_size"] = os.path.getsize(hs_path)
                result["hillshade"] = hs_path
        else:
            # Single-dataset path (BlueTopo, Modeling, BAG, S102V21)
            tile_paths = build_tile_paths(tiles, project_dir, cfg)
            if not tile_paths:
                return None
            factors = compute_overview_factors(
                resolutions=tile_resolutions,
                vrt_resolution_target=vrt_resolution_target,
                overview_levels=cfg.get("overview_levels"),
                filter_coarsest=cfg.get("overview_filter_coarsest", True),
            )
            rel_path = os.path.join(vrt_dir_name,
                                    f"{data_source}_Fetched_UTM{utm}{params_key}.vrt")
            utm_vrt = os.path.join(project_dir, rel_path)
            create_vrt(tile_paths, utm_vrt, factors or None, relative_to_vrt,
                       cfg["band_descriptions"], vrt_resolution_target=vrt_resolution_target)

            if cfg["has_rat"]:
                add_vrt_rat(tiles, project_dir, utm_vrt, cfg, utm=utm)

            fields = {"utm_vrt": rel_path, "utm_ovr": None,
                      "utm": utm, "params_key": params_key}
            fields["utm_vrt_disk_file_size"] = os.path.getsize(utm_vrt)
            fields["utm_ovr_disk_file_size"] = None
            ovr_path = os.path.join(project_dir, rel_path + ".ovr")
            if os.path.isfile(ovr_path):
                fields["utm_ovr"] = rel_path + ".ovr"
                fields["utm_ovr_disk_file_size"] = os.path.getsize(ovr_path)
            elif factors:
                raise RuntimeError(
                    f"Overview failed to create for utm{utm}. "
                    "Please try again. If error persists, please contact NBS.")

            # Build metadata
            native_res = vrt_resolution_target if vrt_resolution_target else (min(tile_resolutions) if tile_resolutions else None)
            if native_res is None:
                raise ValueError(
                    f"No parseable tile resolutions found for UTM {utm}. "
                    "Check that tiles have valid resolution metadata."
                )
            tile_count = len(tiles)
            ovw_count = len(factors) if factors else 0
            fields["tile_count"] = tile_count
            fields["tile_count_plus_overviews"] = tile_count * (1 + ovw_count)
            fields["vrt_resolution"] = native_res
            fields["overview_count"] = ovw_count
            fields["overview_resolutions"] = (
                ",".join(str(int(f * native_res)) for f in factors) if factors else None
            )
            fields["built_timestamp"] = datetime.datetime.now().isoformat()
            res_counts = {}
            for t in tiles:
                r = parse_resolution(t.get("resolution"))
                if r is not None:
                    res_counts[r] = res_counts.get(r, 0) + 1
            for res in (2, 4, 8, 16, 32, 64):
                fields[f"tiles_{res}m"] = res_counts.get(res, 0)

            result = {"utm": utm, "fields": fields,
                      "vrt": utm_vrt, "ovr": fields.get("utm_ovr")}

            if hillshade:
                logger.info("[UTM%s] Generating hillshade...", utm)
                hs_path = utm_vrt.replace(".vrt", "_hillshade.tif")
                generate_hillshade(utm_vrt, hs_path)
                hs_rel = os.path.relpath(hs_path, project_dir)
                fields["hillshade"] = hs_rel
                fields["hillshade_disk_file_size"] = os.path.getsize(hs_path)
                result["hillshade"] = hs_path

        fields["built_hillshade"] = 1 if hillshade else 0
        fields["build_duration_seconds"] = (datetime.datetime.now() - zone_start).total_seconds()
        return result
    finally:
        worker_conn.close()


def _reproject_utm_zone(project_dir, cfg, data_source, utm, vrt_dir,
                         vrt_dir_name, params_key, relative_to_vrt,
                         vrt_resolution_target, tile_resolution_filter,
                         hillshade, total_workers=1):
    """Reproject one UTM zone to EPSG:3857.  Designed to run in a worker process.

    Builds per-resolution VRTs (instant — each has a perfectly aligned
    pixel grid) and warps them together into a single GeoTIFF at the
    finest source resolution.  Coarsest-first source ordering ensures
    finer data overlays coarser in the output.

    Opens a read-only DB connection for tile selection and RAT
    aggregation (schema migration is already done by the main process).
    Returns a result dict for the main process to handle DB updates.
    """
    if total_workers > 1:
        configure_gdal_for_worker(total_workers)
    db_path = os.path.join(project_dir, f"{cfg['canonical_name'].lower()}_registry.db")
    worker_conn = sqlite3.connect(db_path)
    worker_conn.row_factory = sqlite3.Row
    zone_start = datetime.datetime.now()
    try:
        tiles = select_tiles_by_utm(project_dir, worker_conn, utm, cfg,
                                    tile_resolution_filter=tile_resolution_filter)
        if not tiles:
            return None

        tile_resolutions = {parse_resolution(t.get("resolution")) for t in tiles}
        tile_resolutions.discard(None)

        band_descs = cfg.get("band_descriptions")
        if cfg["subdatasets"]:
            band_descs = []
            for sd in cfg["subdatasets"]:
                band_descs.extend(sd["band_descriptions"])

        # Two levels of in-memory VRTs feed into a single warp to GeoTIFF.
        # VRTs are XML metadata with no pixel computation — the warp does
        # all the work in one pass.
        #
        # Level 1 — Per-resolution VRTs: group tiles by resolution so each
        #   VRT has a perfectly aligned pixel grid (no fractional-pixel gaps
        #   between same-resolution tiles).
        # Level 2 — Combined VRT: merges per-resolution VRTs at the finest
        #   resolution with finer data overlaying coarser (source order).
        #   This composites all resolutions onto one aligned grid BEFORE
        #   reprojection, which preserves more fine-resolution data than
        #   warping multiple sources independently (where CRS transform
        #   shifts resolution boundaries and loses edge pixels).
        #
        # The warp then reprojects this single combined VRT to EPSG:3857
        # and writes the GeoTIFF with overviews.
        res_groups = {}
        for tile in tiles:
            res = parse_resolution(tile.get("resolution"))
            res_groups.setdefault(res, []).append(tile)

        vsimem_files = []
        try:
            res_vrts = []
            for res in sorted(res_groups, reverse=True):  # coarsest first
                group_paths = build_tile_paths(res_groups[res], project_dir, cfg)
                if not group_paths:
                    continue
                vrt_path = f"/vsimem/_reproject_UTM{utm}_{res}m.vrt"
                vrt_opts = gdal.BuildVRTOptions(
                    options="-allow_projection_difference -resolution highest -r near")
                vrt = gdal.BuildVRT(vrt_path, group_paths, options=vrt_opts)
                if band_descs:
                    for i, desc in enumerate(band_descs):
                        vrt.GetRasterBand(i + 1).SetDescription(desc)
                vrt = None
                res_vrts.append(vrt_path)
                vsimem_files.append(vrt_path)

            if not res_vrts:
                return None

            # Combine per-resolution VRTs into a single VRT at the finest
            # resolution.  Sources are continuous rasters (not individual
            # tiles), so the combined grid won't straddle tile boundaries.
            target_res = vrt_resolution_target if vrt_resolution_target is not None else min(tile_resolutions)
            combined_vrt = f"/vsimem/_reproject_UTM{utm}_combined.vrt"
            combined_opts = gdal.BuildVRTOptions(
                options=f"-allow_projection_difference -resolution user "
                        f"-tr {target_res} {target_res} -r near")
            vrt = gdal.BuildVRT(combined_vrt, res_vrts, options=combined_opts)
            if band_descs:
                for i, desc in enumerate(band_descs):
                    vrt.GetRasterBand(i + 1).SetDescription(desc)
            vrt = None
            vsimem_files.append(combined_vrt)

            # Warp single combined VRT to EPSG:3857.
            # filter_coarsest=False includes all overview levels since the
            # output is a single GeoTIFF that needs overviews at every scale.
            factors = compute_overview_factors(
                resolutions=tile_resolutions,
                vrt_resolution_target=target_res,
                overview_levels=[16, 32, 64, 128, 256, 512],
                filter_coarsest=False,
            )
            rel_path = os.path.join(vrt_dir_name,
                                    f"{data_source}_Fetched_UTM{utm}{params_key}.tif")
            output_3857 = os.path.join(project_dir, rel_path)
            reproject_to_web_mercator(combined_vrt, output_3857,
                                      overview_factors=factors or None,
                                      target_resolution=target_res)

            # Add RAT
            if cfg["has_rat"]:
                add_vrt_rat(tiles, project_dir, output_3857, cfg, utm=utm)

            result = {"utm": utm, "rel_path": rel_path, "output_path": output_3857}
            result["utm_vrt_disk_file_size"] = os.path.getsize(output_3857)
            ovr_path = output_3857 + ".ovr"
            if os.path.isfile(ovr_path):
                result["utm_ovr"] = rel_path + ".ovr"
                result["utm_ovr_disk_file_size"] = os.path.getsize(ovr_path)
            aux_xml_path = output_3857 + ".aux.xml"
            if os.path.isfile(aux_xml_path):
                result["utm_aux_xml"] = rel_path + ".aux.xml"
                result["utm_aux_xml_disk_file_size"] = os.path.getsize(aux_xml_path)

            # Build metadata — based on final GeoTIFF factors, not intermediary VRTs
            tile_count = len(tiles)
            ovw_count = len(factors) if factors else 0
            result["tile_count"] = tile_count
            result["tile_count_plus_overviews"] = tile_count * (1 + ovw_count)
            result["vrt_resolution"] = target_res
            result["overview_count"] = ovw_count
            result["overview_resolutions"] = (
                ",".join(str(int(f * target_res)) for f in factors) if factors else None
            )
            result["built_timestamp"] = datetime.datetime.now().isoformat()
            res_counts = {}
            for t in tiles:
                r = parse_resolution(t.get("resolution"))
                if r is not None:
                    res_counts[r] = res_counts.get(r, 0) + 1
            for res in (2, 4, 8, 16, 32, 64):
                result[f"tiles_{res}m"] = res_counts.get(res, 0)

            if hillshade:
                logger.info("[UTM%s] Generating hillshade...", utm)
                hs_path = output_3857.replace(".tif", "_hillshade.tif")
                generate_hillshade(output_3857, hs_path)
                result["hillshade"] = hs_path
                result["hillshade_disk_file_size"] = os.path.getsize(hs_path)

            result["built_hillshade"] = 1 if hillshade else 0
            result["build_duration_seconds"] = (datetime.datetime.now() - zone_start).total_seconds()
            return result
        finally:
            for f in vsimem_files:
                try:
                    gdal.Unlink(f)
                except RuntimeError:
                    pass
    finally:
        worker_conn.close()


@dataclass
class BuildResult:
    """Result of a build_vrt operation.

    Attributes
    ----------
    built : list[dict]
        UTM zones that were built. Each dict has ``utm`` and ``vrt`` keys,
        plus ``ovr`` (str or None) for the overview file path.
    skipped : list[str]
        UTM zones that were already up to date or had no tiles after
        resolution filtering.
    failed : list[dict]
        UTM zones that failed during the build. Each dict has
        ``utm`` (str) and ``reason`` (str) keys.
    missing_reset : list[str]
        UTM zones reset due to missing VRT files on disk.
    tile_resolution_filter : list[int] | None
        Resolution filter that was active, or None if unfiltered.
    vrt_resolution_target : float | None
        VRT pixel size override that was active, or None for native resolution.
    """
    built: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    missing_reset: list = field(default_factory=list)
    tile_resolution_filter: list = None
    vrt_resolution_target: float = None


_SYSTEM_FILES = {'.DS_Store', 'Thumbs.db', 'desktop.ini'}


def _verify_dir_absent(project_dir, dir_name):
    """Verify a directory is truly absent before trusting os.path.isdir().

    Attempts to create the directory. If creation fails, the filesystem
    is unreliable (network issue, permissions, dir actually exists) and
    we should not trust earlier os.path.isdir() checks.

    If the directory already exists but is empty (ignoring OS system
    files), it is accepted. If non-empty, raises ValueError.

    Returns immediately without error if verification passes.
    """
    full_path = os.path.join(project_dir, dir_name)
    if os.path.isdir(full_path):
        contents = [f for f in os.listdir(full_path) if f not in _SYSTEM_FILES]
        if contents:
            raise ValueError(
                f"Directory '{dir_name}' reported as absent but actually "
                "exists and is not empty. The filesystem may have returned "
                "incorrect state. Clear the directory or use a different name."
            )
        return
    try:
        os.makedirs(full_path)
        os.rmdir(full_path)  # Clean up — we only needed to test creation
    except OSError as e:
        raise ValueError(
            f"Cannot verify directory '{dir_name}': {e}. "
            "The filesystem may be unreliable — previous directory "
            "existence checks may have returned incorrect results. "
            "No database changes were made."
        ) from e


def _validate_output_dir(project_dir, conn, cfg, params_key, vrt_dir_name):
    """Validate that output_dir is not in conflict with another build config.

    Checks the DB for rows where a different params_key uses the same
    output_dir.  If the conflicting directory still exists on disk, raises
    ValueError.  If the directory was deleted, verifies the filesystem
    is reliable before clearing stale rows to allow reassignment.

    Also handles the case where this params_key previously used a
    different output_dir: if the old dir is gone, resets the rows.
    """
    cursor = conn.cursor()
    all_flags = get_all_reset_flags(cfg)
    utm_cols = get_utm_file_columns(cfg)

    # Check: does this params_key already have a DIFFERENT output_dir?
    cursor.execute(
        "SELECT DISTINCT output_dir FROM vrt_utm "
        "WHERE params_key = ? AND output_dir IS NOT NULL AND output_dir != ?",
        (params_key, vrt_dir_name),
    )
    old_dirs = [row["output_dir"] for row in cursor.fetchall()]
    for old_dir in old_dirs:
        if os.path.isdir(os.path.join(project_dir, old_dir)):
            raise ValueError(
                f"Build configuration already uses directory '{old_dir}'. "
                "Delete it to reassign to a new output directory."
            )
        # Old dir reported gone — verify filesystem before resetting DB
        _verify_dir_absent(project_dir, old_dir)
        set_parts = ["output_dir = ?"] + [f"{col} = NULL" for col in utm_cols]
        for f in all_flags:
            set_parts.append(f"{f} = 0")
        cursor.execute(
            f"UPDATE vrt_utm SET {', '.join(set_parts)} "
            "WHERE params_key = ?",
            (vrt_dir_name, params_key),
        )
        conn.commit()

    # Check: does any OTHER params_key use this output_dir?
    cursor.execute(
        "SELECT DISTINCT params_key FROM vrt_utm "
        "WHERE output_dir = ? AND output_dir IS NOT NULL AND params_key != ?",
        (vrt_dir_name, params_key),
    )
    conflicts = [row["params_key"] for row in cursor.fetchall()]
    for conflict_pk in conflicts:
        if os.path.isdir(os.path.join(project_dir, vrt_dir_name)):
            raise ValueError(
                f"Output directory '{vrt_dir_name}' is already in use by "
                f"a different build configuration. Delete '{vrt_dir_name}' "
                "first to rebuild with your new parameters, or choose a "
                "different output directory."
            )
        # Conflicting dir reported gone — verify filesystem before resetting DB
        _verify_dir_absent(project_dir, vrt_dir_name)
        set_parts = ["output_dir = NULL"] + [f"{col} = NULL" for col in utm_cols]
        for f in all_flags:
            set_parts.append(f"{f} = 0")
        cursor.execute(
            f"UPDATE vrt_utm SET {', '.join(set_parts)} "
            "WHERE params_key = ? AND output_dir = ?",
            (conflict_pk, vrt_dir_name),
        )
        conn.commit()


def build_vrt(project_dir: str, data_source: str = None,
              relative_to_vrt: bool = True,
              vrt_resolution_target: float = None,
              tile_resolution_filter: list = None,
              hillshade: bool = False,
              workers: int = None,
              reproject: bool = False,
              output_dir: str = None,
              debug: bool = False) -> BuildResult:
    """Build a flat GDAL VRT per UTM zone from all source tiles.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        A known source name, a local directory path, or None (defaults to ``"bluetopo"``).
    relative_to_vrt : bool
        Store referenced file paths as relative to the VRT's directory.
    vrt_resolution_target : float | None
        Force output pixel size (in meters).  Must be a positive number.
    tile_resolution_filter : list | None
        Only include tiles at these resolutions (meters).
    hillshade : bool
        If True, generate a hillshade GeoTIFF from the elevation band.
    workers : int | None
        Number of parallel worker processes for building UTM zones.
        None or 1 = sequential.  Must be a positive integer at most
        ``os.cpu_count()``.
    reproject : bool
        If True, reproject to EPSG:3857 (Web Mercator) GeoTIFFs instead
        of building native UTM VRTs.  Uses a temporary VRT as an
        intermediary for correct multi-resolution tile ordering.
    debug : bool
        If True, writes a diagnostic report to the project directory.

    Returns
    -------
    BuildResult
        Structured result with built, skipped, failed, and missing_reset.
    """
    if workers is not None:
        if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
            raise ValueError(
                f"workers must be a positive integer, got {workers!r}")
        max_cpus = os.cpu_count() or 1
        if workers > max_cpus:
            raise ValueError(
                f"workers ({workers}) exceeds available CPUs ({max_cpus})")

    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    cfg, _ = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]

    if reproject and data_source != "BlueTopo":
        raise ValueError(
            "reproject is currently only supported for the BlueTopo data source."
        )

    if int(gdal.VersionInfo()) < cfg["min_gdal_version"]:
        min_ver = cfg["min_gdal_version"]
        raise RuntimeError(
            f"Please update GDAL to >={min_ver // 1000000}.{(min_ver % 1000000) // 10000} "
            "to run build_vrt.\nSome users have encountered issues with "
            "conda's installation of GDAL 3.4. "
            "Try more recent versions of GDAL if you also "
            "encounter issues in your conda environment."
        )

    missing_drivers = [d for d in cfg.get("required_gdal_drivers", [])
                       if gdal.GetDriverByName(d) is None]
    if missing_drivers:
        raise RuntimeError(
            f"GDAL is missing required driver(s) for {data_source}: "
            f"{', '.join(missing_drivers)}. "
            "Reinstall GDAL with HDF5 support to use this data source."
        )

    if not os.path.isdir(project_dir):
        raise ValueError(f"Folder path not found: {project_dir}")

    if not os.path.isfile(os.path.join(project_dir, f"{data_source.lower()}_registry.db")):
        raise ValueError("SQLite database not found. Confirm correct folder. "
                         "Note: fetch_tiles must be run at least once prior to build_vrt")

    if not os.path.isdir(os.path.join(project_dir, data_source)):
        raise ValueError(f"Tile downloads folder not found for {data_source}. "
                         "Confirm correct folder. "
                         "Note: fetch_tiles must be run at least once prior to build_vrt")

    if vrt_resolution_target is not None:
        validate_vrt_resolution_target(vrt_resolution_target)

    if output_dir is not None and ("/" in output_dir or "\\" in output_dir):
        raise ValueError(
            "output_dir must be a single directory name, not a nested path. "
            f"Got: '{output_dir}'"
        )

    report = None
    if debug:
        from nbs.bluetopo._internal.diagnostics import DebugReport
        report = DebugReport(project_dir, data_source, cfg)

    result = BuildResult(tile_resolution_filter=tile_resolution_filter,
                         vrt_resolution_target=vrt_resolution_target)
    try:
        result = _run_build(project_dir, cfg, data_source, relative_to_vrt,
                            vrt_resolution_target, result, report,
                            tile_resolution_filter=tile_resolution_filter,
                            hillshade=hillshade, workers=workers,
                            reproject=reproject, output_dir=output_dir)
    except Exception:
        if report:
            report.capture_exception()
        raise
    finally:
        if report:
            try:
                report.add_result(result)
                report.write()
            finally:
                if report.conn:
                    report.conn.close()
    return result


def _run_build(project_dir, cfg, data_source, relative_to_vrt,
               vrt_resolution_target, result, report=None,
               tile_resolution_filter=None, hillshade=False,
               workers=None, reproject=False, output_dir=None):
    """Core build pipeline, separated so the debug wrapper in build_vrt()
    can handle report lifecycle without re-indenting the main logic.

    Steps: connect DB → seed parameterized rows (if needed) → detect
    missing VRTs → build per-UTM VRTs with overviews and RATs →
    optionally generate hillshade GeoTIFFs.

    When reproject=True, runs an alternative path: builds a temporary
    UTM VRT (no overviews) as an intermediary, then warps to EPSG:3857
    GeoTIFF with overviews and RAT.
    """
    start = datetime.datetime.now()
    logger.info("═══ Begin %s: Building VRTs in %s ═══", data_source, project_dir)

    conn = connect(project_dir, cfg)
    check_internal_version(conn)
    if report:
        report.set_conn(conn)
    try:
        vrt_dir_name = make_vrt_dir_name(data_source, tile_resolution_filter,
                                         vrt_resolution_target, reproject,
                                         output_dir=output_dir)
        params_key = make_params_key(data_source, tile_resolution_filter,
                                     vrt_resolution_target, reproject)

        # Stamp output_dir on any rows that don't have one yet.
        # Always use the auto-generated name so that custom output_dir
        # requests are detected as conflicts by _validate_output_dir.
        auto_vrt_dir_name = f"{data_source}_VRT{params_key}"
        try:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE vrt_utm SET output_dir = ? "
                "WHERE params_key = ? AND output_dir IS NULL",
                (auto_vrt_dir_name, params_key),
            )
            if cursor.rowcount > 0:
                conn.commit()
        except (sqlite3.Error, TypeError):
            pass  # Mocked or unavailable DB — stamp skipped

        if params_key or output_dir:
            if tile_resolution_filter:
                logger.info("Tile resolution filter: %s",
                            make_resolution_label(tile_resolution_filter))
            if vrt_resolution_target is not None:
                logger.info("VRT resolution target: %gm", vrt_resolution_target)
            if reproject:
                logger.info("Reprojecting to EPSG:3857 (Web Mercator)")
            if output_dir:
                logger.info("Output directory: %s", output_dir)
            ensure_params_rows(conn, cfg, params_key, output_dir=vrt_dir_name)

        # Validate output_dir: check for conflicts with other params_keys
        _validate_output_dir(project_dir, conn, cfg, params_key, vrt_dir_name)

        result.missing_reset = missing_utms(project_dir, conn, cfg, params_key)
        if result.missing_reset:
            logger.info("%d utm vrt(s) missing on disk. Added to build list.",
                       len(result.missing_reset))
        utms_to_build = select_unbuilt_utms(conn, cfg, params_key)

        vrt_dir = os.path.join(project_dir, vrt_dir_name)
        os.makedirs(vrt_dir, exist_ok=True)

        # Warn about other VRT directories that may contain stale data
        other_vrt_dirs = [
            d for d in glob.glob(os.path.join(project_dir,
                                              f"{data_source}_VRT*"))
            if os.path.isdir(d) and os.path.basename(d) != vrt_dir_name
        ]
        if other_vrt_dirs:
            logger.info("Note: %d other VRT director(ies) "
                        "exist that may contain stale data:",
                        len(other_vrt_dirs))
            for d in sorted(other_vrt_dirs):
                logger.info("  %s/", os.path.basename(d))

        if utms_to_build:
            # Select the worker function and label based on mode
            if reproject:
                worker_fn = _reproject_utm_zone
                label = "Reprojecting"
                worker_args = lambda utm: (
                    project_dir, cfg, data_source, utm, vrt_dir,
                    vrt_dir_name, params_key, relative_to_vrt,
                    vrt_resolution_target, tile_resolution_filter,
                    hillshade, actual_workers)
            else:
                worker_fn = _build_utm_zone
                label = "Building"
                worker_args = lambda utm: (
                    project_dir, cfg, data_source, utm, vrt_dir,
                    vrt_dir_name, params_key, relative_to_vrt,
                    vrt_resolution_target, tile_resolution_filter,
                    hillshade, actual_workers)

            num_zones = len(utms_to_build)
            use_parallel = workers is not None and workers > 1 and num_zones > 1
            actual_workers = min(workers, num_zones) if use_parallel else 1

            logger.info("%s %d utm zone(s). "
                       "This may take minutes or hours depending on the "
                       "amount of tiles.%s", label, num_zones,
                       f" Using {actual_workers} parallel workers."
                       if use_parallel else "")

            if use_parallel:
                zone_results = []
                with concurrent.futures.ProcessPoolExecutor(
                        max_workers=actual_workers) as executor:
                    futures = {}
                    for ub_utm in utms_to_build:
                        logger.info("[UTM%s] %s...", ub_utm["utm"], label)
                        future = executor.submit(
                            worker_fn, *worker_args(ub_utm["utm"]))
                        futures[future] = ub_utm["utm"]

                    for future in concurrent.futures.as_completed(futures):
                        utm = futures[future]
                        try:
                            zone_result = future.result()
                            if zone_result is not None:
                                zone_results.append(zone_result)
                                logger.info("[UTM%s] Complete", utm)
                        except Exception as e:
                            result.failed.append({"utm": utm, "reason": str(e)})
                            logger.error("[UTM%s] FAILED: %s", utm, e)

                # DB updates sequentially (SQLite single-writer)
                for zone_result in zone_results:
                    utm = zone_result.get("utm") or zone_result.get("fields", {}).get("utm")
                    try:
                        if reproject:
                            fields = {"utm_vrt": zone_result["rel_path"],
                                      "utm": zone_result["utm"],
                                      "params_key": params_key}
                            for key in ("tile_count", "tile_count_plus_overviews",
                                        "vrt_resolution", "overview_count",
                                        "overview_resolutions", "built_timestamp",
                                        "tiles_2m", "tiles_4m", "tiles_8m",
                                        "tiles_16m", "tiles_32m", "tiles_64m",
                                        "build_duration_seconds",
                                        "utm_vrt_disk_file_size",
                                        "utm_ovr", "utm_ovr_disk_file_size",
                                        "utm_aux_xml", "utm_aux_xml_disk_file_size",
                                        "hillshade_disk_file_size",
                                        "built_hillshade"):
                                fields[key] = zone_result.get(key)
                            hs = zone_result.get("hillshade")
                            fields["hillshade"] = (
                                os.path.relpath(hs, project_dir) if hs else None
                            )
                        else:
                            fields = zone_result["fields"]
                        update_utm(conn, fields, cfg)

                        built_entry = {
                            "utm": zone_result["utm"],
                            "vrt": zone_result.get("vrt") or zone_result.get("output_path"),
                            "ovr": zone_result.get("ovr"),
                            "hillshade": zone_result.get("hillshade"),
                        }
                        result.built.append(built_entry)
                    except Exception as e:
                        result.failed.append({"utm": utm, "reason": str(e)})
                        logger.error("[UTM%s] FAILED during DB update: %s",
                                     utm, e)

            else:
                # Sequential processing
                for ub_utm in utms_to_build:
                    utm_start = datetime.datetime.now()
                    utm = ub_utm["utm"]
                    logger.info("[UTM%s] %s...", utm, label)

                    try:
                        zone_result = worker_fn(*worker_args(utm))
                        if zone_result is None:
                            continue

                        # DB update immediately in sequential mode
                        if reproject:
                            fields = {"utm_vrt": zone_result["rel_path"],
                                      "utm": zone_result["utm"],
                                      "params_key": params_key}
                            for key in ("tile_count", "tile_count_plus_overviews",
                                        "vrt_resolution", "overview_count",
                                        "overview_resolutions", "built_timestamp",
                                        "tiles_2m", "tiles_4m", "tiles_8m",
                                        "tiles_16m", "tiles_32m", "tiles_64m",
                                        "build_duration_seconds",
                                        "utm_vrt_disk_file_size",
                                        "utm_ovr", "utm_ovr_disk_file_size",
                                        "utm_aux_xml", "utm_aux_xml_disk_file_size",
                                        "hillshade_disk_file_size",
                                        "built_hillshade"):
                                fields[key] = zone_result.get(key)
                            hs = zone_result.get("hillshade")
                            fields["hillshade"] = (
                                os.path.relpath(hs, project_dir) if hs else None
                            )
                        else:
                            fields = zone_result["fields"]
                        update_utm(conn, fields, cfg)

                        built_entry = {
                            "utm": zone_result["utm"],
                            "vrt": zone_result.get("vrt") or zone_result.get("output_path"),
                            "ovr": zone_result.get("ovr"),
                            "hillshade": zone_result.get("hillshade"),
                        }
                        result.built.append(built_entry)
                        logger.info("[UTM%s] Complete (%s)", utm,
                                    datetime.datetime.now() - utm_start)
                    except Exception as e:
                        result.failed.append({"utm": utm, "reason": str(e)})
                        logger.error("[UTM%s] FAILED: %s", utm, e)
        else:
            if tile_resolution_filter:
                logger.info("All selected UTM zones appear up to date "
                            "(tile resolution filter was applied: %s).",
                            make_resolution_label(tile_resolution_filter))
            else:
                logger.info("All UTM zones appear up to date with the most "
                            "recently fetched tiles.")
            logger.info("Note: deleting the %s folder will "
                        "allow you to recreate from scratch if necessary",
                        vrt_dir_name)

        # Second pass: generate hillshade for already-built zones that lack it
        hillshade_count = 0
        if hillshade:
            # Count hillshades from full builds
            hillshade_count = sum(1 for e in result.built if e.get("hillshade"))

            # Detect missing hillshade files and reset their flags
            vrt_built_flags = get_vrt_built_flags(cfg)
            vrt_built_clause = " AND ".join(f"{f} = 1" for f in vrt_built_flags)
            hs_cursor = conn.cursor()
            hs_cursor.execute(
                f"SELECT utm, hillshade FROM vrt_utm WHERE params_key = ? "
                f"AND ({vrt_built_clause}) AND built_hillshade = 1",
                (params_key,),
            )
            missing_hs = []
            for row in hs_cursor.fetchall():
                hs_path = row["hillshade"]
                if not hs_path or not os.path.isfile(os.path.join(project_dir, hs_path)):
                    missing_hs.append(row["utm"])
            if missing_hs:
                logger.info("%d hillshade(s) missing on disk. Added to build list.",
                            len(missing_hs))
                utm_ph = ", ".join(["?"] * len(missing_hs))
                hs_cursor.execute(
                    f"UPDATE vrt_utm SET hillshade = NULL, hillshade_disk_file_size = NULL, "
                    f"built_hillshade = 0 WHERE params_key = ? AND utm IN ({utm_ph})",
                    [params_key] + missing_hs,
                )
                conn.commit()

            hs_cursor.execute(
                f"SELECT * FROM vrt_utm WHERE params_key = ? "
                f"AND ({vrt_built_clause}) "
                f"AND (built_hillshade IS NULL OR built_hillshade != 1)",
                (params_key,),
            )
            hs_zones = [dict(row) for row in hs_cursor.fetchall()]

            if hs_zones:
                logger.info("Generating hillshade for %d already-built UTM zone(s).",
                            len(hs_zones))
                for hz in hs_zones:
                    utm = hz["utm"]
                    # Determine VRT/GeoTIFF path
                    vrt_col = "utm_combined_vrt" if cfg["subdatasets"] else "utm_vrt"
                    vrt_rel = hz[vrt_col]
                    if not vrt_rel:
                        continue
                    abs_vrt = os.path.join(project_dir, vrt_rel)
                    if vrt_rel.endswith(".tif"):
                        hs_path = abs_vrt.replace(".tif", "_hillshade.tif")
                    else:
                        hs_path = abs_vrt.replace(".vrt", "_hillshade.tif")
                    try:
                        logger.info("[UTM%s] Generating hillshade...", utm)
                        generate_hillshade(abs_vrt, hs_path)
                        hs_rel = os.path.relpath(hs_path, project_dir)
                        hs_cursor.execute(
                            "UPDATE vrt_utm SET hillshade = ?, hillshade_disk_file_size = ?, "
                            "built_hillshade = 1 WHERE utm = ? AND params_key = ?",
                            (hs_rel, os.path.getsize(hs_path), utm, params_key),
                        )
                        conn.commit()
                        hillshade_count += 1
                        logger.info("[UTM%s] Hillshade complete", utm)
                    except Exception as e:
                        result.failed.append({"utm": utm, "reason": f"hillshade: {e}"})
                        logger.error("[UTM%s] Hillshade FAILED: %s", utm, e)

        all_utms_cursor = conn.cursor()
        all_utms_cursor.execute(
            "SELECT utm FROM vrt_utm WHERE params_key = ?", (params_key,))
        all_utm_names = {row["utm"] for row in all_utms_cursor.fetchall()}
        built_utm_names = {e["utm"] for e in result.built}
        failed_utm_names = {f["utm"] for f in result.failed}
        result.skipped = sorted(all_utm_names - built_utm_names - failed_utm_names)

        # Summary
        logger.info("─── SUMMARY ───")
        built_msg = "%d UTM zones successfully built"
        if hillshade_count:
            hs_label = "hillshade" if hillshade_count == 1 else "hillshades"
            built_msg += f" ({hillshade_count} {hs_label})"
        logger.info("Built:   " + built_msg, len(result.built))
        logger.info("Failed:  %d UTM zones failed to build",
                    len(result.failed))
        skipped_reason = ("already up to date or zones with no matching tiles "
                         "after resolution filtering"
                         if tile_resolution_filter else "already up to date")
        logger.info("Skipped: %d UTM zones %s",
                    len(result.skipped), skipped_reason)
        logger.info("Total:   %d UTM zones", len(all_utm_names))

        logger.info("═══ Complete %s: Building VRTs (%s) ═══",
                    data_source, datetime.datetime.now() - start)
    finally:
        if not report:
            conn.close()
    return result
