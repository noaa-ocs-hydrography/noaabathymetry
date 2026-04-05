"""
fetch_tiles.py - Orchestrate NBS tile discovery and download.

Thin orchestrator that coordinates:
1. Data source resolution
2. Tessellation and XML catalog download
3. Geometry intersection for tile discovery
4. Tile synchronization with latest tilescheme
5. Parallel tile download with checksum verification
"""

import datetime
import logging
import os
import platform
from dataclasses import dataclass, field

from nbs.noaabathymetry._internal.config import (
    make_resolution_label,
    parse_resolution,
    resolve_data_source,
)
from nbs.noaabathymetry._internal.db import check_internal_version, connect
from nbs.noaabathymetry._internal.download import (
    _get_s3_client,
    all_db_tiles,
    build_download_plan,
    classify_tiles,
    execute_downloads,
    get_tessellation,
    get_xml,
    insert_new,
    update_records,
    upsert_tiles,
)
from nbs.noaabathymetry._internal.spatial import get_tile_list, parse_geometry_input

logger = logging.getLogger("noaabathymetry")


@dataclass
class FetchResult:
    """Result of a fetch_tiles operation.

    Attributes
    ----------
    downloaded : list[str]
        Tiles successfully downloaded in this run.
    failed : list[dict]
        Tiles that failed download. Each dict has ``tile`` and ``reason`` keys.
    not_found : list[str]
        Tiles whose files could not be located on S3.
    existing : list[str]
        Tiles already downloaded, verified, and up to date.
    filtered_out : list[str]
        Tiles excluded by the resolution filter.
    missing_reset : list[str]
        Tiles previously downloaded but missing from disk.
    available_tiles_intersecting_aoi : int
        Number of tiles with valid metadata intersecting the area of
        interest geometry.  Includes tiles already tracked.
    new_tiles_tracked : int
        Number of tiles actually newly added to tracking in this run.
        Tiles already in the database are not counted.
    tile_resolution_filter : list[int] | None
        Resolution filter that was active, or None if unfiltered.
    """
    downloaded: list = field(default_factory=list)
    failed: list = field(default_factory=list)
    not_found: list = field(default_factory=list)
    existing: list = field(default_factory=list)
    filtered_out: list = field(default_factory=list)
    missing_reset: list = field(default_factory=list)
    available_tiles_intersecting_aoi: int = 0
    new_tiles_tracked: int = 0
    tile_resolution_filter: list = None


def _fetch_impl(
    project_dir: str,
    geometry: str = None,
    data_source: str = None,
    tile_resolution_filter: list = None,
    debug: bool = False,
) -> FetchResult:
    """Discover, download, and update NBS tiles.

    Orchestrates the full fetch workflow:

    1. Resolve data source config (named source or local directory).
    2. Download tessellation geopackage and optional XML catalog.
    3. If a geometry is provided, intersect with tile scheme to discover tiles.
    4. Synchronize tile records with the latest tilescheme deliveries.
    5. Download all pending tiles with checksum verification.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.  Created if it does not exist.
    geometry : str | None
        Geometry input defining the area of interest.  Accepts a file path,
        bounding box (``xmin,ymin,xmax,ymax``), WKT, or GeoJSON string.
        String inputs assume EPSG:4326.  Pass None to skip discovery.
    data_source : str | None
        A known source name (e.g. ``"bluetopo"``, ``"bag"``, ``"s102v30"``),
        a local directory path, or None (defaults to ``"bluetopo"``).
    tile_resolution_filter : list | None
        Only fetch tiles at these resolutions (meters).
    debug : bool
        If True, writes a diagnostic report to the project directory.

    Returns
    -------
    FetchResult
        Structured result with downloaded, failed, not_found, existing, filtered_out, and missing_reset tiles.
    """
    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)
    if geometry:
        _is_path_like = (
            os.path.sep in geometry
            or geometry.startswith("~")
            or os.path.isfile(geometry)
        )
        if _is_path_like:
            geometry = os.path.expanduser(geometry)
            if not os.path.isabs(geometry):
                msg = "Please use an absolute path for your geometry path."
                if "windows" not in platform.system().lower():
                    msg += "\nTypically for non windows systems this means starting with '/'"
                raise ValueError(msg)

    cfg, local_dir = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]
    geom_prefix = local_dir or cfg["geom_prefix"]
    bucket = cfg["bucket"]

    db_path = os.path.join(project_dir, f"{data_source.lower()}_registry.db")
    if not geometry and not os.path.isfile(db_path):
        raise ValueError(
            "No existing project found. A geometry defining your area of "
            "interest is required for the first fetch to initialize a project.\n"
            "Pass a file path (shapefile, gpkg, geojson), GeoJSON string, "
            "bounding box (xmin,ymin,xmax,ymax), or WKT.\n\n"
            "Examples:\n"
            '  nbs fetch -d /path/to/project -g "-71.1,42.3,-70.9,42.4"\n'
            '  nbs fetch -d /path/to/project -g /path/to/area.geojson'
        )

    report = None
    if debug:
        from nbs.noaabathymetry._internal.diagnostics import DebugReport
        report = DebugReport(project_dir, data_source, cfg)

    result = FetchResult(tile_resolution_filter=tile_resolution_filter)
    try:
        result = _run_fetch(project_dir, geometry, cfg, data_source,
                            geom_prefix, bucket, local_dir, result, report,
                            tile_resolution_filter=tile_resolution_filter)
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


def fetch_tiles(
    project_dir: str,
    geometry: str = None,
    data_source: str = None,
    tile_resolution_filter: list = None,
    debug: bool = False,
) -> FetchResult:
    """Discover, download, and update NBS tiles.

    See :func:`_fetch_impl` for full documentation.
    """
    return _fetch_impl(project_dir, geometry, data_source,
                       tile_resolution_filter, debug)


def _run_fetch(project_dir, geometry, cfg, data_source,
               geom_prefix, bucket, local_dir, result, report=None,
               tile_resolution_filter=None):
    """Core fetch pipeline, separated so the debug wrapper in fetch_tiles()
    can handle report lifecycle without re-indenting the main logic.

    Steps: connect DB → download XML/tessellation → discover tiles via
    geometry → upsert delivery dates → classify → download → update records.
    """
    start = datetime.datetime.now()
    logger.info("═══ Fetch ═══")
    logger.info("Project: %s", project_dir)
    logger.info("Data source: %s", data_source)
    logger.info("")
    if tile_resolution_filter:
        logger.info("Tile resolution filter: %s",
                    make_resolution_label(tile_resolution_filter))
    os.makedirs(project_dir, exist_ok=True)

    conn = connect(project_dir, cfg)
    check_internal_version(conn)
    from nbs.noaabathymetry._internal.ratelimit import log_command
    log_command(conn, "fetch")
    if report:
        report.set_conn(conn)
    try:
        # Download XML catalog if needed (S102 sources)
        xml_prefix = cfg.get("xml_prefix")
        if xml_prefix:
            get_xml(conn, project_dir, xml_prefix, data_source, cfg, bucket=bucket)

        # Download tessellation geopackage
        geom_file = get_tessellation(conn, project_dir, geom_prefix, data_source, cfg,
                                     local_dir=local_dir, bucket=bucket)

        # Discover new tiles via geometry intersection
        if geometry:
            geometry_ds = parse_geometry_input(geometry)
            tile_list = get_tile_list(geometry_ds, geom_file)
            if tile_list is None:
                tile_list = []
            total_intersected = len(tile_list)
            if tile_resolution_filter:
                res_field = cfg["gpkg_fields"]["resolution"]
                res_set = set(tile_resolution_filter)
                tile_list = [
                    t for t in tile_list
                    if parse_resolution(t.get(res_field)) in res_set
                ]
            available, newly_tracked = insert_new(conn, tile_list, cfg)
            result.available_tiles_intersecting_aoi = available
            result.new_tiles_tracked = newly_tracked
            logger.info("─── Discovery ───")
            logger.info("%d available %s tile(s) intersecting area of interest "
                        "(%d newly tracked)",
                        available, data_source, newly_tracked)
            if tile_resolution_filter and len(tile_list) != total_intersected:
                logger.info("  (%d tile(s) excluded by resolution filter)",
                            total_intersected - len(tile_list))

        # Synchronize with latest tilescheme
        upsert_tiles(conn, project_dir, geom_file, cfg)

        # Download tiles
        db_tiles = all_db_tiles(conn)
        if tile_resolution_filter:
            res_set = set(tile_resolution_filter)
            result.filtered_out = [
                t["tilename"] for t in db_tiles
                if parse_resolution(t.get("resolution")) not in res_set
            ]
            db_tiles = [
                t for t in db_tiles
                if parse_resolution(t.get("resolution")) in res_set
            ]
        existing, missing, new = classify_tiles(db_tiles, project_dir, cfg)
        result.existing = existing
        result.missing_reset = missing

        client = None if local_dir else _get_s3_client()
        download_dict, tiles_found, tiles_not_found = build_download_plan(
            db_tiles, project_dir, cfg, data_source,
            client=client, bucket=bucket, local_dir=local_dir,
            skip_tilenames=set(existing))

        logger.info("─── Tile Status ───")
        logger.info("%d tile(s) with new data", len(new))
        logger.info("%d tile(s) already downloaded are missing locally",
                    len(missing))

        if download_dict:
            logger.info("─── Download ───")
        results = execute_downloads(download_dict, data_source)

        for r in results:
            if r["Result"] is True:
                result.downloaded.append(r["Tile"])
                download_dict[r["Tile"]]["downloaded_timestamp"] = r.get("downloaded_timestamp")
            elif r["Result"] == "not_found":
                result.not_found.append(r["Tile"])
            else:
                result.failed.append({"tile": r["Tile"], "reason": r.get("Reason", "unknown")})
        result.not_found.extend(tiles_not_found)

        if result.downloaded:
            update_records(conn, download_dict, result.downloaded, cfg)

        # Summary
        failed_verifications = [f for f in result.failed if "incorrect hash" in f["reason"]]
        logger.info("─── SUMMARY ───")
        logger.info("Downloaded:   %d tiles successfully downloaded",
                    len(result.downloaded))
        logger.info("Failed:       %d tiles failed to download",
                    len(result.failed))
        logger.info("Not found:    %d tiles not found on S3",
                    len(result.not_found))
        logger.info("Existing:     %d tiles already up to date locally",
                    len(result.existing))
        if result.filtered_out:
            logger.info("Filtered out: %d tiles excluded by resolution filter",
                        len(result.filtered_out))
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM tiles")
        total = cursor.fetchone()[0]
        logger.info("Total:        %d tiles", total)
        if result.not_found:
            logger.warning("%d tile(s) not found on S3. The NBS may be "
                           "actively updating. Rerun fetch later to retry.",
                           len(result.not_found))
        if result.failed:
            logger.warning("Rerun fetch to retry failed downloads.")
            if failed_verifications:
                failed_names = [f["tile"] for f in failed_verifications]
                logger.warning("%d tiles failed checksum "
                               "verification: %s",
                               len(failed_verifications), failed_names)
        logger.info("═════════════")
    finally:
        if not report:
            conn.close()
    return result
