"""
status.py - Check local project freshness against the remote tile scheme.

Reads the remote geopackage from S3 via GDAL's virtual filesystem
and compares delivery datetimes against the local registry database.  No
files are downloaded and the local DB is not modified (except the
command_usage counter for rate limiting).
"""

import logging
import os
import platform
from dataclasses import dataclass, field

from osgeo import gdal, ogr

from nbs.noaabathymetry._internal.config import (
    get_disk_fields,
    get_verified_fields,
    resolve_data_source,
)
from nbs.noaabathymetry._internal.db import connect
from nbs.noaabathymetry._internal.download import (
    _get_s3_client,
    _list_s3_latest,
    all_db_tiles,
)
from nbs.noaabathymetry._internal.ratelimit import check_rate_limit, log_command

logger = logging.getLogger("noaabathymetry")


@dataclass
class StatusResult:
    """Result of a status_tiles operation.

    Attributes
    ----------
    up_to_date : list[dict]
        Tiles whose local delivery datetime matches the remote and whose
        files exist on disk.  Each dict has ``tile``, ``utm``,
        ``resolution``, and ``local_datetime`` keys.
    updates_available : list[dict]
        Tiles with a newer delivery datetime on S3.  Each dict has
        ``tile``, ``utm``, ``resolution``, ``local_datetime``, and
        ``remote_datetime`` keys.
    missing_from_disk : list[dict]
        Tiles whose delivery datetime matches the remote but whose files
        are missing from disk.  Each dict has ``tile``, ``utm``,
        ``resolution``, and ``local_datetime`` keys.
    removed_from_scheme : list[dict]
        Tiles tracked locally that no longer appear in the remote
        geopackage.  Each dict has ``tile``, ``utm``, ``resolution``,
        and ``local_datetime`` keys.
    total_tracked : int
        Total number of tiles in the local database.
    """
    up_to_date: list = field(default_factory=list)
    updates_available: list = field(default_factory=list)
    missing_from_disk: list = field(default_factory=list)
    removed_from_scheme: list = field(default_factory=list)
    total_tracked: int = 0


def _read_remote_geopackage(cfg):
    """Read the remote tile scheme geopackage from S3.

    Returns a dict mapping tile names to their geopackage field dicts,
    or raises RuntimeError if the geopackage cannot be read.
    """
    gdal.SetConfigOption("AWS_NO_SIGN_REQUEST", "YES")

    bucket = cfg["bucket"]
    prefix = cfg["geom_prefix"]
    data_source = cfg["canonical_name"]

    client = _get_s3_client()
    source_key, _ = _list_s3_latest(
        client, bucket, prefix, "geometry", data_source, retry=True)
    if source_key is None:
        raise RuntimeError(
            f"No tile scheme found on S3 for {data_source}. "
            "Check your internet connection.")

    # Download entire geopackage to RAM via GDAL (single HTTP GET).
    remote_url = f"/vsicurl/https://{bucket}.s3.amazonaws.com/{source_key}"
    mem_path = "/vsimem/_status_tilescheme.gpkg"
    ret = gdal.CopyFile(remote_url, mem_path)
    if ret != 0:
        raise RuntimeError(
            f"Failed to download tile scheme for {data_source}.")

    try:
        ds = ogr.Open(mem_path)
        if ds is None:
            raise RuntimeError(
                f"Unable to read tile scheme for {data_source}.")

        gpkg_fields = cfg["gpkg_fields"]
        lyr = ds.GetLayer()
        defn = lyr.GetLayerDefn()

        tiles_map = {}
        for ft in lyr:
            fields = {}
            for i in range(defn.GetFieldCount()):
                name = defn.GetFieldDefn(i).name
                fields[name] = ft.GetField(name)
            tile_name = fields.get(gpkg_fields["tile"])
            if tile_name is not None:
                tiles_map[tile_name] = fields
        ds = None
    finally:
        gdal.Unlink(mem_path)

    return tiles_map


def _tile_files_exist(tile, project_dir, cfg):
    """Return True if all disk files for a tile exist on disk and are verified."""
    disk_fields = get_disk_fields(cfg)
    verified_fields = get_verified_fields(cfg)
    for df in disk_fields:
        path = tile.get(df)
        if not path or not os.path.isfile(os.path.join(project_dir, path)):
            return False
    for vf in verified_fields:
        if tile.get(vf) != 1:
            return False
    return True


def _tile_info(tile):
    """Extract standard tile info dict from a DB tile row."""
    return {
        "tile": tile["tilename"],
        "utm": tile.get("utm") or "Unknown",
        "resolution": tile.get("resolution") or "Unknown",
        "local_datetime": tile.get("delivered_date"),
    }


def _log_grouped(label, tiles):
    """Log tiles grouped by UTM zone and resolution."""
    logger.info("%s:", label)
    groups = {}
    for t in tiles:
        groups.setdefault(t["utm"], {}).setdefault(t["resolution"], []).append(t)
    for utm in sorted(groups):
        logger.info("  %s:", utm)
        for res in sorted(groups[utm]):
            n = len(groups[utm][res])
            logger.info("    %s:  %d tile%s", res, n, "s" if n != 1 else "")


def _log_table(label, tiles, include_remote=False):
    """Log tiles as a verbose table."""
    logger.info("%s:", label)
    if include_remote:
        logger.info("  %-24s %-6s%-6s%-20s%s", "Tile", "UTM", "Res",
                     "Local datetime", "Remote datetime")
        for t in sorted(tiles, key=lambda x: (x["utm"], x["resolution"], x["tile"])):
            logger.info("  %-24s %-6s%-6s%-20s%s",
                         t["tile"], t["utm"], t["resolution"],
                         t.get("local_datetime") or "None",
                         t.get("remote_datetime") or "None")
    else:
        logger.info("  %-24s %-6s%-6s%s", "Tile", "UTM", "Res", "Local datetime")
        for t in sorted(tiles, key=lambda x: (x["utm"], x["resolution"], x["tile"])):
            logger.info("  %-24s %-6s%-6s%s",
                         t["tile"], t["utm"], t["resolution"],
                         t.get("local_datetime") or "None")


def status_tiles(
    project_dir: str,
    data_source: str = None,
    verbose: bool = False,
) -> StatusResult:
    """Check local project freshness against the remote tile scheme.

    Reads the remote geopackage from S3 and compares
    delivery datetimes against the local registry database.  Does not
    download tile data or modify the local database.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        A known source name, or None (defaults to ``"bluetopo"``).
    verbose : bool
        If True, log individual tiles instead of UTM/resolution counts.


    Returns
    -------
    StatusResult
        Structured result with up_to_date, updates_available,
        missing_from_disk, and removed_from_scheme tiles.

    Raises
    ------
    ValueError
        If ``project_dir`` is not an absolute path, the project
        directory does not exist, or the registry database is not found.
    ValueError
        If the rate limit is exceeded.
    RuntimeError
        If the remote tile scheme cannot be read from S3.
    """
    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    cfg, _ = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]

    if not os.path.isdir(project_dir):
        raise ValueError(f"Folder path not found: {project_dir}")

    db_name = f"{data_source.lower()}_registry.db"
    if not os.path.isfile(os.path.join(project_dir, db_name)):
        raise ValueError(
            f"Registry database not found ({db_name}). "
            "Note: fetch must be run at least once prior to status")

    conn = connect(project_dir, cfg)
    try:
        check_rate_limit(conn, "status")

        logger.info("═══ Begin %s: Checking status in %s ═══",
                     data_source, project_dir)

        gpkg_fields = cfg["gpkg_fields"]
        db_tiles = all_db_tiles(conn)
        remote_tiles = _read_remote_geopackage(cfg)

        result = StatusResult(total_tracked=len(db_tiles))

        for db_tile in db_tiles:
            tile_name = db_tile["tilename"]
            info = _tile_info(db_tile)
            remote = remote_tiles.get(tile_name)

            if remote is None:
                result.removed_from_scheme.append(info)
                continue

            remote_date = remote.get(gpkg_fields["delivered_date"])
            local_date = db_tile.get("delivered_date")

            if local_date is None or (remote_date and remote_date > local_date):
                info["remote_datetime"] = remote_date
                result.updates_available.append(info)
            elif not _tile_files_exist(db_tile, project_dir, cfg):
                result.missing_from_disk.append(info)
            else:
                result.up_to_date.append(info)

        # Output
        up_to_date_count = len(result.up_to_date)
        updates_count = len(result.updates_available)
        missing_count = len(result.missing_from_disk)
        removed_count = len(result.removed_from_scheme)

        logger.info("Project: %s", project_dir)
        logger.info("Data source: %s", data_source)
        logger.info("")

        if result.total_tracked == 0:
            logger.info("No tiles tracked. Run 'nbs fetch -d %s -g <geometry>' "
                         "to discover tiles.", project_dir)
        else:
            logger.info("Tracked tiles:    %d", result.total_tracked)
            logger.info("  Up to date:     %d", up_to_date_count)
            if updates_count:
                logger.info("  Updates:        %d", updates_count)
            if missing_count:
                logger.info("  Missing:        %d", missing_count)
            if removed_count:
                logger.info("  Removed:        %d", removed_count)

            if not updates_count and not missing_count and not removed_count:
                logger.info("")
                logger.info("All tiles are up to date.")
            else:
                if updates_count:
                    logger.info("")
                    if verbose:
                        _log_table("Updates available", result.updates_available,
                                   include_remote=True)
                    else:
                        _log_grouped("Updates available", result.updates_available)

                if missing_count:
                    logger.info("")
                    if verbose:
                        _log_table("Missing from disk", result.missing_from_disk)
                    else:
                        _log_grouped("Missing from disk", result.missing_from_disk)

                if removed_count:
                    logger.info("")
                    if verbose:
                        _log_table("Removed from scheme", result.removed_from_scheme)
                    else:
                        _log_grouped("Removed from scheme", result.removed_from_scheme)

                if updates_count or missing_count:
                    logger.info("")
                    logger.info("Run 'nbs fetch -d %s' to download updates.",
                                project_dir)

        logger.info("═══ Complete %s: Checking status ═══", data_source)
        return result
    finally:
        conn.close()
