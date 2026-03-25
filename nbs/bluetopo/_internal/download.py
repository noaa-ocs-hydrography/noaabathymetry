"""
download.py - Download and track NBS bathymetric tiles.

Handles downloading tile-scheme geopackages, XML catalogs, and tile files
from S3 (or local directories), with SHA-256 checksum verification.

All S3 tile downloads use direct links from the geopackage — the S3 key
is extracted from the URL and downloaded directly (no listing).
"""

import concurrent.futures
import datetime
import hashlib
import logging
import os
import re
import shutil
import sqlite3
import sys
import time

# Register datetime adapter/converter to avoid DeprecationWarning on Python 3.12+
def _adapt_datetime_iso(val):
    return val.isoformat()

sqlite3.register_adapter(datetime.datetime, _adapt_datetime_iso)

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from osgeo import ogr
from tqdm import tqdm

from nbs.bluetopo._internal.config import (
    _timestamp,
    get_built_flags,
    get_disk_fields,
    get_utm_file_columns,
    get_verified_fields,
)

logger = logging.getLogger("bluetopo")


# ---------------------------------------------------------------------------
# S3 client
# ---------------------------------------------------------------------------

def _get_s3_client():
    """Create an anonymous (unsigned) boto3 S3 client for public bucket access."""
    return boto3.client(
        "s3",
        aws_access_key_id="",
        aws_secret_access_key="",
        config=Config(signature_version=UNSIGNED),
    )


def _stream_hash(path):
    """Compute SHA-256 of a file by streaming in 64 KB chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _s3_key_from_url(url):
    """Extract the S3 object key from a virtual-hosted S3 URL.

    Expects ``https://bucket.s3.amazonaws.com/key`` format
    (used by all NBS geopackage URLs).
    """
    parts = url.split("amazonaws.com/")
    if len(parts) < 2:
        raise ValueError(f"Cannot parse S3 key from URL: {url}")
    return parts[1]


# ---------------------------------------------------------------------------
# Tessellation + XML download (with retry)
# ---------------------------------------------------------------------------

def _list_s3_latest(client, bucket, prefix, label, data_source, retry=True):
    """List S3 objects under *prefix* and return the latest by LastModified.

    Parameters
    ----------
    client : botocore.client.S3
        Anonymous S3 client.
    bucket : str
        S3 bucket name.
    prefix : str
        S3 key prefix to search under.
    label : str
        Human-readable asset label for log messages (e.g. ``"geometry"``).
    data_source : str
        Data source name for log messages.
    retry : bool
        If True and no objects found, wait 5 seconds and retry once.

    Returns
    -------
    tuple[str | None, list[dict]]
        ``(latest_s3_key, all_objects)`` sorted newest-first,
        or ``(None, [])`` if nothing found.
    """
    paginator = client.get_paginator("list_objects_v2")
    objs = paginator.paginate(Bucket=bucket, Prefix=prefix).build_full_result()
    if "Contents" not in objs:
        if retry:
            logger.warning("%s: No %s found in %s, retrying in 5 seconds...",
                          data_source, label, prefix)
            time.sleep(5)
            objs = paginator.paginate(Bucket=bucket, Prefix=prefix).build_full_result()
        if "Contents" not in objs:
            return None, []
    objects = objs["Contents"]
    objects.sort(key=lambda x: x["LastModified"], reverse=True)
    return objects[0]["Key"], objects


def get_tessellation(conn, project_dir, prefix, data_source, cfg,
                     local_dir=None, bucket="noaa-ocs-nationalbathymetry-pds"):
    """Download the tile-scheme geopackage from S3 (or copy from local).

    Removes any previously downloaded tessellation first.  Picks the latest
    file by LastModified (S3) or filename sort (local).  Retries once after
    5 seconds if no objects found on S3.

    Returns the absolute path to the downloaded geopackage, or raises
    RuntimeError if none found after retry.
    """
    catalog_table = cfg["catalog_table"]
    catalog_pk = cfg["catalog_pk"]
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {catalog_table} WHERE {catalog_pk} = 'Tessellation'")
    for tilescheme in [dict(row) for row in cursor.fetchall()]:
        try:
            os.remove(os.path.join(project_dir, tilescheme["location"]))
        except (OSError, PermissionError):
            continue

    if local_dir is not None:
        gpkg_files = os.listdir(prefix)
        gpkg_files = [f for f in gpkg_files if f.endswith(".gpkg") and "Tile_Scheme" in f]
        if not gpkg_files:
            raise RuntimeError(
                f"[{_timestamp()}] {data_source}: No tile scheme geopackage found in {prefix}")
        gpkg_files.sort(reverse=True)
        filename = gpkg_files[0]
        if len(gpkg_files) > 1:
            logger.info("%s: More than one geometry found in %s, using %s",
                        data_source, prefix, filename)
        destination_name = os.path.join(project_dir, data_source, "Tessellation", filename)
        relative = os.path.join(data_source, "Tessellation", filename)
        os.makedirs(os.path.dirname(destination_name), exist_ok=True)
        try:
            shutil.copy(os.path.join(prefix, filename), destination_name)
        except Exception as e:
            raise OSError(
                f"[{_timestamp()}] {data_source}: Failed to download tile scheme. "
                "Possibly due to conflict with an open existing file. "
                "Please close all files and attempt again") from e
    else:
        client = _get_s3_client()
        source_key, all_objects = _list_s3_latest(
            client, bucket, prefix, "geometry", data_source, retry=True)
        if source_key is None:
            raise RuntimeError(
                f"[{_timestamp()}] {data_source}: No tile scheme geopackage found in "
                f"{prefix} after retry. The NBS may be updating. Please try again later.")
        filename = os.path.basename(source_key)
        relative = os.path.join(data_source, "Tessellation", filename)
        if len(all_objects) > 1:
            logger.info("%s: More than one geometry found in %s, using %s",
                        data_source, prefix, filename)
        destination_name = os.path.join(project_dir, relative)
        os.makedirs(os.path.dirname(destination_name), exist_ok=True)
        try:
            client.download_file(bucket, source_key, destination_name)
        except (OSError, PermissionError) as e:
            raise OSError(
                f"[{_timestamp()}] {data_source}: Failed to download tile scheme. "
                "Possibly due to conflict with an open existing file. "
                "Please close all files and attempt again") from e

    logger.info("%s: Downloaded %s", data_source, filename)
    cursor.execute(
        f"""REPLACE INTO {catalog_table}({catalog_pk}, location, downloaded)
                      VALUES(?, ?, ?)""",
        ("Tessellation", relative, datetime.datetime.now()),
    )
    conn.commit()
    return destination_name


def get_xml(conn, project_dir, prefix, data_source, cfg,
            bucket="noaa-ocs-nationalbathymetry-pds"):
    """Download the S102 CATALOG.XML from S3.

    Prefers timestamped files (e.g. CATALOG_20260316_132904.XML) over
    plain CATALOG.XML for download-then-rename safety.  Downloads to the
    original filename, then renames to CATALOG.XML.  If rename fails
    (busy file handle on Windows), the timestamped file persists on disk
    as a signal to the user.

    Retries once after 5 seconds if no objects found.
    """
    catalog_table = cfg["catalog_table"]
    catalog_pk = cfg["catalog_pk"]
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {catalog_table} WHERE {catalog_pk} = 'XML'")
    for record in [dict(row) for row in cursor.fetchall()]:
        try:
            if os.path.isfile(os.path.join(project_dir, record["location"])):
                os.remove(os.path.join(project_dir, record["location"]))
        except (OSError, PermissionError):
            continue

    client = _get_s3_client()
    source_key, all_objects = _list_s3_latest(
        client, bucket, prefix, "XML", data_source, retry=True)
    if source_key is None:
        logger.warning("%s: No XML found in %s after retry", data_source, prefix)
        return None

    # Prefer timestamped version over plain CATALOG.XML for rename safety
    if len(all_objects) > 1:
        timestamped = [o for o in all_objects
                       if os.path.basename(o["Key"]).upper() != "CATALOG.XML"]
        if timestamped:
            source_key = timestamped[0]["Key"]

    filename = os.path.basename(source_key)
    relative = os.path.join(data_source, "Data", filename)
    if len(all_objects) > 1:
        logger.info("%s: More than one XML found in %s, using %s",
                    data_source, prefix, filename)
    destination_name = os.path.join(project_dir, relative)
    filename_renamed = "CATALOG.XML"
    relative_renamed = os.path.join(data_source, "Data", filename_renamed)
    destination_name_renamed = os.path.join(project_dir, relative_renamed)
    os.makedirs(os.path.dirname(destination_name), exist_ok=True)
    try:
        client.download_file(bucket, source_key, destination_name)
    except (OSError, PermissionError) as e:
        raise OSError(
            f"[{_timestamp()}] {data_source}: Failed to download XML. "
            "Possibly due to conflict with an open existing file. "
            "Please close all files and attempt again") from e
    try:
        os.replace(destination_name, destination_name_renamed)
    except (OSError, PermissionError) as e:
        raise OSError(
            f"[{_timestamp()}] {data_source}: Failed to rename XML to CATALOG.XML. "
            "Possibly due to conflict with an open existing file named CATALOG.XML. "
            "Please close all files and attempt again") from e
    logger.info("%s: Downloaded %s", data_source, filename_renamed)
    cursor.execute(
        f"""REPLACE INTO {catalog_table}({catalog_pk}, location, downloaded)
                      VALUES(?, ?, ?)""",
        ("XML", relative_renamed, datetime.datetime.now()),
    )
    conn.commit()
    return destination_name_renamed


# ---------------------------------------------------------------------------
# Tile classification
# ---------------------------------------------------------------------------

def all_db_tiles(conn):
    """Return all rows from the ``tiles`` table as a list of dicts."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles")
    return [dict(row) for row in cursor.fetchall()]


def classify_tiles(db_tiles, project_dir, cfg):
    """Classify tiles into existing, missing, or new.

    Parameters
    ----------
    db_tiles : list[dict]
        Tile rows from the database.
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration.

    Returns
    -------
    tuple[list[str], list[str], list[str]]
        ``(existing, missing, new)`` where each is a list of tilenames.

        - **existing** — downloaded, checksum-verified, and files present on disk.
        - **missing** — disk path recorded but file absent or not verified.
        - **new** — no disk path recorded (never downloaded).
    """
    disk_fields = get_disk_fields(cfg)
    verified_fields = get_verified_fields(cfg)

    existing = []
    missing = []
    new = []

    for tile in db_tiles:
        # Check if all disk paths are set
        all_paths_set = all(tile.get(df) for df in disk_fields)
        if not all_paths_set:
            new.append(tile["tilename"])
            continue

        # Check if all files exist on disk
        all_exist = all(
            os.path.isfile(os.path.join(project_dir, tile[df]))
            for df in disk_fields
        )
        if not all_exist:
            missing.append(tile["tilename"])
            continue

        # Check if all verified
        all_verified = all(tile.get(vf) == 1 for vf in verified_fields)
        if not all_verified:
            missing.append(tile["tilename"])
            continue

        existing.append(tile["tilename"])

    return existing, missing, new


# ---------------------------------------------------------------------------
# Download plan
# ---------------------------------------------------------------------------

def _build_tile_download(tile, cfg, data_source, client, bucket, local_dir):
    """Build a download plan dict for a single tile from its file_slots.

    Each slot maps to a file entry with source path/key, destination path,
    and expected checksum. Returns None if any slot has no valid link
    (meaning the tile cannot be downloaded).
    """
    slots = cfg["file_slots"]
    download = {"tile": tile["tilename"], "utm": tile["utm"], "files": []}

    if local_dir is not None:
        download["transport"] = "local"
    else:
        download["transport"] = "s3"
        download["client"] = client
        download["bucket"] = bucket

    for slot in slots:
        name = slot["name"]
        link = tile.get(f"{name}_link")
        if not link or str(link).lower() == "none":
            return None

        if local_dir is not None:
            # Local source: link is a file path
            basename = os.path.basename(link)
            rel_disk = os.path.join(data_source, f"UTM{tile['utm']}", basename)
        else:
            # S3 source: extract key from URL, use basename for local path
            try:
                s3_key = _s3_key_from_url(link)
            except ValueError:
                return None
            basename = os.path.basename(link)
            rel_disk = os.path.join(data_source, f"UTM{tile['utm']}", basename)

        file_entry = {
            "name": name,
            "source": link if local_dir is not None else s3_key,
            "disk": rel_disk,
            "dest": None,  # set by build_download_plan
            "checksum": tile.get(f"{name}_sha256_checksum"),
        }
        download["files"].append(file_entry)

    return download


def build_download_plan(db_tiles, project_dir, cfg, data_source,
                        client=None, bucket=None, local_dir=None,
                        skip_tilenames=None):
    """Build a download plan for all tiles needing download.

    Iterates over *db_tiles*, skips those in *skip_tilenames* or already
    verified on disk, and builds a per-file download spec for the rest.

    Parameters
    ----------
    db_tiles : list[dict]
        Tile rows from the database.
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration.
    data_source : str
        Canonical data source name.
    client : botocore.client.S3 | None
        S3 client (None for local sources).
    bucket : str | None
        S3 bucket name (None for local sources).
    local_dir : str | None
        Local directory path, or None for S3 sources.
    skip_tilenames : set[str] | None
        Tilenames to skip (already classified as existing/verified).

    Returns
    -------
    tuple[dict, list[str], list[str]]
        ``(download_dict, tiles_found, tiles_not_found)`` where
        *download_dict* is keyed by tilename.
    """
    disk_fields = get_disk_fields(cfg)
    verified_fields = get_verified_fields(cfg)
    download_dict = {}
    tiles_found = []
    tiles_not_found = []
    skip = skip_tilenames or set()

    for tile in db_tiles:
        tilename = tile["tilename"]
        if tilename in skip:
            continue

        # Fallback: skip tiles already downloaded and verified on disk
        all_paths_set = all(tile.get(df) for df in disk_fields)
        if all_paths_set:
            all_exist = all(
                os.path.isfile(os.path.join(project_dir, tile[df]))
                for df in disk_fields
            )
            all_verified = all(tile.get(vf) == 1 for vf in verified_fields)
            if all_exist and all_verified:
                continue

        download = _build_tile_download(
            tile, cfg, data_source, client, bucket, local_dir)
        if download is None:
            tiles_not_found.append(tilename)
            continue

        # Set actual destination paths
        for f in download["files"]:
            f["dest"] = os.path.join(project_dir, f["disk"])
            os.makedirs(os.path.dirname(f["dest"]), exist_ok=True)

        download_dict[tilename] = download
        tiles_found.append(tilename)

    return download_dict, tiles_found, tiles_not_found


# ---------------------------------------------------------------------------
# Download execution
# ---------------------------------------------------------------------------

def pull(download):
    """Download all files for a single tile and verify checksums.

    Iterates over ``download["files"]``, fetching each from S3 or
    copying from a local directory.  After each file lands, verifies
    its SHA-256 checksum against the expected value from the geopackage.

    Parameters
    ----------
    download : dict
        Download spec built by :func:`_build_tile_download`, containing
        ``tile``, ``transport``, ``files``, and transport-specific keys.

    Returns
    -------
    dict
        ``{"Tile": str, "Result": bool, "Reason": str}``
    """
    try:
        for f in download["files"]:
            if download["transport"] == "s3":
                download["client"].download_file(
                    download["bucket"], f["source"], f["dest"])
            else:
                shutil.copy(f["source"], f["dest"])

            if not os.path.isfile(f["dest"]):
                return {"Tile": download["tile"], "Result": False,
                        "Reason": f"missing download for {f['name']}"}

            if f["checksum"]:
                actual_hash = _stream_hash(f["dest"])
                if f["checksum"].lower() != actual_hash.lower():
                    return {"Tile": download["tile"], "Result": False,
                            "Reason": f"incorrect hash for {f['name']} "
                                      f"(expected={f['checksum'][:12]}... "
                                      f"got={actual_hash[:12]}...)"}
    except Exception as e:
        return {"Tile": download["tile"], "Result": False,
                "Reason": f"exception: {e}"}
    return {"Tile": download["tile"], "Result": True, "Reason": "success"}


def execute_downloads(download_dict, data_source):
    """Execute all downloads in a thread pool with a tqdm progress bar.

    Uses ``(cpu_count - 1)`` worker threads.

    Parameters
    ----------
    download_dict : dict[str, dict]
        Download plans keyed by tilename.
    data_source : str
        Data source name shown in the progress bar.

    Returns
    -------
    list[dict]
        Result dicts from :func:`pull`, one per tile.
    """
    results = []
    download_length = len(download_dict)
    if download_length:
        logger.info("Fetching %d tiles", download_length)
        with tqdm(
            total=download_length,
            bar_format=("{desc}: {percentage:3.0f}%|{bar}| "
                        "{n_fmt}/{total_fmt} Tiles {elapsed}, "
                        "{remaining} Est. Time Remaining{postfix}"),
            desc=f"{data_source} Fetch",
            colour="#0085CA",
            position=0,
            leave=True,
        ) as progress:
            max_workers = max(1, (os.cpu_count() or 1) - 1)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                for result in executor.map(pull, download_dict.values()):
                    results.append(result)
                    progress.update(1)
    return results


# ---------------------------------------------------------------------------
# Record updates
# ---------------------------------------------------------------------------

def update_records(conn, download_dict, successful_downloads, cfg):
    """Update ``tiles`` and ``vrt_utm`` tables after successful downloads.

    For each successfully downloaded tile, sets disk paths and verified
    flags in ``tiles``, ensures a ``vrt_utm`` row exists for the
    affected UTM zone, and resets all build flags so ``build_vrt``
    will rebuild affected zones.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    download_dict : dict[str, dict]
        Download plans keyed by tilename.
    successful_downloads : list[str]
        Tilenames that downloaded successfully.
    cfg : dict
        Data source configuration.
    """
    slots = cfg["file_slots"]

    tiles_records = []
    affected_utms = set()

    for tilename, download in download_dict.items():
        if tilename not in successful_downloads:
            continue

        # Build tile record: disk paths + verified flags + tilename
        tile_values = []
        for f in download["files"]:
            tile_values.append(f["disk"])
        for _ in download["files"]:
            tile_values.append(1)  # integer verified flag
        tile_values.append(tilename)
        tiles_records.append(tuple(tile_values))

        affected_utms.add(download["utm"])

    if not tiles_records:
        return

    # Build SQL from file_slots
    disk_cols = [f"{s['name']}_disk" for s in slots]
    verified_cols = [f"{s['name']}_verified" for s in slots]
    set_parts = [f"{col} = ?" for col in disk_cols + verified_cols]
    set_clause = ", ".join(set_parts)

    cursor = conn.cursor()
    try:
        cursor.execute("BEGIN TRANSACTION;")

        cursor.executemany(
            f"UPDATE tiles SET {set_clause} WHERE tilename = ?",
            tiles_records,
        )

        # Ensure default partition rows exist for affected UTMs.
        # Built flags are set to 0 in the INSERT so the row is valid
        # even before the UPDATE below runs.
        built_flags = get_built_flags(cfg)
        insert_cols = ["utm", "params_key"] + built_flags
        if cfg.get("subdatasets"):
            insert_cols.append("built_combined")
        insert_col_str = ", ".join(insert_cols)
        insert_ph = ", ".join(["?"] * len(insert_cols))
        insert_rows = []
        for utm in affected_utms:
            vals = [utm, ""] + [0] * (len(insert_cols) - 2)
            insert_rows.append(tuple(vals))
        cursor.executemany(
            f"INSERT OR IGNORE INTO vrt_utm({insert_col_str}) VALUES({insert_ph})",
            insert_rows,
        )

        # Reset all partitions (default + parameterized) for affected UTMs
        utm_file_cols = get_utm_file_columns(cfg)
        reset_parts = [f"{col} = NULL" for col in utm_file_cols]
        for f in built_flags:
            reset_parts.append(f"{f} = 0")
        if cfg.get("subdatasets"):
            reset_parts.append("built_combined = 0")
        reset_clause = ", ".join(reset_parts)
        utm_ph = ", ".join(["?"] * len(affected_utms))
        cursor.execute(
            f"UPDATE vrt_utm SET {reset_clause} WHERE utm IN ({utm_ph})",
            list(affected_utms),
        )

        cursor.execute("COMMIT;")
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Tile insertion and upsert
# ---------------------------------------------------------------------------

def insert_new(conn, tiles, cfg):
    """Insert newly discovered tile names into the ``tiles`` table.

    Tiles are filtered to include only those with a valid tile name,
    delivery date, and links in all file slots.  Geopackage field names
    are mapped to DB column names using ``cfg["gpkg_fields"]`` and
    ``cfg["file_slots"]``.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    tiles : list[dict]
        Tile records from geometry intersection, using geopackage field names.
    cfg : dict
        Data source configuration.

    Returns
    -------
    int
        Number of tiles that passed the filter and were submitted for
        insertion (some may already exist due to ``ON CONFLICT DO NOTHING``).
    """
    cursor = conn.cursor()
    gpkg_fields = cfg["gpkg_fields"]
    slots = cfg["file_slots"]

    tile_list = []
    for tile in tiles:
        # Map geopackage field names to standard names
        tile_name = tile.get(gpkg_fields["tile"])
        delivered_date = tile.get(gpkg_fields["delivered_date"])
        if not tile_name or not delivered_date:
            continue

        # Check all file slots have links
        has_all_links = True
        for slot in slots:
            link = tile.get(slot["gpkg_link"])
            if not link or str(link).lower() == "none":
                has_all_links = False
                break
        if not has_all_links:
            continue

        tile_list.append((tile_name,))

    cursor.executemany(
        "INSERT INTO tiles(tilename) VALUES(?) ON CONFLICT DO NOTHING",
        tile_list,
    )
    conn.commit()
    return len(tile_list)


def upsert_tiles(conn, project_dir, tile_scheme, cfg):
    """Synchronize tile records with the latest tilescheme deliveries.

    For every tile already in the DB, compares ``delivered_date`` against
    the tilescheme geopackage.  If the geopackage has a newer date, old
    files are removed from disk and the record is upserted with updated
    links, checksums, and delivery date (disk/verified fields are cleared
    so the tile will be re-downloaded).

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    project_dir : str
        Absolute path to the project directory.
    tile_scheme : str
        Path to the tile-scheme geopackage file.
    cfg : dict
        Data source configuration.
    """
    gpkg_fields = cfg["gpkg_fields"]
    slots = cfg["file_slots"]
    disk_fields = get_disk_fields(cfg)
    data_source = cfg["canonical_name"]

    # Diagnostic info for error messages
    debug_info = (f"Data Source: {data_source}\n"
                  f"Python: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")

    # Date format expected from NBS geopackages: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.
    # String comparison is chronologically correct for both formats.
    _date_re = re.compile(r'^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$')
    _date_validated = False

    db_tiles = all_db_tiles(conn)
    ts_ds = ogr.Open(tile_scheme)
    ts_lyr = ts_ds.GetLayer()
    ts_defn = ts_lyr.GetLayerDefn()

    # Read all tilescheme features
    ts_tiles_map = {}
    for ft in ts_lyr:
        field_list = {}
        for field_num in range(ts_defn.GetFieldCount()):
            field_name = ts_defn.GetFieldDefn(field_num).name
            field_list[field_name] = ft.GetField(field_name)
        # Map to standard names
        tile_name = field_list.get(gpkg_fields["tile"])
        if tile_name is None:
            continue
        if tile_name in ts_tiles_map:
            raise ValueError(f"More than one tilename {tile_name} "
                             f"found in tileset.\nPlease alert NBS.\n{debug_info}")
        ts_tiles_map[tile_name] = field_list
    ts_ds = None

    insert_tiles = []
    for db_tile in db_tiles:
        ts_tile = ts_tiles_map.get(db_tile["tilename"])
        if ts_tile is None:
            logger.warning("%s in database appears to have "
                          "been removed from latest tilescheme",
                          db_tile["tilename"])
            continue

        delivered_date = ts_tile.get(gpkg_fields["delivered_date"])
        if delivered_date is None:
            logger.warning("Unexpected removal of delivered date "
                          "for tile %s", db_tile["tilename"])
            continue

        if not _date_validated:
            if not _date_re.match(delivered_date):
                raise ValueError(
                    f"Unexpected date format '{delivered_date}' for tile "
                    f"{db_tile['tilename']}. Expected 'YYYY-MM-DD HH:MM:SS'. "
                    f"The tilescheme format may have changed. "
                    f"Please contact NBS or update BlueTopo.\n{debug_info}")
            _date_validated = True

        # String comparison works chronologically for YYYY-MM-DD [HH:MM:SS] format.
        # Tiles with no stored date are always updated; tiles with a newer
        # geopackage date trigger re-download (old files removed, record upserted).
        if (db_tile["delivered_date"] is None) or (delivered_date > db_tile["delivered_date"]):
            # Remove old files
            try:
                for df in disk_fields:
                    if db_tile.get(df) and os.path.isfile(os.path.join(project_dir, db_tile[df])):
                        os.remove(os.path.join(project_dir, db_tile[df]))
            except (OSError, PermissionError) as e:
                logger.error("Failed to remove older files for tile "
                             "%s. Please close all files and "
                             "attempt fetch again.", db_tile["tilename"])
                raise e

            # Build insert tuple: tilename, then per-slot links + checksums,
            # then delivered_date, resolution, utm
            values = [ts_tile.get(gpkg_fields["tile"])]
            for slot in slots:
                values.append(ts_tile.get(slot["gpkg_link"]))
            values.append(delivered_date)
            values.append(ts_tile.get(gpkg_fields["resolution"]))
            values.append(ts_tile.get(gpkg_fields["utm"]))
            for slot in slots:
                values.append(ts_tile.get(slot["gpkg_checksum"]))
            insert_tiles.append(tuple(values))

    if insert_tiles:
        cursor = conn.cursor()
        # Build column lists from file_slots
        link_cols = [f"{s['name']}_link" for s in slots]
        checksum_cols = [f"{s['name']}_sha256_checksum" for s in slots]
        disk_cols = [f"{s['name']}_disk" for s in slots]
        verified_cols = [f"{s['name']}_verified" for s in slots]

        insert_cols = ["tilename"] + link_cols + ["delivered_date", "resolution", "utm"] + checksum_cols
        placeholders = ", ".join(["?"] * len(insert_cols))
        col_names = ", ".join(insert_cols)

        # ON CONFLICT: update links, date, resolution, utm, checksums; clear disk + verified
        update_parts = []
        for col in link_cols + ["delivered_date", "resolution", "utm"] + checksum_cols:
            update_parts.append(f"{col} = EXCLUDED.{col}")
        for col in disk_cols + verified_cols:
            update_parts.append(f"{col} = NULL")
        update_clause = ", ".join(update_parts)

        cursor.executemany(
            f"""INSERT INTO tiles({col_names})
                VALUES({placeholders})
                ON CONFLICT(tilename) DO UPDATE
                SET {update_clause}""",
            insert_tiles,
        )
        conn.commit()
