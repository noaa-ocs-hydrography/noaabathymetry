"""Project verification and manifest generation."""

import datetime
import json
import logging
import os
from dataclasses import dataclass, field

from nbs.noaabathymetry._internal.config import (
    get_disk_fields,
    get_utm_file_columns,
    get_verified_fields,
    resolve_data_source,
)
from nbs.noaabathymetry._internal.db import connect
from nbs.noaabathymetry._internal.download import _stream_hash, all_db_tiles

logger = logging.getLogger("noaabathymetry")


@dataclass
class VerifyResult:
    """Result of a verify_tiles operation.

    Attributes
    ----------
    verified : list[str]
        Tile names that passed all checks.
    unverified : list[str]
        Tile names where at least one verified flag is not 1.
    missing_files : list[dict]
        Tiles with files missing from disk.  Each dict has
        ``tilename`` and ``missing`` (list of column names) keys.
    checksum_mismatch : list[dict]
        Tiles where re-hashed SHA-256 does not match stored value.
        Each dict has ``tilename``, ``file``, ``expected``, and
        ``actual`` keys.
    """
    verified: list = field(default_factory=list)
    unverified: list = field(default_factory=list)
    missing_files: list = field(default_factory=list)
    checksum_mismatch: list = field(default_factory=list)


def verify_tiles(project_dir, data_source=None):
    """Verify integrity of all tiles in a project.

    Checks that every tile has its verified flags set, all disk files
    exist, and all SHA-256 checksums match.  The checksum step re-hashes
    every file on disk, which can be slow for large projects.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        Data source name.  Defaults to ``"bluetopo"``.

    Returns
    -------
    VerifyResult
        Summary of verification results.

    Raises
    ------
    ValueError
        If the registry database does not exist.
    """
    import platform

    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    cfg, _ = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]
    disk_fields = get_disk_fields(cfg)
    verified_fields = get_verified_fields(cfg)
    slots = cfg["file_slots"]

    db_name = f"{data_source.lower()}_registry.db"
    if not os.path.isfile(os.path.join(project_dir, db_name)):
        raise ValueError(
            f"Registry database not found ({db_name}). "
            "Note: fetch must be run at least once.")

    conn = connect(project_dir, cfg)
    try:
        tiles = all_db_tiles(conn)
    finally:
        conn.close()

    # Scan directories once for file existence checks
    dirs_to_scan = set()
    for tile in tiles:
        for df in disk_fields:
            path = tile.get(df)
            if path:
                dirs_to_scan.add(os.path.dirname(path))
    existing_files = set()
    for rel_dir in dirs_to_scan:
        abs_dir = os.path.join(project_dir, rel_dir) if rel_dir else project_dir
        try:
            with os.scandir(abs_dir) as entries:
                for e in entries:
                    if e.is_file(follow_symlinks=False):
                        existing_files.add(
                            os.path.join(rel_dir, e.name) if rel_dir else e.name)
        except FileNotFoundError:
            pass

    result = VerifyResult()
    total = len(tiles)

    for i, tile in enumerate(tiles):
        tilename = tile["tilename"]
        tile_ok = True

        # Check verified flags
        for vf in verified_fields:
            if tile.get(vf) != 1:
                result.unverified.append(tilename)
                tile_ok = False
                break

        if not tile_ok:
            continue

        # Check disk files exist
        missing = []
        for df in disk_fields:
            path = tile.get(df)
            if not path or path not in existing_files:
                missing.append(df)
        if missing:
            result.missing_files.append({
                "tilename": tilename, "missing": missing})
            continue

        # Check checksums
        checksum_ok = True
        for slot in slots:
            name = slot["name"]
            disk_path = tile.get(f"{name}_disk")
            expected = tile.get(f"{name}_sha256_checksum")
            if not disk_path or not expected:
                continue
            abs_path = os.path.join(project_dir, disk_path)
            actual = _stream_hash(abs_path)
            if expected.lower() != actual.lower():
                result.checksum_mismatch.append({
                    "tilename": tilename,
                    "file": disk_path,
                    "expected": expected,
                    "actual": actual,
                })
                checksum_ok = False

        if checksum_ok:
            result.verified.append(tilename)

        # Log progress at an interval scaled to project size
        interval = max(1, min(100, total // 10))
        if (i + 1) % interval == 0 or i + 1 == total:
            logger.info("Verified %d/%d tiles", i + 1, total)

    return result


def generate_manifest(project_dir, data_source=None, include_mosaics=True):
    """Generate a manifest of all project files with checksums and sizes.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        Data source name.  Defaults to ``"bluetopo"``.
    include_mosaics : bool
        Include mosaic VRTs, OVRs, and hillshades in the manifest.

    Returns
    -------
    dict
        Manifest with ``package_version``, ``data_source``,
        ``exported_at``, ``tile_count``, ``mosaics_included``,
        and ``files`` list.

    Raises
    ------
    ValueError
        If the registry database does not exist.
    """
    import platform
    from importlib.metadata import version, PackageNotFoundError
    try:
        pkg_version = version("noaabathymetry")
    except PackageNotFoundError:
        pkg_version = "unknown"

    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    cfg, _ = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]
    disk_fields = get_disk_fields(cfg)
    slots = cfg["file_slots"]

    db_name = f"{data_source.lower()}_registry.db"
    db_path = os.path.join(project_dir, db_name)
    if not os.path.isfile(db_path):
        raise ValueError(
            f"Registry database not found ({db_name}). "
            "Note: fetch must be run at least once.")

    conn = connect(project_dir, cfg)
    try:
        tiles = all_db_tiles(conn)

        # Collect all relative paths we need to check
        all_paths = set()
        all_paths.add(db_name)

        catalog_table = cfg["catalog_table"]
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {catalog_table}")
        catalog_rows = [dict(row) for row in cursor.fetchall()]
        for row in catalog_rows:
            if row.get("location"):
                all_paths.add(row["location"])

        for tile in tiles:
            for slot in slots:
                disk_path = tile.get(f"{slot['name']}_disk")
                if disk_path:
                    all_paths.add(disk_path)

        if include_mosaics:
            utm_cols = get_utm_file_columns(cfg)
            cursor.execute("SELECT * FROM mosaic_utm")
            mosaic_rows = [dict(row) for row in cursor.fetchall()]
            for utm_row in mosaic_rows:
                for col in utm_cols:
                    if utm_row.get(col):
                        all_paths.add(utm_row[col])
                if utm_row.get("hillshade"):
                    all_paths.add(utm_row["hillshade"])
        else:
            mosaic_rows = []

        # Scan directories once with scandir — builds {rel_path: size}
        dirs_to_scan = set()
        for p in all_paths:
            dirs_to_scan.add(os.path.dirname(p))
        file_sizes = {}
        for rel_dir in dirs_to_scan:
            abs_dir = os.path.join(project_dir, rel_dir) if rel_dir else project_dir
            try:
                with os.scandir(abs_dir) as entries:
                    for e in entries:
                        if e.is_file(follow_symlinks=False):
                            rel = os.path.join(rel_dir, e.name) if rel_dir else e.name
                            file_sizes[rel] = e.stat().st_size
            except FileNotFoundError:
                pass

        # Build manifest file list using cached sizes
        files = []

        # Registry DB
        if db_name in file_sizes:
            files.append({"path": db_name, "size": file_sizes[db_name]})

        # Catalog files
        for row in catalog_rows:
            location = row.get("location")
            if location and location in file_sizes:
                files.append({"path": location, "size": file_sizes[location]})

        # Tile files
        for tile in tiles:
            for slot in slots:
                name = slot["name"]
                disk_path = tile.get(f"{name}_disk")
                checksum = tile.get(f"{name}_sha256_checksum")
                if disk_path:
                    entry = {"path": disk_path}
                    if disk_path in file_sizes:
                        entry["size"] = file_sizes[disk_path]
                    if checksum:
                        entry["sha256"] = checksum
                    files.append(entry)

        # Mosaic files
        if include_mosaics:
            for utm_row in mosaic_rows:
                for col in utm_cols:
                    path = utm_row.get(col)
                    if path and path in file_sizes:
                        files.append({"path": path, "size": file_sizes[path]})
                # Auxiliary files excluded from get_utm_file_columns
                hs_path = utm_row.get("hillshade")
                if hs_path and hs_path in file_sizes:
                    files.append({"path": hs_path, "size": file_sizes[hs_path]})

    finally:
        conn.close()

    return {
        "package_version": pkg_version,
        "data_source": data_source,
        "exported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "tile_count": len(tiles),
        "mosaics_included": include_mosaics,
        "files": files,
    }
