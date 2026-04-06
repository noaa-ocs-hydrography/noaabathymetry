"""Tile cleanup for tiles removed from NBS."""

import json
import logging
import os
from dataclasses import dataclass, field

from nbs.noaabathymetry._internal.config import (
    get_all_reset_flags,
    get_disk_fields,
    get_utm_file_columns,
    resolve_data_source,
)
from nbs.noaabathymetry._internal.db import connect
from nbs.noaabathymetry._internal.status import _read_remote_geopackage

logger = logging.getLogger("noaabathymetry")


@dataclass
class CleanupResult:
    """Result of a clean_removed_from_nbs operation.

    Each list contains dicts with ``tilename`` and ``files`` keys,
    e.g. ``{"tilename": "T1", "files": ["BlueTopo/UTM18/T1.tiff"]}``.

    Attributes
    ----------
    removed_from_nbs : list[dict]
        Tiles removed from NBS whose files were successfully
        deleted and rows removed from the database.
    marked_for_deletion : list[dict]
        Tiles removed from NBS whose files could not be deleted.
        These are stored in the garbage table and will be retried on
        the next cleanup run.
    garbage_collected : list[dict]
        Entries from prior runs that were successfully cleaned up.
    garbage_remaining : list[dict]
        Entries from prior runs that still could not be deleted.
    """
    removed_from_nbs: list = field(default_factory=list)
    marked_for_deletion: list = field(default_factory=list)
    garbage_collected: list = field(default_factory=list)
    garbage_remaining: list = field(default_factory=list)


def _ensure_garbage_table(conn):
    """Create the garbage_tiles table if it does not exist."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS garbage_tiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tilename TEXT NOT NULL,
            files TEXT NOT NULL
        )"""
    )
    conn.commit()


def _is_file_referenced(conn, path, disk_fields):
    """Return True if any active tile row references this relative path."""
    conditions = " OR ".join(f"{df} = ?" for df in disk_fields)
    params = [path] * len(disk_fields)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COUNT(*) FROM tiles WHERE {conditions}", params)
    return cursor.fetchone()[0] > 0


def _try_delete_garbage_files(files, project_dir, conn, disk_fields):
    """Attempt to delete a list of relative file paths.

    Skips files that are referenced by an active tile row.
    Returns True if all files were handled (deleted or skipped).
    Returns False if any file could not be deleted.
    """
    to_delete = []
    for rel_path in files:
        if _is_file_referenced(conn, rel_path, disk_fields):
            continue
        abs_path = os.path.join(project_dir, rel_path)
        if os.path.isfile(abs_path):
            to_delete.append(abs_path)

    # Pre-check: verify all files are accessible before deleting any
    for p in to_delete:
        try:
            with open(p, "a"):
                pass
        except (OSError, PermissionError):
            return False

    # All files accessible — delete them
    for p in to_delete:
        try:
            os.remove(p)
        except (OSError, PermissionError):
            return False
    return True


def _reset_utms(conn, utms, cfg):
    """Reset mosaic built flags for a set of UTM zones."""
    if not utms:
        return
    built_flags = get_all_reset_flags(cfg)
    utm_file_cols = get_utm_file_columns(cfg)
    reset_parts = [f"{col} = NULL" for col in utm_file_cols]
    for f in built_flags:
        reset_parts.append(f"{f} = 0")
    reset_clause = ", ".join(reset_parts)
    utm_ph = ", ".join(["?"] * len(utms))
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE mosaic_utm SET {reset_clause} WHERE utm IN ({utm_ph})",
        list(utms),
    )


def clean_removed_from_nbs(project_dir, data_source=None, remote_tiles=None,
                              local_tiles=None):
    """Remove tiles that no longer appear in the NBS remote tile scheme.

    Performs two phases:

    1. **Garbage cleanup** — retries deletion of files from prior runs
       that could not be deleted (stored in the ``garbage_tiles`` table).
       Before deleting, checks that no active tile row references the
       same file path.
    2. **Scheme comparison** — compares local tiles against the remote tile
       scheme.  Tiles not in the scheme are removed from the ``tiles``
       table and their file paths are recorded in ``garbage_tiles``.
       File deletion is then attempted immediately.

    File deletion is atomic per garbage entry: either all files in the
    entry are deleted, or none are.

    UTM mosaic built flags are reset for any UTM zone that loses tiles,
    so the next mosaic build will regenerate affected VRTs.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        Data source name.  Defaults to ``"bluetopo"``.
    remote_tiles : dict | None
        Pre-fetched tile map dict (e.g. from :func:`parse_tile_scheme`
        or :func:`fetch_tile_scheme`).  When ``None``, downloads the
        tile scheme from S3.
    local_tiles : list[dict] | None
        Pre-fetched list of local tile rows (e.g. from
        ``all_db_tiles``).  When ``None``, reads from the database.

    Returns
    -------
    CleanupResult
        Summary of what was deleted, marked, or retried.
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

    db_name = f"{data_source.lower()}_registry.db"
    if not os.path.isfile(os.path.join(project_dir, db_name)):
        raise ValueError(
            f"Registry database not found ({db_name}). "
            "Note: fetch must be run at least once.")

    conn = connect(project_dir, cfg)
    result = CleanupResult()
    affected_utms = set()

    try:
        _ensure_garbage_table(conn)
        cursor = conn.cursor()

        # Phase 1: retry previously stored garbage entries
        cursor.execute("SELECT id, tilename, files FROM garbage_tiles")
        garbage_rows = cursor.fetchall()

        for row in garbage_rows:
            row_id = row["id"]
            tilename = row["tilename"]
            files = json.loads(row["files"])

            entry = {"tilename": tilename, "files": files}
            if _try_delete_garbage_files(files, project_dir, conn, disk_fields):
                cursor.execute(
                    "DELETE FROM garbage_tiles WHERE id = ?", (row_id,))
                result.garbage_collected.append(entry)
            else:
                result.garbage_remaining.append(entry)
                logger.debug("Still cannot delete files for %s", tilename)

        if result.garbage_collected:
            conn.commit()
            logger.info("Cleaned up %d previously stored garbage entry(ies)",
                        len(result.garbage_collected))

        # Phase 2: compare against remote scheme
        if remote_tiles is None:
            remote_tiles = _read_remote_geopackage(cfg)

        if local_tiles is None:
            cursor.execute("SELECT * FROM tiles")
            local_tiles = [dict(row) for row in cursor.fetchall()]

        # Identify tiles no longer in remote scheme
        removed = []
        for tile in local_tiles:
            if tile["tilename"] not in remote_tiles:
                removed.append(tile)
                utm = tile.get("utm")
                if utm:
                    affected_utms.add(utm)

        # Move removed tiles from tiles table to garbage table and reset
        # UTM flags atomically. If we crash after this commit, Phase 1
        # picks them up on next run.
        garbage_entries = []
        if removed:
            for tile in removed:
                files = []
                for df in disk_fields:
                    path = tile.get(df)
                    if path:
                        files.append(path)
                files_json = json.dumps(files)
                garbage_entries.append({
                    "tilename": tile["tilename"],
                    "files": files,
                    "files_json": files_json,
                })
                cursor.execute(
                    "DELETE FROM tiles WHERE tilename = ?",
                    (tile["tilename"],))

            cursor.executemany(
                "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
                [(e["tilename"], e["files_json"]) for e in garbage_entries],
            )
            _reset_utms(conn, affected_utms, cfg)
            conn.commit()

        # Now attempt file deletion for each newly added garbage entry
        for ge in garbage_entries:
            tilename = ge["tilename"]
            files = ge["files"]
            files_json = ge["files_json"]

            entry = {"tilename": tilename, "files": files}
            if _try_delete_garbage_files(files, project_dir, conn, disk_fields):
                cursor.execute(
                    "DELETE FROM garbage_tiles "
                    "WHERE tilename = ? AND files = ?",
                    (tilename, files_json))
                result.removed_from_nbs.append(entry)
            else:
                result.marked_for_deletion.append(entry)
                logger.warning("Cannot delete files for %s, stored for "
                               "future cleanup", tilename)

        if result.removed_from_nbs:
            conn.commit()

        total = len(result.removed_from_nbs) + len(result.marked_for_deletion)
        if total:
            logger.info("Removed from NBS: %d tile(s) — %d deleted, "
                        "%d stored for cleanup",
                        total, len(result.removed_from_nbs),
                        len(result.marked_for_deletion))
        if affected_utms:
            logger.info("Reset mosaic built flags for UTM zone(s): %s",
                        ", ".join(sorted(affected_utms)))

        return result
    finally:
        conn.close()
