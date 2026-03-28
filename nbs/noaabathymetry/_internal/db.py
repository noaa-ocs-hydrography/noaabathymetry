"""
db.py - SQLite registry database operations.

Creates and maintains the survey registry database that tracks tile
downloads, VRT build state, and catalog metadata.  The schema is
driven by config file_slots and subdataset definitions.
"""

import logging
import os
import sqlite3

from nbs.noaabathymetry._internal.config import (
    get_catalog_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
)

logger = logging.getLogger("noaabathymetry")

# Increment when a release includes breaking changes that make older
# projects incompatible.  check_internal_version() compares this against the
# stored value and tells users to recreate their project.
INTERNAL_VERSION = 2


def connect(project_dir: str, cfg: dict) -> sqlite3.Connection:
    """Create or connect to the SQLite survey registry database.

    The registry contains four tables whose schemas are driven by *cfg*:

    - **catalog** (or **tileset**) -- tracks downloaded tessellation and XML files.
    - **tiles** -- one row per tile with links, disk paths, checksums, and verified flags.
    - **vrt_utm** -- VRT/OVR paths and built flags per UTM zone.
    - **metadata** -- key-value pairs (e.g. internal version tracking).

    On first run, tables are created.  On subsequent runs, any new columns
    required by the config are added via ``ALTER TABLE ADD COLUMN``.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration from ``config.get_config()``.

    Returns
    -------
    conn : sqlite3.Connection
        Connection with ``row_factory = sqlite3.Row``.
    """
    data_source = cfg["canonical_name"]
    catalog_fields = get_catalog_fields(cfg)
    vrt_utm_fields = get_vrt_utm_fields(cfg)
    tiles_fields = get_tiles_fields(cfg)
    catalog_table = cfg["catalog_table"]
    catalog_pk = cfg["catalog_pk"]

    database_path = os.path.join(project_dir, f"{data_source.lower()}_registry.db")
    try:
        conn = sqlite3.connect(database_path)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        logger.error("Failed to establish SQLite database connection.")
        raise e
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {catalog_table} (
            {catalog_pk} text PRIMARY KEY
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS vrt_utm (
            utm text NOT NULL,
            params_key text NOT NULL DEFAULT '',
            PRIMARY KEY (utm, params_key)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tiles (
            tilename text PRIMARY KEY
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
            key text PRIMARY KEY,
            value text
            );
            """
        )
        conn.commit()
        # Schema migration: add any columns required by the config that don't
        # yet exist.  This allows configs to evolve (e.g. new file slots or
        # subdatasets) without requiring users to recreate their database.
        table_field_pairs = [
            (catalog_table, catalog_fields),
            ("vrt_utm", vrt_utm_fields),
            ("tiles", tiles_fields),
        ]
        for table_name, field_dict in table_field_pairs:
            cursor.execute(f"SELECT name FROM pragma_table_info('{table_name}')")
            existing = [dict(row)["name"] for row in cursor.fetchall()]
            for field, ftype in field_dict.items():
                if field not in existing:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {field} {ftype}")
                    conn.commit()
    except sqlite3.Error as e:
        logger.error("Failed to create SQLite tables.")
        raise e
    return conn


def check_internal_version(conn):
    """Check the project's internal version and set it if absent.

    Call this at the beginning of ``fetch_tiles`` or ``build_vrt``
    (not from worker processes or diagnostics).

    For new projects (no version stored), the current internal version
    is recorded.  For existing projects created with an older internal
    version, a ``RuntimeError`` is raised instructing the user to
    recreate their project.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection (from :func:`connect`).
    """
    if not isinstance(conn, sqlite3.Connection):
        return
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM metadata WHERE key = 'internal_version'")
    row = cursor.fetchone()
    if row is None:
        # No version stored. Check if this is a brand new project or
        # an old project that predates version tracking.
        cursor.execute("SELECT COUNT(*) as cnt FROM tiles")
        has_data = cursor.fetchone()["cnt"] > 0
        if has_data:
            raise RuntimeError(
                "This project was created with an older version of "
                "noaabathymetry that predates internal version tracking. "
                "This release includes significant changes that are "
                "not compatible with existing projects. Please delete "
                "the existing project directory and re-run fetch_tiles "
                "and build_vrt, or use a new directory."
            )
        cursor.execute(
            "INSERT INTO metadata(key, value) VALUES('internal_version', ?)",
            (str(INTERNAL_VERSION),),
        )
        conn.commit()
    else:
        stored_version = int(row["value"])
        if stored_version < INTERNAL_VERSION:
            raise RuntimeError(
                f"This project was created with an older version of "
                f"noaabathymetry (internal version v{stored_version}, "
                f"current is v{INTERNAL_VERSION}). "
                "This release includes significant changes that are "
                "not compatible with existing projects. Please delete "
                "the existing project directory and re-run fetch_tiles "
                "and build_vrt, or use a new directory."
            )
