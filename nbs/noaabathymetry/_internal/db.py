"""
db.py - SQLite registry database operations.

Creates and maintains the survey registry database that tracks tile
downloads, mosaic build state, and catalog metadata.  The schema is
driven by config file_slots and subdataset definitions.
"""

import datetime
import logging
import os
import sqlite3

from nbs.noaabathymetry._internal.config import (
    get_catalog_fields,
    get_mosaic_fields,
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
    - **mosaic_utm** -- mosaic/OVR paths and built flags per UTM zone.
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
    mosaic_utm_fields = get_mosaic_fields(cfg)
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
            CREATE TABLE IF NOT EXISTS mosaic_utm (
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
            id integer PRIMARY KEY CHECK (id = 1),
            internal_version integer,
            data_source text,
            initialized text
            );
            """
        )
        conn.commit()
        # Migrate from old key-value metadata table if needed
        cursor.execute("SELECT name FROM pragma_table_info('metadata')")
        meta_cols = [dict(row)["name"] for row in cursor.fetchall()]
        if "key" in meta_cols and "internal_version" not in meta_cols:
            # Old key-value schema — read existing values and recreate
            cursor.execute("SELECT key, value FROM metadata")
            old_data = {row["key"]: row["value"] for row in cursor.fetchall()}
            cursor.execute("DROP TABLE metadata")
            cursor.execute(
                """
                CREATE TABLE metadata (
                id integer PRIMARY KEY CHECK (id = 1),
                internal_version integer,
                data_source text,
                initialized text
                );
                """
            )
            if old_data.get("internal_version"):
                cursor.execute(
                    "INSERT INTO metadata(id, internal_version) VALUES(1, ?)",
                    (int(old_data["internal_version"]),))
            conn.commit()
        # Schema migration: add any columns required by the config that don't
        # yet exist.  This allows configs to evolve (e.g. new file slots or
        # subdatasets) without requiring users to recreate their database.
        table_field_pairs = [
            (catalog_table, catalog_fields),
            ("mosaic_utm", mosaic_utm_fields),
            ("tiles", tiles_fields),
        ]
        for table_name, field_dict in table_field_pairs:
            cursor.execute(f"SELECT name FROM pragma_table_info('{table_name}')")
            existing = [dict(row)["name"] for row in cursor.fetchall()]
            for field, ftype in field_dict.items():
                if field not in existing:
                    cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {field} {ftype}")
                    conn.commit()
        # Seed project metadata on first connect
        cursor.execute("SELECT initialized FROM metadata WHERE id = 1")
        row = cursor.fetchone()
        if row is None:
            now = datetime.datetime.now().isoformat()
            cursor.execute(
                "INSERT INTO metadata(id, internal_version, data_source, initialized) "
                "VALUES(1, NULL, ?, ?)",
                (data_source, now))
            conn.commit()
        elif row["initialized"] is None:
            now = datetime.datetime.now().isoformat()
            cursor.execute(
                "UPDATE metadata SET data_source = ?, initialized = ? WHERE id = 1",
                (data_source, now))
            conn.commit()
    except sqlite3.Error as e:
        logger.error("Failed to create SQLite tables.")
        raise e
    from nbs.noaabathymetry._internal.ratelimit import ensure_usage_table
    ensure_usage_table(conn)
    return conn


def check_internal_version(conn):
    """Check the project's internal version and set it if absent.

    Call this at the beginning of ``fetch_tiles`` or ``mosaic_tiles``
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
    cursor.execute("SELECT internal_version FROM metadata WHERE id = 1")
    row = cursor.fetchone()
    stored_version = row["internal_version"] if row else None
    if stored_version is None:
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
                "the existing project directory and re-run fetch "
                "and mosaic, or use a new directory."
            )
        cursor.execute(
            "UPDATE metadata SET internal_version = ? WHERE id = 1",
            (INTERNAL_VERSION,),
        )
        conn.commit()
    else:
        if stored_version < INTERNAL_VERSION:
            raise RuntimeError(
                f"This project was created with an older version of "
                f"noaabathymetry (internal version v{stored_version}, "
                f"current is v{INTERNAL_VERSION}). "
                "This release includes significant changes that are "
                "not compatible with existing projects. Please delete "
                "the existing project directory and re-run fetch "
                "and mosaic, or use a new directory."
            )
