"""
db.py - SQLite registry database operations.

Creates and maintains the survey registry database that tracks tile
downloads, VRT build state, and catalog metadata.  The schema is
driven by config file_slots and subdataset definitions.
"""

import os
import sqlite3

from nbs.bluetopo._internal.config import (
    get_catalog_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
)


def connect(project_dir: str, cfg: dict) -> sqlite3.Connection:
    """Create or connect to the SQLite survey registry database.

    The registry contains three tables whose schemas are driven by *cfg*:

    - **catalog** (or **tileset**) -- tracks downloaded tessellation and XML files.
    - **tiles** -- one row per tile with links, disk paths, checksums, and verified flags.
    - **vrt_utm** -- VRT/OVR paths and built flags per UTM zone.

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
        print("Failed to establish SQLite database connection.")
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
            utm text PRIMARY KEY
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
        conn.commit()
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
        print("Failed to create SQLite tables.")
        raise e
    return conn
