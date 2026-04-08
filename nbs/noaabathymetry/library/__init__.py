"""Developer-facing extended API for noaabathymetry.

Provides extended versions of the core functions (fetch, mosaic, status)
with additional parameters for advanced use cases such as supplying
a cached tile-scheme geopackage or composing custom workflows.

Also exposes building-block functions for working with remote
tile-scheme geopackages independently.
"""

import os
import sqlite3

from nbs.noaabathymetry._internal.config import resolve_data_source
from nbs.noaabathymetry._internal.status import _status_impl
from nbs.noaabathymetry._internal.fetcher import _fetch_impl
from nbs.noaabathymetry._internal.builder import _mosaic_impl
from nbs.noaabathymetry.library.scheme import (
    fetch_tile_scheme,
    list_tile_scheme,
    parse_tile_scheme,
)
from nbs.noaabathymetry.library.cleanup import clean_removed_from_nbs, CleanResult
from nbs.noaabathymetry.library.verify import verify_tiles, VerifyResult, generate_manifest
from nbs.noaabathymetry.library.export import export_project, ExportResult


def get_readonly_db_conn(project_dir, data_source=None):
    """Open a read-only connection to the project's registry database.

    Rows are returned as ``sqlite3.Row`` objects, which support
    both index and key-based access (e.g. ``row["tilename"]``).

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        Data source name.  Defaults to ``"bluetopo"``.

    Returns
    -------
    sqlite3.Connection
        A read-only database connection.

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
    db_name = f"{cfg['canonical_name'].lower()}_registry.db"
    db_path = os.path.join(project_dir, db_name)
    if not os.path.isfile(db_path):
        raise ValueError(
            f"Registry database not found ({db_name}). "
            "Note: fetch must be run at least once.")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def extended_status_tiles(project_dir, data_source=None, verbosity="normal",
                          remote_tiles=None):
    """Check local project freshness against the remote tile scheme.

    Extended version of :func:`nbs.noaabathymetry.status_tiles` with
    support for pre-fetched tile data.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        Data source name.  Defaults to ``"bluetopo"``.
    verbosity : str
        ``"quiet"``, ``"normal"`` (default), or ``"verbose"``.
    remote_tiles : dict | None
        Pre-fetched tile map dict (e.g. from :func:`parse_tile_scheme`).
        When provided, skips the S3 download.
    """
    return _status_impl(project_dir, data_source, verbosity,
                        remote_tiles=remote_tiles)


def extended_fetch_tiles(project_dir, geometry=None, data_source=None,
                         tile_resolution_filter=None, debug=False):
    """Discover, download, and update NBS tiles.

    Extended version of :func:`nbs.noaabathymetry.fetch_tiles`.
    """
    return _fetch_impl(project_dir, geometry, data_source,
                       tile_resolution_filter, debug)


def extended_mosaic_tiles(project_dir, data_source=None, relative_to_vrt=True,
                          mosaic_resolution_target=None,
                          tile_resolution_filter=None,
                          hillshade=False, workers=None, reproject=False,
                          output_dir=None, debug=False,
                          hillshade_dir=None, hillshade_resolution=16):
    """Build a per-UTM-zone mosaic from all source tiles.

    Extended version of :func:`nbs.noaabathymetry.mosaic_tiles` with
    additional hillshade options.

    Parameters
    ----------
    hillshade_dir : str | None
        Single directory name (relative to *project_dir*) for hillshade
        output.  Requires ``hillshade=True``.  When ``None`` (default),
        hillshades are written next to the mosaic files.
    hillshade_resolution : int | float
        Pixel size in meters for the hillshade.  Default 16.
    """
    return _mosaic_impl(project_dir, data_source, relative_to_vrt,
                        mosaic_resolution_target, tile_resolution_filter,
                        hillshade, workers, reproject, output_dir, debug,
                        hillshade_dir=hillshade_dir,
                        hillshade_resolution=hillshade_resolution)
