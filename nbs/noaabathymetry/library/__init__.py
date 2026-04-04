"""Developer-facing extended API for noaabathymetry.

Provides extended versions of the core functions (fetch, mosaic, status)
with additional parameters for advanced use cases such as supplying
a cached tile-scheme geopackage or composing custom workflows.

Also exposes building-block functions for working with remote
tile-scheme geopackages independently.
"""

from nbs.noaabathymetry._internal.status import _status_impl
from nbs.noaabathymetry._internal.fetcher import _fetch_impl
from nbs.noaabathymetry._internal.builder import _mosaic_impl
from nbs.noaabathymetry.library.scheme import (
    fetch_tile_scheme,
    list_tile_scheme,
    parse_tile_scheme,
)


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
                          output_dir=None, debug=False):
    """Build a per-UTM-zone mosaic from all source tiles.

    Extended version of :func:`nbs.noaabathymetry.mosaic_tiles`.
    """
    return _mosaic_impl(project_dir, data_source, relative_to_vrt,
                        mosaic_resolution_target, tile_resolution_filter,
                        hillshade, workers, reproject, output_dir, debug)
