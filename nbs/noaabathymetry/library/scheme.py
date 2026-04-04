"""Tile-scheme discovery, download, and parsing functions.

These functions expose the building blocks for working with remote
tile-scheme geopackages on S3, enabling callers to implement their
own caching and freshness strategies.
"""

import logging
import uuid

from osgeo import gdal

from nbs.noaabathymetry._internal.config import resolve_data_source
from nbs.noaabathymetry._internal.download import _get_s3_client, _list_s3_latest
from nbs.noaabathymetry._internal.status import _parse_geopackage

logger = logging.getLogger("noaabathymetry")


def list_tile_scheme(data_source=None):
    """List the latest remote tile-scheme geopackage metadata on S3.

    Performs an S3 listing to find the current latest geopackage under
    the data source's prefix.  Does not download the object.

    Parameters
    ----------
    data_source : str | None
        Data source name (e.g. ``"bluetopo"``, ``"bag"``).
        Defaults to ``"bluetopo"``.

    Returns
    -------
    tuple[str, datetime.datetime, str] | None
        ``(source_key, last_modified, etag)`` for the latest object,
        or ``None`` if the listing fails or no objects are found.

        - ``source_key``: Full S3 object key.
        - ``last_modified``: Timezone-aware ``datetime`` from S3.
        - ``etag``: S3 ETag with surrounding quotes stripped.
    """
    try:
        cfg, _ = resolve_data_source(data_source)
        client = _get_s3_client()
        source_key, objects = _list_s3_latest(
            client, cfg["bucket"], cfg["geom_prefix"],
            "geometry", cfg["canonical_name"], retry=False)
        if source_key is None:
            return None
        latest = objects[0]
        return (
            source_key,
            latest["LastModified"],
            latest.get("ETag", "").strip('"'),
        )
    except Exception:
        logger.debug("list_tile_scheme failed", exc_info=True)
        return None


def fetch_tile_scheme(data_source=None):
    """Download the latest remote tile-scheme geopackage from S3.

    Parameters
    ----------
    data_source : str | None
        Data source name (e.g. ``"bluetopo"``, ``"bag"``).
        Defaults to ``"bluetopo"``.

    Returns
    -------
    tuple[bytes, str, datetime.datetime, str]
        ``(raw_bytes, source_key, last_modified, etag)``

        - ``raw_bytes``: The geopackage file contents.
        - ``source_key``: Full S3 object key.
        - ``last_modified``: Timezone-aware ``datetime`` from S3.
        - ``etag``: S3 ETag with surrounding quotes stripped.

    Raises
    ------
    RuntimeError
        If no objects are found on S3 or the download fails.
    """
    cfg, _ = resolve_data_source(data_source)
    bucket = cfg["bucket"]
    client = _get_s3_client()

    source_key, _ = _list_s3_latest(
        client, bucket, cfg["geom_prefix"],
        "geometry", cfg["canonical_name"], retry=True)
    if source_key is None:
        raise RuntimeError(
            f"No tile scheme found on S3 for {cfg['canonical_name']}. "
            "Check your internet connection.")

    response = client.get_object(Bucket=bucket, Key=source_key)
    raw_bytes = response["Body"].read()

    return (
        raw_bytes,
        source_key,
        response["LastModified"],
        response.get("ETag", "").strip('"'),
    )


def parse_tile_scheme(source, data_source=None):
    """Parse a tile scheme into a tile-name-keyed dict.

    Accepts either raw geopackage bytes or a pre-parsed GeoJSON dict.
    When a GeoJSON dict is provided, tile-map extraction is near-instant
    (~10 ms) since no OGR parsing is needed.

    Thread-safe: when parsing bytes, uses a unique ``/vsimem/`` path.

    Parameters
    ----------
    source : bytes | dict
        Raw geopackage file contents (e.g. from
        :func:`fetch_tile_scheme`), **or** a GeoJSON ``FeatureCollection``
        dict whose features contain the tile properties.
    data_source : str | None
        Data source name (needed to resolve field mappings).
        Defaults to ``"bluetopo"``.

    Returns
    -------
    dict[str, dict]
        ``{tile_name: {field: value, ...}, ...}``

    Raises
    ------
    RuntimeError
        If *source* is bytes and the geopackage cannot be parsed.
    """
    cfg, _ = resolve_data_source(data_source)
    if isinstance(source, dict):
        tile_field = cfg["gpkg_fields"]["tile"]
        tile_map = {}
        for feat in source.get("features", []):
            props = feat.get("properties", {})
            tile_name = props.get(tile_field)
            if tile_name is not None:
                tile_map[tile_name] = props
        return tile_map
    mem_path = f"/vsimem/_parse_{uuid.uuid4().hex}.gpkg"
    gdal.FileFromMemBuffer(mem_path, source)
    try:
        return _parse_geopackage(mem_path, cfg)
    finally:
        gdal.Unlink(mem_path)
