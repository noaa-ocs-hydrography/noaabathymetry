"""
fetch_tiles.py - Download and track NBS bathymetric tiles from AWS S3.

Supports all NBS data sources (BlueTopo, Modeling, BAG, S102V21, S102V22,
S102V30) as well as local directory sources (HSD).  The workflow is:

1. Download the tile-scheme geopackage (tessellation) and optional XML catalog.
2. Intersect a user-provided geometry with the tile scheme to discover tiles.
3. Insert newly discovered tiles into the ``tiles`` table.
4. Compare tile deliveries against the latest tilescheme and re-download
   tiles with newer delivery dates.
5. Download tile files from S3 (or copy from local dir) with SHA-256
   verification, using a thread pool for parallelism.
6. Update the registry DB with disk paths and verification status, and
   insert/reset ``vrt_subregion`` and ``vrt_utm`` records so ``build_vrt``
   knows which VRTs to rebuild.

State is persisted in the same ``<source>_registry.db`` SQLite database used
by ``build_vrt``.
"""

import concurrent.futures
import datetime
import hashlib
import json
import os
import platform
import random
import shutil
import sqlite3
import sys

import boto3
import numpy as np
from botocore import UNSIGNED
from botocore.client import Config
from osgeo import gdal, ogr, osr
from tqdm import tqdm

from nbs.bluetopo.core.build_vrt import connect_to_survey_registry
from nbs.bluetopo.core.datasource import (
    _timestamp,
    get_config,
    get_local_config,
    get_vrt_subregion_fields,
    get_vrt_utm_fields,
    get_disk_field,
    get_disk_fields,
    get_verified_fields,
    get_vrt_file_columns,
    get_utm_file_columns,
)

# Diagnostic string included in error messages to help NBS troubleshoot reports.
debug_info = f"""
Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}
GDAL {gdal.VersionInfo()}
SQLite {sqlite3.sqlite_version}
Date {datetime.datetime.now()}
"""


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()

sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.datetime.fromisoformat(val)

sqlite3.register_converter("datetime", convert_datetime)


def _get_s3_client():
    """Create an anonymous (unsigned) boto3 S3 client for public bucket access."""
    cred = {
        "aws_access_key_id": "",
        "aws_secret_access_key": "",
        "config": Config(signature_version=UNSIGNED),
    }
    return boto3.client("s3", **cred)


def get_tessellation(
    conn: sqlite3.Connection,
    project_dir: str,
    prefix: str,
    data_source: str,
    cfg: dict,
    local_dir: str = None,
    bucket: str = "noaa-ocs-nationalbathymetry-pds",
) -> str:
    """
    Download the tile-scheme geopackage from S3 (or copy from a local directory).

    Any previously downloaded tessellation file is removed first.  When
    *local_dir* is provided, the geopackage is copied from the local
    directory rather than downloaded from S3.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    project_dir : str
        Absolute path to the project directory.
    prefix : str
        S3 key prefix for the geopackage, or a local directory path for
        local sources.
    data_source : str
        Canonical data source name (e.g. ``"BlueTopo"``).
    cfg : dict
        Data source configuration.
    local_dir : str | None
        Path to a local directory containing tile data.  When not None,
        the geopackage is copied from *prefix* instead of downloaded.
    bucket : str
        S3 bucket name.

    Returns
    -------
    str | None
        Absolute path to the downloaded geopackage, or None if not found.
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
        # Local directory source
        gpkg_files = os.listdir(prefix)
        gpkg_files = [f for f in gpkg_files if f.endswith(".gpkg") and "Tile_Scheme" in f]
        if len(gpkg_files) == 0:
            print(f"[{_timestamp()}] {data_source}: No geometry found in {prefix}")
            return None
        gpkg_files.sort(reverse=True)
        filename = gpkg_files[0]
        if len(gpkg_files) > 1:
            print(f"[{_timestamp()}] {data_source}: More than one geometry found in {prefix}, using {gpkg_files[0]}")
        destination_name = os.path.join(project_dir, data_source, "Tessellation", gpkg_files[0])
        if not os.path.exists(os.path.dirname(destination_name)):
            os.makedirs(os.path.dirname(destination_name))
        try:
            shutil.copy(os.path.join(prefix, gpkg_files[0]), destination_name)
            relative = os.path.join(data_source, "Tessellation", gpkg_files[0])
        except Exception as e:
            raise OSError(f"[{_timestamp()}] {data_source}: "
                          "Failed to download tile scheme "
                          "possibly due to conflict with an open existing file. "
                          "Please close all files and attempt again") from e
    else:
        client = _get_s3_client()
        pageinator = client.get_paginator("list_objects_v2")
        objs = pageinator.paginate(Bucket=bucket, Prefix=prefix).build_full_result()
        if "Contents" not in objs:
            print(f"[{_timestamp()}] {data_source}: No geometry found in {prefix}")
            return None
        tileschemes = objs["Contents"]
        tileschemes.sort(key=lambda x: x["LastModified"], reverse=True)
        source_name = tileschemes[0]["Key"]
        filename = os.path.basename(source_name)
        relative = os.path.join(data_source, "Tessellation", filename)
        if len(tileschemes) > 1:
            print(f"[{_timestamp()}] {data_source}: More than one geometry found in {prefix}, using {filename}")
        destination_name = os.path.join(project_dir, relative)
        if not os.path.exists(os.path.dirname(destination_name)):
            os.makedirs(os.path.dirname(destination_name))
        try:
            client.download_file(bucket, source_name, destination_name)
        except (OSError, PermissionError) as e:
            raise OSError(f"[{_timestamp()}] {data_source}: "
                          "Failed to download tile scheme "
                          "possibly due to conflict with an open existing file. "
                          "Please close all files and attempt again") from e
    print(f"[{_timestamp()}] {data_source}: Downloaded {filename}")
    cursor.execute(
        f"""REPLACE INTO {catalog_table}({catalog_pk}, location, downloaded)
                      VALUES(?, ?, ?)""",
        ("Tessellation", relative, datetime.datetime.now()),
    )
    conn.commit()
    return destination_name



def get_xml(
    conn: sqlite3.Connection,
    project_dir: str,
    prefix: str,
    data_source: str,
    cfg: dict,
    bucket: str = "noaa-ocs-nationalbathymetry-pds",
) -> str:
    """
    Download the S102 CATALOG.XML from S3.

    Used by S102V21, S102V22, and S102V30 sources which require an XML catalog
    alongside the tile data.  The downloaded file is renamed to
    ``CATALOG.XML`` in the ``Data/`` subdirectory.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    project_dir : str
        Absolute path to the project directory.
    prefix : str
        S3 key prefix for the XML file.
    data_source : str
        Canonical data source name.
    cfg : dict
        Data source configuration.
    bucket : str
        S3 bucket name.

    Returns
    -------
    str | None
        Absolute path to the renamed ``CATALOG.XML``, or None if not found.
    """
    catalog_table = cfg["catalog_table"]
    catalog_pk = cfg["catalog_pk"]
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {catalog_table} WHERE {catalog_pk} = 'XML'")
    for tilescheme in [dict(row) for row in cursor.fetchall()]:
        try:
            if os.path.isfile(os.path.join(project_dir, tilescheme["location"])):
                os.remove(os.path.join(project_dir, tilescheme["location"]))
        except (OSError, PermissionError):
            continue
    client = _get_s3_client()
    pageinator = client.get_paginator("list_objects_v2")
    objs = pageinator.paginate(Bucket=bucket, Prefix=prefix).build_full_result()
    if "Contents" not in objs:
        print(f"[{_timestamp()}] {data_source}: No XML found in {prefix}")
        return None
    tileschemes = objs["Contents"]
    tileschemes.sort(key=lambda x: x["LastModified"], reverse=True)
    source_name = tileschemes[0]["Key"]
    filename = os.path.basename(source_name)
    relative = os.path.join(data_source, "Data", filename)
    if len(tileschemes) > 1:
        print(f"[{_timestamp()}] {data_source}: More than one XML found in {prefix}, using {filename}")
    destination_name = os.path.join(project_dir, relative)
    filename_renamed = 'CATALOG.XML'
    relative_renamed = os.path.join(data_source, "Data", filename_renamed)
    destination_name_renamed = os.path.join(project_dir, relative_renamed)
    if not os.path.exists(os.path.dirname(destination_name)):
        os.makedirs(os.path.dirname(destination_name))
    try:
        client.download_file(bucket, source_name, destination_name)
    except (OSError, PermissionError) as e:
        raise OSError(f"[{_timestamp()}] {data_source}: "
                      "Failed to download XML "
                      "possibly due to conflict with an open existing file. "
                      "Please close all files and attempt again") from e
    try:
        os.replace(destination_name, destination_name_renamed)
    except (OSError, PermissionError) as e:
        raise OSError(f"[{_timestamp()}] {data_source}: "
                      "Failed to rename XML to CATALOG.xml. "
                      "Possibly due to conflict with an open existing file named CATALOG.XML. "
                      "Please close all files and attempt again") from e
    print(f"[{_timestamp()}] {data_source}: Downloaded {filename_renamed}")
    cursor.execute(
        f"""REPLACE INTO {catalog_table}({catalog_pk}, location, downloaded)
                      VALUES(?, ?, ?)""",
        ("XML", relative, datetime.datetime.now()),
    )
    conn.commit()
    return destination_name_renamed


def download_tiles(
    conn: sqlite3.Connection,
    project_dir: str,
    tile_prefix: str,
    data_source: str,
    cfg: dict,
    local_dir: str = None,
    bucket: str = "noaa-ocs-nationalbathymetry-pds",
) -> tuple:
    """
    Download all pending tile files and verify checksums.

    The download process has three phases:

    1. **Classify** -- each tile in the DB is checked: already downloaded and
       verified (skip), downloaded but unverified (re-download), or new
       (download).
    2. **Resolve** -- for tiles needing download, the S3 object keys are
       looked up (or local paths are resolved) and collected into
       *download_dict*.
    3. **Fetch** -- a thread pool downloads all resolved tiles in parallel
       with a tqdm progress bar, verifying SHA-256 checksums on completion.

    Successfully downloaded tiles have their ``tiles``, ``vrt_subregion``,
    and ``vrt_utm`` records updated via :func:`update_records`.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    project_dir : str
        Absolute path to the project directory.
    tile_prefix : str | None
        S3 key prefix for tile data.  Only used by ``prefix_listing``
        strategy sources.  None for ``direct_link`` sources.
    data_source : str
        Canonical data source name.
    cfg : dict
        Data source configuration.
    local_dir : str | None
        Path to a local directory containing tile data.  When not None,
        files are copied from local disk instead of downloaded from S3.
    bucket : str
        S3 bucket name.

    Returns
    -------
    tuple
        ``(tiles_found, tiles_not_found, successful_downloads,
        failed_downloads, existing_tiles, missing_tiles,
        failed_verifications, new_tile_list)``
    """
    file_layout = cfg["file_layout"]
    download_strategy = cfg["download_strategy"]
    disk_fields = get_disk_fields(cfg)
    verified_fields = get_verified_fields(cfg)

    download_tile_list = all_db_tiles(conn)
    random.shuffle(download_tile_list)

    if file_layout == "dual_file":
        new_tile_list = [t for t in download_tile_list if t["geotiff_disk"] is None or t["rat_disk"] is None]
    else:
        new_tile_list = [t for t in download_tile_list if t["file_disk"] is None]

    print("\nResolving fetch list...")
    if local_dir is None:
        client = _get_s3_client()
        paginator = client.get_paginator("list_objects_v2")
    existing_tiles = []
    missing_tiles = []
    tiles_found = []
    tiles_not_found = []
    download_dict = {}

    for fields in download_tile_list:
        # Check if already downloaded and verified
        if file_layout == "dual_file":
            has_all = fields["geotiff_disk"] and fields["rat_disk"]
            if has_all:
                both_exist = (os.path.isfile(os.path.join(project_dir, fields["geotiff_disk"]))
                              and os.path.isfile(os.path.join(project_dir, fields["rat_disk"])))
                if both_exist:
                    if fields["geotiff_verified"] != "True" or fields["rat_verified"] != "True":
                        missing_tiles.append(fields["tilename"])
                    else:
                        existing_tiles.append(fields["tilename"])
                        continue
                else:
                    missing_tiles.append(fields["tilename"])
        else:
            has_file = fields["file_disk"]
            if has_file:
                if os.path.isfile(os.path.join(project_dir, fields["file_disk"])):
                    if fields["file_verified"] != "True":
                        missing_tiles.append(fields["tilename"])
                    else:
                        existing_tiles.append(fields["tilename"])
                        continue
                else:
                    missing_tiles.append(fields["tilename"])

        tilename = fields["tilename"]

        if download_strategy == "prefix_listing":
            # BlueTopo / Modeling: list S3 objects under tile_prefix/{tilename}/
            pfx = tile_prefix + f"/{tilename}/"
            objs = paginator.paginate(Bucket=bucket, Prefix=pfx).build_full_result()
            if len(objs) > 0:
                download_dict[tilename] = {
                    "tile": tilename,
                    "transport": "s3",
                    "bucket": bucket,
                    "client": client,
                    "subregion": fields["subregion"],
                    "utm": fields["utm"],
                }
                for object_name in objs["Contents"]:
                    source_name = object_name["Key"]
                    relative = os.path.join(data_source, f"UTM{fields['utm']}", os.path.basename(source_name))
                    destination_name = os.path.join(project_dir, relative)
                    if not os.path.exists(os.path.dirname(destination_name)):
                        os.makedirs(os.path.dirname(destination_name))
                    if ".aux" in source_name.lower():
                        download_dict[tilename]["rat"] = source_name
                        download_dict[tilename]["rat_dest"] = destination_name
                        download_dict[tilename]["rat_verified"] = fields["rat_verified"]
                        download_dict[tilename]["rat_disk"] = relative
                        download_dict[tilename]["rat_sha256_checksum"] = fields["rat_sha256_checksum"]
                    else:
                        download_dict[tilename]["geotiff"] = source_name
                        download_dict[tilename]["geotiff_dest"] = destination_name
                        download_dict[tilename]["geotiff_verified"] = fields["geotiff_verified"]
                        download_dict[tilename]["geotiff_disk"] = relative
                        download_dict[tilename]["geotiff_sha256_checksum"] = fields["geotiff_sha256_checksum"]
                tiles_found.append(tilename)
            else:
                tiles_not_found.append(tilename)

        elif download_strategy == "direct_link":
            if file_layout == "dual_file":
                # Local dual-file sources (HSD, unknown local)
                if fields["geotiff_link"] and fields["rat_link"]:
                    download_dict[tilename] = {
                        "tile": tilename,
                        "subregion": fields["subregion"],
                        "utm": fields["utm"],
                    }
                    if local_dir is None:
                        download_dict[tilename]["transport"] = "s3"
                        download_dict[tilename]["client"] = client
                        download_dict[tilename]["bucket"] = bucket
                    else:
                        download_dict[tilename]["transport"] = "local"
                    download_dict[tilename]["rat"] = fields["rat_link"]
                    download_dict[tilename]["rat_disk"] = os.path.join(data_source, f"UTM{fields['utm']}", os.path.basename(fields["rat_link"]))
                    download_dict[tilename]["rat_dest"] = os.path.join(project_dir, download_dict[tilename]["rat_disk"])
                    download_dict[tilename]["rat_verified"] = fields["rat_verified"]
                    download_dict[tilename]["rat_sha256_checksum"] = fields["rat_sha256_checksum"]
                    download_dict[tilename]["geotiff"] = fields["geotiff_link"]
                    download_dict[tilename]["geotiff_disk"] = os.path.join(data_source, f"UTM{fields['utm']}", os.path.basename(fields["geotiff_link"]))
                    download_dict[tilename]["geotiff_dest"] = os.path.join(project_dir, download_dict[tilename]["geotiff_disk"])
                    download_dict[tilename]["geotiff_verified"] = fields["geotiff_verified"]
                    download_dict[tilename]["geotiff_sha256_checksum"] = fields["geotiff_sha256_checksum"]
                    if not os.path.exists(os.path.dirname(download_dict[tilename]["geotiff_dest"])):
                        os.makedirs(os.path.dirname(download_dict[tilename]["geotiff_dest"]))
                    tiles_found.append(tilename)
                else:
                    tiles_not_found.append(tilename)
            else:
                # Single file sources (BAG, S102V21, S102V22, S102V30)
                if fields["file_link"] and fields["file_link"] != "None":
                    if local_dir is not None:
                        download_dict[tilename] = {
                            "tile": tilename,
                            "transport": "local",
                            "subregion": fields["subregion"],
                            "utm": fields["utm"],
                        }
                        download_dict[tilename]["file"] = fields["file_link"]
                        download_dict[tilename]["file_disk"] = os.path.join(data_source, "Data", os.path.basename(fields["file_link"]))
                        download_dict[tilename]["file_dest"] = os.path.join(project_dir, download_dict[tilename]["file_disk"])
                        download_dict[tilename]["file_verified"] = fields["file_verified"]
                        download_dict[tilename]["file_sha256_checksum"] = fields["file_sha256_checksum"]
                        if not os.path.exists(os.path.dirname(download_dict[tilename]["file_dest"])):
                            os.makedirs(os.path.dirname(download_dict[tilename]["file_dest"]))
                        tiles_found.append(tilename)
                    else:
                        found = False
                        for obj in client.list_objects(Bucket=bucket, Prefix=fields['file_link'].split('amazonaws.com/')[1])['Contents']:
                            if os.path.basename(fields["file_link"])[7:13] in obj['Key']:
                                download_dict[tilename] = {
                                    "tile": tilename,
                                    "transport": "s3",
                                    "bucket": bucket,
                                    "client": client,
                                    "subregion": fields["subregion"],
                                    "utm": fields["utm"],
                                }
                                source_name = obj["Key"]
                                download_dict[tilename]["file"] = source_name
                                download_dict[tilename]["file_disk"] = os.path.join(data_source, "Data", os.path.basename(fields["file_link"]))
                                download_dict[tilename]["file_dest"] = os.path.join(project_dir, download_dict[tilename]["file_disk"])
                                download_dict[tilename]["file_verified"] = fields["file_verified"]
                                download_dict[tilename]["file_sha256_checksum"] = fields["file_sha256_checksum"]
                                if not os.path.exists(os.path.dirname(download_dict[tilename]["file_dest"])):
                                    os.makedirs(os.path.dirname(download_dict[tilename]["file_dest"]))
                                found = True
                                tiles_found.append(tilename)
                                break
                        if found is False:
                            tiles_not_found.append(tilename)

    def pull(downloads: dict) -> dict:
        """
        Download files and verify hash.

        Parameters
        ----------
        downloads : dict
            dict holding necessary values to execute download and checksum verification.

        Returns
        -------
        dict
            result of download attempt.
        """
        try:
            if file_layout == "dual_file":
                if downloads["transport"] == "s3":
                    downloads["client"].download_file(downloads["bucket"], downloads["geotiff"], downloads["geotiff_dest"])
                    downloads["client"].download_file(downloads["bucket"], downloads["rat"], downloads["rat_dest"])
                else:
                    shutil.copy(downloads["geotiff"], downloads["geotiff_dest"])
                    shutil.copy(downloads["rat"], downloads["rat_dest"])
                if not os.path.isfile(downloads["geotiff_dest"]) or not os.path.isfile(downloads["rat_dest"]):
                    return {"Tile": downloads["tile"], "Result": False, "Reason": "missing download"}
                with open(downloads["geotiff_dest"], "rb") as f:
                    geotiff_hash = hashlib.sha256(f.read()).hexdigest()
                with open(downloads["rat_dest"], "rb") as f:
                    rat_hash = hashlib.sha256(f.read()).hexdigest()
                if downloads["geotiff_sha256_checksum"] != geotiff_hash or downloads["rat_sha256_checksum"] != rat_hash:
                    return {"Tile": downloads["tile"], "Result": False, "Reason": "incorrect hash"}
            else:
                if downloads["transport"] == "s3":
                    downloads["client"].download_file(downloads["bucket"], downloads["file"], downloads["file_dest"])
                else:
                    shutil.copy(downloads["file"], downloads["file_dest"])
                if not os.path.isfile(downloads["file_dest"]):
                    return {"Tile": downloads["tile"], "Result": False, "Reason": "missing download"}
                with open(downloads["file_dest"], "rb") as f:
                    file_hash = hashlib.sha256(f.read()).hexdigest()
                if downloads["file_sha256_checksum"] != file_hash:
                    return {"Tile": downloads["tile"], "Result": False, "Reason": "incorrect hash"}
        except Exception as e:
            return {"Tile": downloads["tile"], "Result": False, "Reason": "exception"}
        return {"Tile": downloads["tile"], "Result": True, "Reason": "success"}

    print(f"{len(new_tile_list)} tile(s) with new data")
    print(f"{len(missing_tiles)} tile(s) already downloaded are missing locally")
    download_length = len(download_dict.keys())
    results = []
    if download_length:
        print(f"\nFetching {download_length} tiles")
        with tqdm(
            total=download_length,
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} Tiles {elapsed}, {remaining} Est. Time Remaining" "{postfix}",
            desc=f"{data_source} Fetch",
            colour="#0085CA",
            position=0,
            leave=True,
        ) as progress:
            with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count() - 1) as executor:
                for i in executor.map(pull, download_dict.values()):
                    results.append(i)
                    progress.update(1)
    successful_downloads = [download["Tile"] for download in results if download["Result"] == True]
    failed_downloads = [download["Tile"] for download in results if download["Result"] == False]
    failed_verifications = [download["Tile"] for download in results if (download["Result"] == False and download["Reason"] == "incorrect hash")]

    if len(successful_downloads) > 0:
        update_records(conn, download_dict, successful_downloads, cfg)

    return (
        list(set(tiles_found)),
        list(set(tiles_not_found)),
        successful_downloads,
        failed_downloads,
        existing_tiles,
        missing_tiles,
        failed_verifications,
        new_tile_list,
    )


def _geometry_to_datasource(geom):
    """Wrap an OGR Geometry in an in-memory DataSource with EPSG:4326."""
    driver = ogr.GetDriverByName("MEMORY")
    ds = driver.CreateDataSource("geom_input")
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    lyr = ds.CreateLayer("geometry", srs, geom.GetGeometryType())
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(geom)
    lyr.CreateFeature(feat)
    return ds


def _bbox_to_datasource(xmin, ymin, xmax, ymax):
    """Build a polygon DataSource from bounding box coordinates."""
    if xmin >= xmax:
        raise ValueError(f"xmin ({xmin}) must be less than xmax ({xmax})")
    if ymin >= ymax:
        raise ValueError(f"ymin ({ymin}) must be less than ymax ({ymax})")
    ring = ogr.Geometry(ogr.wkbLinearRing)
    ring.AddPoint_2D(xmin, ymin)
    ring.AddPoint_2D(xmax, ymin)
    ring.AddPoint_2D(xmax, ymax)
    ring.AddPoint_2D(xmin, ymax)
    ring.AddPoint_2D(xmin, ymin)
    poly = ogr.Geometry(ogr.wkbPolygon)
    poly.AddGeometry(ring)
    return _geometry_to_datasource(poly)


def _wkt_to_datasource(wkt):
    """Build a DataSource from a WKT geometry string."""
    try:
        geom = ogr.CreateGeometryFromWkt(wkt)
    except RuntimeError as exc:
        raise ValueError(f"Invalid WKT geometry: {wkt}") from exc
    if geom is None:
        raise ValueError(f"Invalid WKT geometry: {wkt}")
    return _geometry_to_datasource(geom)


def _geojson_to_datasource(geojson_str):
    """Build a DataSource from a GeoJSON geometry or Feature string."""
    try:
        obj = json.loads(geojson_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid GeoJSON: {exc}") from exc
    if obj.get("type") == "Feature":
        geojson_str = json.dumps(obj["geometry"])
    geom = ogr.CreateGeometryFromJson(geojson_str)
    if geom is None:
        raise ValueError(f"Invalid GeoJSON geometry: {geojson_str}")
    return _geometry_to_datasource(geom)


def parse_geometry_input(geom_input):
    """Parse a geometry input string into an OGR DataSource.

    Accepts four formats (all string inputs assume EPSG:4326):

    1. File path — any GDAL-compatible vector file (shapefile, gpkg, geojson)
    2. Bounding box — ``xmin,ymin,xmax,ymax`` (four comma-separated floats)
    3. WKT — ``POLYGON((...))`` or any OGC WKT geometry type
    4. GeoJSON — ``{"type":"Polygon",...}`` or a GeoJSON Feature object

    Parameters
    ----------
    geom_input : str
        The geometry specification in one of the formats above.

    Returns
    -------
    ogr.DataSource
        An in-memory (or file-backed) OGR DataSource containing the geometry.

    Raises
    ------
    ValueError
        If the input cannot be parsed as any recognized format.
    """
    # 1. File path
    if os.path.isfile(geom_input):
        ds = ogr.Open(geom_input)
        if ds is None:
            raise ValueError(f"Unable to open geometry file: {geom_input}")
        return ds

    # 2. Bounding box (4 comma-separated floats)
    parts = geom_input.split(",")
    if len(parts) == 4:
        try:
            coords = [float(p.strip()) for p in parts]
            return _bbox_to_datasource(*coords)
        except ValueError:
            pass  # Not valid floats — fall through to other formats

    # 3. WKT
    wkt_keywords = (
        "POINT", "LINESTRING", "POLYGON",
        "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
    )
    if geom_input.strip().upper().startswith(wkt_keywords):
        return _wkt_to_datasource(geom_input.strip())

    # 4. GeoJSON
    if geom_input.strip().startswith("{"):
        return _geojson_to_datasource(geom_input.strip())

    raise ValueError(
        f"'{geom_input}' is not a recognized geometry format and does not "
        "exist as a file. Accepted formats: file path, "
        "bounding box (xmin,ymin,xmax,ymax), WKT, or GeoJSON string."
    )


def get_tile_list(desired_area, tile_scheme_filename):
    """
    Get the list of tiles inside the given polygon(s).

    Parameters
    ----------
    desired_area : str | ogr.DataSource
        A GDAL-compatible file path or an already-opened OGR DataSource
        denoting geometries that reflect the region of interest.
    tile_scheme_filename : str
        A GDAL-compatible file path denoting geometries that reflect the
        tessellation scheme with addressing information for the desired tiles.

    Returns
    -------
    feature_list : list[dict]
        List of tiles intersecting with the provided polygon(s).
    """
    if isinstance(desired_area, ogr.DataSource):
        data_source = desired_area
    else:
        data_source = ogr.Open(desired_area)
        if data_source is None:
            print("Unable to open desired area file")
            return None
    source = ogr.Open(tile_scheme_filename)
    if source is None:
        print("Unable to open tile scheme file")
        return None
    driver = ogr.GetDriverByName("MEMORY")
    intersect = driver.CreateDataSource("memData")
    intersect_lyr = intersect.CreateLayer("mem", geom_type=ogr.wkbPolygon)
    source_layer = source.GetLayer(0)
    source_crs = source_layer.GetSpatialRef()
    num_target_layers = data_source.GetLayerCount()
    feature_list = []
    for layer_num in range(num_target_layers):
        target_layer = data_source.GetLayer(layer_num)
        target_crs = target_layer.GetSpatialRef()
        if target_crs is None:
            raise ValueError(
                "Geometry input has no CRS defined. "
                "File-based inputs must include coordinate reference system information."
            )
        same_crs = target_crs.IsSame(source_crs)
        if not same_crs:
            transformed_input = transform_layer(target_layer, source_crs)
            target_layer = transformed_input.GetLayer(0)
        target_layer.Intersection(source_layer, intersect_lyr)
        if not same_crs:
            transformed_input = None
        lyr_defn = intersect_lyr.GetLayerDefn()
        for feature in intersect_lyr:
            fields = {}
            for idx in range(lyr_defn.GetFieldCount()):
                fields[lyr_defn.GetFieldDefn(idx).name] = feature.GetField(idx)
            feature_list.append(fields)
    return feature_list


def transform_layer(input_layer: ogr.Layer, desired_crs: osr.SpatialReference) -> ogr.DataSource:
    """
    Transform a provided ogr layer to the provided coordinate reference system.

    Parameters
    ----------
    input_layer : ogr.Layer
        the ogr layer to be transformed.
    desired_crs : osr.SpatialReference
        the coordinate system for the transform.

    Returns
    -------
    out_ds : ogr.DataSource
        transformed ogr memory datasource.
    """
    target_crs = input_layer.GetSpatialRef()
    coord_trans = osr.CoordinateTransformation(target_crs, desired_crs)
    driver = ogr.GetDriverByName("MEMORY")
    out_ds = driver.CreateDataSource("memData")
    out_lyr = out_ds.CreateLayer("out_lyr", geom_type=input_layer.GetGeomType())
    out_defn = out_lyr.GetLayerDefn()
    in_feature = input_layer.GetNextFeature()
    while in_feature:
        geom = in_feature.GetGeometryRef()
        geom.Transform(coord_trans)
        out_feature = ogr.Feature(out_defn)
        out_feature.SetGeometry(geom)
        out_lyr.CreateFeature(out_feature)
        out_feature = None
        in_feature = input_layer.GetNextFeature()
    return out_ds


def update_records(conn: sqlite3.Connection, download_dict: dict,
                   successful_downloads: list, cfg: dict) -> None:
    """
    Update the registry DB after a successful download batch.

    Within a single transaction:

    1. Updates the ``tiles`` table with disk paths and sets verified flags.
    2. Upserts ``vrt_subregion`` records with built flags set to 0 (forcing
       ``build_vrt`` to rebuild).
    3. Upserts ``vrt_utm`` records with built flags set to 0.

    The upserts use ``ON CONFLICT ... DO UPDATE`` so that existing subregion
    and UTM records have their built flags reset when new tile data arrives.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    download_dict : dict
        Keyed by tilename; values are dicts with download metadata (disk
        paths, subregion, utm, etc.).
    successful_downloads : list[str]
        Tilenames that were successfully downloaded and verified.
    cfg : dict
        Data source configuration.
    """
    file_layout = cfg["file_layout"]
    vrt_subregion_fields = get_vrt_subregion_fields(cfg)
    vrt_utm_fields = get_vrt_utm_fields(cfg)

    # Build column lists (excluding PKs)
    sr_cols = [k for k in vrt_subregion_fields.keys()]
    utm_cols = [k for k in vrt_utm_fields.keys()]

    tiles_records = []
    subregion_records = []
    utm_records = []

    for tilename in download_dict:
        if tilename in successful_downloads:
            if file_layout == "dual_file":
                tiles_records.append((
                    download_dict[tilename]["geotiff_disk"],
                    download_dict[tilename]["rat_disk"],
                    "True", "True", tilename
                ))
            else:
                tiles_records.append((
                    download_dict[tilename]["file_disk"],
                    "True", tilename
                ))

            # Build subregion record: region, utm, then Nones for all vrt columns, then 0 for built flags
            sr_values = [download_dict[tilename]["subregion"], download_dict[tilename]["utm"]]
            for col in sr_cols:
                if col in ("region", "utm"):
                    continue
                if "built" in col:
                    sr_values.append(0)
                else:
                    sr_values.append(None)
            subregion_records.append(tuple(sr_values))

            # Build utm record: utm, then Nones for vrt columns, then 0 for built flags
            utm_values = [download_dict[tilename]["utm"]]
            for col in utm_cols:
                if col == "utm":
                    continue
                if "built" in col:
                    utm_values.append(0)
                else:
                    utm_values.append(None)
            utm_records.append(tuple(utm_values))

    if len(tiles_records) == 0:
        return

    cursor = conn.cursor()
    cursor.execute("BEGIN TRANSACTION;")

    # Update tiles
    if file_layout == "dual_file":
        cursor.executemany(
            """UPDATE tiles
               SET geotiff_disk = ?, rat_disk = ?,
               geotiff_verified = ?, rat_verified = ?
               WHERE tilename = ?""",
            tiles_records,
        )
    else:
        cursor.executemany(
            """UPDATE tiles
               SET file_disk = ?,
               file_verified = ?
               WHERE tilename = ?""",
            tiles_records,
        )

    # Upsert subregion records
    sr_col_names = ", ".join(sr_cols)
    sr_placeholders = ", ".join(["?"] * len(sr_cols))
    sr_update_parts = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in sr_cols if col != "region"
    )
    cursor.executemany(
        f"""INSERT INTO vrt_subregion({sr_col_names})
            VALUES({sr_placeholders})
            ON CONFLICT(region) DO UPDATE
            SET {sr_update_parts}""",
        subregion_records,
    )

    # Upsert utm records
    utm_col_names = ", ".join(utm_cols)
    utm_placeholders = ", ".join(["?"] * len(utm_cols))
    utm_update_parts = ", ".join(
        f"{col} = EXCLUDED.{col}" for col in utm_cols if col != "utm"
    )
    cursor.executemany(
        f"""INSERT INTO vrt_utm({utm_col_names})
            VALUES({utm_placeholders})
            ON CONFLICT(utm) DO UPDATE
            SET {utm_update_parts}""",
        utm_records,
    )

    cursor.execute("COMMIT;")
    conn.commit()


def insert_new(conn: sqlite3.Connection, tiles: list, cfg: dict) -> int:
    """
    Insert newly discovered tile names into the ``tiles`` table.

    Tiles are filtered to include only those with valid delivery data.
    Raw geopackage field names are normalized to standard lowercase names
    using ``tilescheme_field_map`` (when present) before filtering.

    For ``dual_file`` sources, tiles must have ``delivered_date``,
    ``geotiff_link``, and ``rat_link``.  For ``single_file`` sources,
    tiles must have ``delivered_date`` and a non-None ``file_link``.

    Uses ``ON CONFLICT DO NOTHING`` so existing tiles are not affected.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    tiles : list[dict]
        Tile records from :func:`get_tile_list` (field names match the
        geopackage columns).
    cfg : dict
        Data source configuration.

    Returns
    -------
    int
        Number of tiles that passed the delivery filter.
    """
    cursor = conn.cursor()
    file_layout = cfg["file_layout"]
    field_map = cfg["tilescheme_field_map"]

    # Normalize gpkg field names to standard lowercase names
    normalized = []
    for tile in tiles:
        norm = {k.lower(): v for k, v in tile.items()}
        if field_map:
            for std_name, gpkg_name in field_map.items():
                norm[std_name] = norm.get(gpkg_name, norm.get(std_name))
        normalized.append(norm)

    if file_layout == "dual_file":
        tile_list = [
            (tile["tile"],) for tile in normalized
            if tile.get("delivered_date") and tile.get("geotiff_link") and tile.get("rat_link")
        ]
    else:
        tile_list = [
            (tile["tile"],) for tile in normalized
            if tile.get("delivered_date")
            and tile.get("file_link")
            and str(tile.get("file_link", "")).lower() != "none"
        ]

    cursor.executemany(
        """INSERT INTO tiles(tilename)
                          VALUES(?) ON CONFLICT DO NOTHING""",
        tile_list,
    )
    conn.commit()
    return len(tile_list)


def all_db_tiles(conn: sqlite3.Connection) -> list:
    """
    Retrieve all tile records in tiles table of SQLite database.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.

    Returns
    -------
    list
        all tile records as dictionaries.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles")
    return [dict(row) for row in cursor.fetchall()]


def upsert_tiles(conn: sqlite3.Connection, project_dir: str, tile_scheme: str,
                 cfg: dict) -> None:
    """
    Synchronize tile records with the latest tilescheme deliveries.

    For every tile already in the DB, compares the ``delivered_date`` against
    the tilescheme.  If the tilescheme has a newer delivery:

    1. Old tile files are removed from disk.
    2. The tile's subregion is determined by spatial intersection with a
       generated global 1.2-degree tileset.
    3. The tile record is upserted with updated links, checksums, and
       delivery date; disk and verified fields are cleared to force
       re-download.

    For sources with a ``tilescheme_field_map``, geopackage field names are
    mapped to standard names (``tile``, ``file_link``, ``delivered_date``,
    etc.) before processing.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    project_dir : str
        Absolute path to the project directory.
    tile_scheme : str
        Path to the downloaded tile-scheme geopackage.
    cfg : dict
        Data source configuration.
    """
    file_layout = cfg["file_layout"]
    field_map = cfg["tilescheme_field_map"]
    disk_fields = get_disk_fields(cfg)

    db_tiles = all_db_tiles(conn)
    ts_ds = ogr.Open(tile_scheme)
    ts_lyr = ts_ds.GetLayer()
    ts_defn = ts_lyr.GetLayerDefn()
    ts_tiles = []
    for ft in ts_lyr:
        field_list = {}
        geom = ft.GetGeometryRef()
        field_list["wkt_geom"] = geom.ExportToWkt()
        for field_num in range(ts_defn.GetFieldCount()):
            field_name = ts_defn.GetFieldDefn(field_num).name
            field_list[field_name.lower()] = ft.GetField(field_name)
        if field_map:
            # Map tilescheme fields to standard names
            for std_name, ts_name in field_map.items():
                field_list[std_name] = ft.GetField(ts_name)
        ts_tiles.append(field_list)
    ts_ds = None

    global_tileset = global_region_tileset(1, "1.2")
    gs = ogr.Open(global_tileset)
    lyr = gs.GetLayer()
    insert_tiles = []

    for db_tile in db_tiles:
        ts_tile = [t for t in ts_tiles if db_tile["tilename"] == t["tile"]]
        if len(ts_tile) == 0:
            print(f"Warning: {db_tile['tilename']} in database appears to have "
                  "been removed from latest tilescheme")
            continue
        if len(ts_tile) > 1:
            raise ValueError(f"More than one tilename {db_tile['tilename']} "
                             "found in tileset.\n"
                             "Please alert NBS.\n"
                             f"{debug_info}")
        ts_tile = ts_tile[0]

        if ts_tile["delivered_date"] is None:
            print("Warning: Unexpected removal of delivered date "
                  f"for tile {db_tile['tilename']}")
            continue

        if (db_tile["delivered_date"] is None) or (ts_tile["delivered_date"] > db_tile["delivered_date"]):
            try:
                for df in disk_fields:
                    if db_tile.get(df) and os.path.isfile(os.path.join(project_dir, db_tile[df])):
                        os.remove(os.path.join(project_dir, db_tile[df]))
            except (OSError, PermissionError) as e:
                print(f"Failed to remove older files for tile "
                      f"{db_tile['tilename']}. Please close all files and "
                      "attempt fetch again.")
                gdal.Unlink(global_tileset)
                raise e

            lyr.SetSpatialFilter(ogr.CreateGeometryFromWkt(ts_tile["wkt_geom"]))
            if lyr.GetFeatureCount() != 1:
                gdal.Unlink(global_tileset)
                raise ValueError(f"Error getting subregion for "
                                 f"{db_tile['tilename']}. \n"
                                 f"{lyr.GetFeatureCount()} subregion(s). \n"
                                 f"{debug_info}")
            region_ft = lyr.GetNextFeature()
            ts_tile["region"] = region_ft.GetField("Region")

            if file_layout == "dual_file":
                insert_tiles.append((
                    ts_tile["tile"],
                    ts_tile["geotiff_link"],
                    ts_tile["rat_link"],
                    ts_tile["delivered_date"],
                    ts_tile["resolution"],
                    ts_tile["utm"],
                    ts_tile["region"],
                    ts_tile["geotiff_sha256_checksum"],
                    ts_tile["rat_sha256_checksum"],
                ))
            else:
                insert_tiles.append((
                    ts_tile["tile"],
                    ts_tile["file_link"],
                    ts_tile["delivered_date"],
                    ts_tile["resolution"],
                    ts_tile["utm"],
                    ts_tile["region"],
                    ts_tile["file_sha256_checksum"],
                ))

    if insert_tiles:
        cursor = conn.cursor()
        if file_layout == "dual_file":
            cursor.executemany(
                """
                INSERT INTO tiles(tilename, geotiff_link, rat_link,
                delivered_date, resolution, utm, subregion,
                geotiff_sha256_checksum, rat_sha256_checksum)
                VALUES(?, ?, ? ,? ,? ,?, ?, ?, ?)
                ON CONFLICT(tilename) DO UPDATE
                SET geotiff_link = EXCLUDED.geotiff_link,
                rat_link = EXCLUDED.rat_link,
                delivered_date = EXCLUDED.delivered_date,
                resolution = EXCLUDED.resolution,
                utm = EXCLUDED.utm,
                subregion = EXCLUDED.subregion,
                geotiff_sha256_checksum = EXCLUDED.geotiff_sha256_checksum,
                rat_sha256_checksum = EXCLUDED.rat_sha256_checksum,
                geotiff_verified = Null,
                rat_verified = Null,
                geotiff_disk = Null,
                rat_disk = Null
                """,
                insert_tiles,
            )
        else:
            cursor.executemany(
                """
                INSERT INTO tiles(tilename, file_link,
                delivered_date, resolution, utm, subregion,
                file_sha256_checksum)
                VALUES(?, ?, ? ,? ,? ,?, ?)
                ON CONFLICT(tilename) DO UPDATE
                SET file_link = EXCLUDED.file_link,
                delivered_date = EXCLUDED.delivered_date,
                resolution = EXCLUDED.resolution,
                utm = EXCLUDED.utm,
                subregion = EXCLUDED.subregion,
                file_sha256_checksum = EXCLUDED.file_sha256_checksum,
                file_verified = Null,
                file_disk = Null
                """,
                insert_tiles,
            )
        conn.commit()
    gdal.Unlink(global_tileset)


def convert_base(charset: str, input: int, minimum: int) -> str:
    """
    Convert integer to new base system using the given symbols with a
    minimum length filled using leading characters of the lowest value in the
    given charset.

    Parameters
    ----------
    charset : str
        length of this str will be the new base system and characters
        given will be the symbols used.
    input : int
        integer to convert.
    minimum : int
        returned output will be adjusted to this desired length using
        leading characters of the lowest value in charset.

    Returns
    -------
    str
        converted value in given system.
    """
    res = ""
    while input:
        res += charset[input % len(charset)]
        input //= len(charset)
    return (res[::-1] or charset[0]).rjust(minimum, charset[0])


def global_region_tileset(index: int, size: str) -> str:
    """
    Generate a global tilescheme.

    Parameters
    ----------
    index : int
        index of tileset to determine tilescheme name.
    size : str
        length of the side of an individual tile in degrees.

    Returns
    -------
    location : str
        gdal memory filepath to global tilescheme.
    """
    charset = "BCDFGHJKLMNPQRSTVWXZ"
    name = convert_base(charset, index, 2)
    roundnum = len(size.split(".")[1])
    size = float(size)
    location = "/vsimem/global_tileset.gpkg"
    ds = ogr.GetDriverByName("GPKG").CreateDataSource(location)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    layer = ds.CreateLayer("global_tileset", srs, ogr.wkbMultiPolygon)
    layer.CreateFields(
        [
            ogr.FieldDefn("Region", ogr.OFTString),
            ogr.FieldDefn("UTM_Zone", ogr.OFTInteger),
            ogr.FieldDefn("Hemisphere", ogr.OFTString),
        ]
    )
    layer_defn = layer.GetLayerDefn()
    layer.StartTransaction()
    y = round(-90 + size, roundnum)
    y_count = 0
    while y <= 90:
        ns = "N"
        if y <= 0:
            ns = "S"
        x = -180
        x_count = 0
        while x < 180:
            current_utm = "{:02d}".format(int(np.ceil((180 + x + 0.00000001) / 6)))
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint_2D(x, y)
            ring.AddPoint_2D(round(x + size, roundnum), y)
            ring.AddPoint_2D(round(x + size, roundnum), round(y - size, roundnum))
            ring.AddPoint_2D(x, round(y - size, roundnum))
            ring.AddPoint_2D(x, y)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            poly = poly.Buffer(-0.002)
            multipoly = ogr.Geometry(ogr.wkbMultiPolygon)
            multipoly.AddGeometry(poly)
            feat = ogr.Feature(layer_defn)
            feat.SetGeometry(multipoly)
            charset = "2456789BCDFGHJKLMNPQRSTVWXZ"
            x_rep = convert_base(charset, x_count, 3)
            y_rep = convert_base(charset, y_count, 3)
            feat.SetField("Region", f"{name}{x_rep}{y_rep}")
            feat.SetField("UTM_Zone", current_utm)
            feat.SetField("Hemisphere", ns)
            layer.CreateFeature(feat)
            x = round(x + size, roundnum)
            x_count += 1
        y = round(y + size, roundnum)
        y_count += 1
    layer.CommitTransaction()
    return location


def sweep_files(conn: sqlite3.Connection, project_dir: str, cfg: dict) -> tuple:
    """
    Untrack tiles whose files are missing from disk.

    For each tile that references a file no longer on disk:

    1. The tile is deleted from ``tiles``.
    2. Any remaining file for that tile is also removed.
    3. Subregion and UTM records that no longer have any downloaded tiles
       are deleted, along with their associated VRT/OVR files.

    This is triggered by the ``--untrack`` CLI flag and is useful for
    cleaning up after manually deleting tile files.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection.
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration.

    Returns
    -------
    tuple[int, int, int]
        ``(untracked_tiles, untracked_subregions, untracked_utms)``
    """
    disk_fields_list = get_disk_fields(cfg)
    vrt_cols = get_vrt_file_columns(cfg)
    utm_cols = get_utm_file_columns(cfg)

    db_tiles = all_db_tiles(conn)
    cursor = conn.cursor()
    untracked_tiles = 0
    untracked_subregions = 0
    untracked_utms = 0

    for fields in db_tiles:
        # Check if any disk field references a missing file
        has_missing = False
        for df in disk_fields_list:
            if fields.get(df) and not os.path.isfile(os.path.join(project_dir, fields[df])):
                has_missing = True
                break
        if not has_missing:
            continue

        cursor.execute(
            "DELETE FROM tiles where tilename = ? RETURNING *",
            (fields["tilename"],),
        )
        del_tile = cursor.fetchone()
        if del_tile:
            untracked_tiles += 1
            for df in disk_fields_list:
                try:
                    if del_tile[df] and os.path.isfile(os.path.join(project_dir, del_tile[df])):
                        os.remove(os.path.join(project_dir, del_tile[df]))
                except (OSError, PermissionError):
                    continue

            # Build WHERE clause for subregion deletion based on schema
            disk_not_null = " AND ".join(f"{df} is not null" for df in disk_fields_list)
            cursor.execute(
                f"""DELETE FROM vrt_subregion
                    WHERE region NOT IN
                    (SELECT subregion FROM tiles WHERE {disk_not_null})
                    RETURNING *;"""
            )
            del_subregions = cursor.fetchall()
            untracked_subregions += len(del_subregions)
            for del_subregion in del_subregions:
                for col in vrt_cols:
                    try:
                        if del_subregion[col] and os.path.isfile(os.path.join(project_dir, del_subregion[col])):
                            os.remove(os.path.join(project_dir, del_subregion[col]))
                    except (OSError, PermissionError):
                        continue

            cursor.execute(
                f"""DELETE FROM vrt_utm
                    WHERE utm NOT IN
                    (SELECT utm FROM tiles WHERE {disk_not_null})
                    RETURNING *;"""
            )
            del_utms = cursor.fetchall()
            untracked_utms += len(del_utms)
            for del_utm in del_utms:
                for col in utm_cols:
                    try:
                        if del_utm[col] and os.path.isfile(os.path.join(project_dir, del_utm[col])):
                            os.remove(os.path.join(project_dir, del_utm[col]))
                    except (OSError, PermissionError):
                        continue
            conn.commit()
    return untracked_tiles, untracked_subregions, untracked_utms


def main(
    project_dir: str,
    desired_area_filename: str = None,
    untrack_missing: bool = False,
    data_source: str = None,
) -> tuple:
    """
    Main entry point: discover, download, and update NBS tiles.

    Orchestrates the full fetch workflow:

    1. Resolve data source config (named source or local directory).
    2. Download tessellation geopackage and optional XML catalog.
    3. Optionally untrack tiles with missing files (``--untrack``).
    4. If a geometry file is provided, intersect it with the tile scheme
       to discover and begin tracking new tiles.
    5. Synchronize tile records with the latest tilescheme deliveries.
    6. Download all pending tiles with checksum verification.
    7. Print a summary of results.

    Can also be used with a local directory path as *data_source* (e.g.
    ``"/path/to/HSD_tiles"``).  The directory must contain a
    ``*_Tile_Scheme*.gpkg`` file; the source name is inferred from the
    geopackage filename prefix.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.  Created if it does not exist.
    desired_area_filename : str | None
        Path to a geometry file (shapefile, geopackage, etc.) defining the
        area of interest.  Tiles intersecting this area are added to
        tracking.  Pass None to skip discovery and only update existing tiles.
    untrack_missing : bool
        If True, remove tiles from tracking whose files are missing locally.
    data_source : str | None
        A known source name (e.g. ``"bluetopo"``, ``"bag"``, ``"s102v30"``),
        a local directory path, or None (defaults to ``"bluetopo"``).

    Returns
    -------
    tuple[list, list]
        ``(successful_downloads, tiles_not_found_or_failed)``
    """
    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)
    if desired_area_filename:
        _is_path_like = (
            os.path.sep in desired_area_filename
            or desired_area_filename.startswith("~")
            or os.path.isfile(desired_area_filename)
        )
        if _is_path_like:
            desired_area_filename = os.path.expanduser(desired_area_filename)
            if not os.path.isabs(desired_area_filename):
                msg = "Please use an absolute path for your geometry path."
                if "windows" not in platform.system().lower():
                    msg += "\nTypically for non windows systems this means starting with '/'"
                raise ValueError(msg)

    # Resolve data source config
    local_dir = None
    if data_source is None:
        data_source = "bluetopo"

    try:
        cfg = get_config(data_source)
        if cfg["geom_prefix"] is None and cfg["tile_prefix"] is None:
            raise ValueError(
                f"{data_source} is a local-only data source. "
                "Please provide a local directory path instead of the source name."
            )
        data_source = cfg["canonical_name"]
        geom_prefix = cfg["geom_prefix"]
    except ValueError:
        if not os.path.isdir(data_source):
            raise
        local_dir = data_source
        geom_prefix = local_dir
        files = os.listdir(geom_prefix)
        files = [f for f in files if f.endswith(".gpkg") and "Tile_Scheme" in f]
        files.sort(reverse=True)
        resolved_name = None
        for f in files:
            resolved_name = os.path.basename(f).split("_")[0]
            break
        if resolved_name is None:
            raise ValueError("Please pass in directory which contains a tile scheme file if you're using a local data source.")
        cfg = get_local_config(resolved_name)
        data_source = cfg["canonical_name"]

    tile_prefix = cfg["tile_prefix"]

    start = datetime.datetime.now()
    print(f"[{_timestamp()}] {data_source}: Beginning work in project folder: {project_dir}")
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)

    conn = connect_to_survey_registry(project_dir, cfg)

    # Download XML if needed
    xml_prefix = cfg.get("xml_prefix")
    if xml_prefix:
        get_xml(conn, project_dir, xml_prefix, data_source, cfg)

    # Download tessellation
    geom_file = get_tessellation(conn, project_dir, geom_prefix, data_source, cfg,
                                 local_dir=local_dir)

    if untrack_missing:
        untracked_tiles, untracked_sr, untracked_utms = sweep_files(conn, project_dir, cfg)
        print(f"Untracked {untracked_tiles} tile(s), "
              f"{untracked_sr} subregion vrt(s), "
              f"{untracked_utms} utm vrt(s)")

    if desired_area_filename:
        desired_area_ds = parse_geometry_input(desired_area_filename)
        tile_list = get_tile_list(desired_area_ds, geom_file)
        if tile_list is None:
            tile_list = []
        available_tile_count = insert_new(conn, tile_list, cfg)
        print(f"\nTracking {available_tile_count} available {data_source} tile(s) "
              f"discovered in a total of {len(tile_list)} intersected tile(s) "
              "with given polygon.")

    upsert_tiles(conn, project_dir, geom_file, cfg)

    (
        tiles_found,
        tiles_not_found,
        successful_downloads,
        failed_downloads,
        existing_tiles,
        missing_tiles,
        failed_verifications,
        new_tile_list,
    ) = download_tiles(conn, project_dir, tile_prefix, data_source, cfg,
                       local_dir=local_dir)

    print("\n___________________________________ SUMMARY ___________________________________")
    print("\nExisting:")
    print(
        "Number of tiles already existing locally without updates:",
        len(existing_tiles),
    )
    if new_tile_list or missing_tiles:
        print("\nSearch:")
        print(f"Number of tiles to attempt to fetch: {len(new_tile_list) + len(missing_tiles)} [ {len(new_tile_list)} new data + {len(missing_tiles)} missing locally ]")
        if len(tiles_found) < (len(new_tile_list) + len(missing_tiles)):
            print("* Some tiles we wanted to fetch were not found in the S3 bucket."
                  "\n* The NBS may be actively updating the tiles in question."
                  "\n* You can rerun fetch_tiles at a later time to download these tiles."
                  "\n* Please contact the NBS if this issue does not fix itself on subsequent later runs.")
        print("\nFetch:")
        print(f"Number of tiles found in S3 successfully downloaded: {len(successful_downloads)}/{len(tiles_found)}")
        if len(failed_downloads):
            print("* Some tiles appear to have failed downloading."
                  "\n* Please rerun fetch_tiles to retry.")
            if len(failed_verifications):
                print(f"{len(failed_verifications)} tiles failed checksum verification: {failed_verifications}"
                      f"\nPlease contact the NBS if this issue does not fix itself on subsequent runs.")
    print(f"\n[{_timestamp()}] {data_source}: Operation complete after {datetime.datetime.now() - start}")
    return successful_downloads, list(set(tiles_not_found + failed_downloads))
