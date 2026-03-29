"""
spatial.py - Geometry parsing, CRS transforms, and tile intersection.

Handles user-provided geometry inputs (file paths, bounding boxes, WKT,
GeoJSON) and intersects them with tile-scheme geopackages to discover
which tiles cover the area of interest.
"""

import json
import logging
import os

from osgeo import ogr, osr

logger = logging.getLogger("noaabathymetry")

# "Memory" was merged into "MEM" in GDAL 3.11; older versions only know "Memory".
_ogr_mem_driver = ogr.GetDriverByName("MEM")
if _ogr_mem_driver is None:
    _ogr_mem_driver = ogr.GetDriverByName("Memory")


def _geometry_to_datasource(geom):
    """Wrap an OGR Geometry in a single-feature in-memory DataSource with EPSG:4326."""
    driver = _ogr_mem_driver
    ds = driver.CreateDataSource("geom_input")
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    lyr = ds.CreateLayer("geometry", srs, geom.GetGeometryType())
    feat = ogr.Feature(lyr.GetLayerDefn())
    feat.SetGeometry(geom)
    lyr.CreateFeature(feat)
    return ds


def _bbox_to_datasource(xmin, ymin, xmax, ymax):
    """Build a rectangular polygon DataSource from bounding box coordinates (EPSG:4326)."""
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

    1. File path -- any GDAL-compatible vector file (shapefile, gpkg, geojson)
    2. Bounding box -- ``xmin,ymin,xmax,ymax`` (four comma-separated floats)
    3. WKT -- ``POLYGON((...))`` or any OGC WKT geometry type
    4. GeoJSON -- ``{"type":"Polygon",...}`` or a GeoJSON Feature object

    Parameters
    ----------
    geom_input : str
        The geometry specification in one of the formats above.

    Returns
    -------
    ogr.DataSource

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
    """Return tile records from the tile-scheme geopackage that intersect the geometry.

    Handles multi-layer inputs and reprojects to the tile scheme's CRS
    before computing the intersection.

    Parameters
    ----------
    desired_area : str | ogr.DataSource
        A GDAL-compatible file path or an already-opened OGR DataSource.
    tile_scheme_filename : str
        Path to the tile-scheme geopackage.

    Returns
    -------
    list[dict] | None
        Tile attribute dicts for intersecting tiles, or None if either
        input cannot be opened.
    """
    if isinstance(desired_area, ogr.DataSource):
        data_source = desired_area
    else:
        data_source = ogr.Open(desired_area)
        if data_source is None:
            logger.warning("Unable to open desired area file")
            return None
    source = ogr.Open(tile_scheme_filename)
    if source is None:
        logger.warning("Unable to open tile scheme file")
        return None
    driver = _ogr_mem_driver
    source_layer = source.GetLayer(0)
    source_crs = source_layer.GetSpatialRef()
    num_target_layers = data_source.GetLayerCount()
    feature_list = []
    for layer_num in range(num_target_layers):
        intersect = driver.CreateDataSource(f"intersect_{layer_num}")
        intersect_lyr = intersect.CreateLayer("mem", geom_type=ogr.wkbPolygon)
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


def transform_layer(input_layer, desired_crs):
    """Reproject all features in an OGR layer to the given CRS.

    Creates a new in-memory DataSource containing the transformed
    geometries (attribute fields are not copied since only geometry is
    needed for spatial intersection).

    Parameters
    ----------
    input_layer : ogr.Layer
        The OGR layer to be reprojected.
    desired_crs : osr.SpatialReference
        The target coordinate reference system.

    Returns
    -------
    ogr.DataSource
        In-memory DataSource with geometries in *desired_crs*.
    """
    target_crs = input_layer.GetSpatialRef()
    coord_trans = osr.CoordinateTransformation(target_crs, desired_crs)
    driver = _ogr_mem_driver
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
