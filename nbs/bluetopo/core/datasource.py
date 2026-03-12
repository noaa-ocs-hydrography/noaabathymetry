"""
datasource.py - Configuration-driven data source definitions.

All data source variation is captured here. Adding a new NBS product
(e.g. S102V30) requires only a new entry in ``DATA_SOURCES`` -- no new
functions in build_vrt or fetch_tiles.

File layouts
------------
dual_file
    Two files per tile: a GeoTIFF (.tif) and a Raster Attribute Table
    (.tif.aux.xml).  Used by BlueTopo, Modeling, and HSD.
single_file
    One file per tile (BAG or S102 HDF5).  Used by BAG, S102V21, S102V22, and S102V30.

Download strategies
-------------------
prefix_listing
    Tiles are discovered by listing S3 objects under ``tile_prefix/{tilename}/``.
    Used by BlueTopo and Modeling.
direct_link
    Tile locations are stored as direct links in the tilescheme geopackage.
    Used by BAG, S102V21, S102V22, S102V30, HSD, and unknown local sources.

Config key reference
--------------------
canonical_name : str
    Display name used in logs, folder names, and the registry DB filename.
min_gdal_version : int
    Minimum GDAL version encoded as ``major*1_000_000 + minor*10_000``
    (e.g. 3090000 = GDAL 3.9).
required_gdal_drivers : list[str]
    GDAL driver short names that must be available to build VRTs for this
    source (e.g. ``["BAG"]``, ``["S102"]``).  Empty for GeoTIFF sources.
geom_prefix : str | None
    S3 key prefix for the tile-scheme geopackage.  None for local-only sources.
tile_prefix : str | None
    S3 key prefix for tile data.  None for local-only sources and sources
    that use ``direct_link`` download strategy.
xml_prefix : str | None
    S3 key prefix for the CATALOG.XML (S102 sources only).
bucket : str
    S3 bucket name.
download_strategy : str
    ``"prefix_listing"`` (list S3 objects under tile_prefix) or
    ``"direct_link"`` (tile URLs stored in tilescheme geopackage).
file_layout : str
    ``"dual_file"`` or ``"single_file"`` -- controls DB columns and download logic.
catalog_table : str
    Name of the catalog table in the SQLite registry (``"tileset"`` or ``"catalog"``).
catalog_pk : str
    Primary key column of the catalog table (``"tilescheme"`` or ``"file"``).
subdatasets : list[dict] | None
    For multi-subdataset sources (S102V22, S102V30), a list of dicts each
    containing ``name``, ``suffix``, ``band_descriptions``, and
    ``s102_protocol``.  None for single-dataset sources.
band_descriptions : list[str] | None
    Band description labels for single-dataset sources (e.g. ``["Elevation",
    "Uncertainty", "Contributor"]``).  None when subdatasets are used instead.
has_rat : bool
    Whether to build a GDAL Raster Attribute Table on UTM VRTs.
rat_open_method : str | None
    ``"direct"`` (read RAT from GeoTIFF band) or ``"s102_quality"`` (read
    via ``S102:"path":<quality_group>`` driver, where the quality group name
    comes from the second subdataset's ``name`` field).
rat_band : int | None
    1-based band index where the RAT is read from / written to.
rat_fields : dict | None
    Ordered mapping of ``{field_name: [python_type, gdal_usage]}`` defining the
    RAT column schema.
rat_zero_fields : list[str]
    Fields whose values are forced to 0 during RAT aggregation (S102V22,
    S102V30).
tilescheme_field_map : dict | None
    Maps standard field names (``tile``, ``file_link``, ...) to geopackage
    field names for sources whose tilescheme uses non-standard column names.
    None for BlueTopo/Modeling which use standard names directly.
"""

import copy
import datetime

from osgeo import gdal


DATA_SOURCES = {
    # -------------------------------------------------------------------------
    # BlueTopo -- publicly available national bathymetric compilation
    # -------------------------------------------------------------------------
    "bluetopo": {
        "canonical_name": "BlueTopo",
        "min_gdal_version": 3040000,
        "required_gdal_drivers": [],
        # AWS
        "geom_prefix": "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme",
        "tile_prefix": "BlueTopo",
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "prefix_listing",
        "file_layout": "dual_file",
        "catalog_table": "tileset",
        "catalog_pk": "tilescheme",
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty", "Contributor"],
        # RAT
        "has_rat": True,
        "rat_open_method": "direct",
        "rat_band": 3,
        "rat_fields": {
            "value": [int, gdal.GFU_MinMax],
            "count": [int, gdal.GFU_PixelCount],
            "data_assessment": [int, gdal.GFU_Generic],
            "feature_least_depth": [float, gdal.GFU_Generic],
            "significant_features": [float, gdal.GFU_Generic],
            "feature_size": [float, gdal.GFU_Generic],
            "coverage": [int, gdal.GFU_Generic],
            "bathy_coverage": [int, gdal.GFU_Generic],
            "horizontal_uncert_fixed": [float, gdal.GFU_Generic],
            "horizontal_uncert_var": [float, gdal.GFU_Generic],
            "vertical_uncert_fixed": [float, gdal.GFU_Generic],
            "vertical_uncert_var": [float, gdal.GFU_Generic],
            "license_name": [str, gdal.GFU_Generic],
            "license_url": [str, gdal.GFU_Generic],
            "source_survey_id": [str, gdal.GFU_Generic],
            "source_institution": [str, gdal.GFU_Generic],
            "survey_date_start": [str, gdal.GFU_Generic],
            "survey_date_end": [str, gdal.GFU_Generic],
        },
        # Tilescheme field mapping
        "tilescheme_field_map": None,
    },
    # -------------------------------------------------------------------------
    # Modeling -- test-and-evaluation bathymetric compilation for modeling
    # -------------------------------------------------------------------------
    "modeling": {
        "canonical_name": "Modeling",
        "min_gdal_version": 3040000,
        "required_gdal_drivers": [],
        # AWS
        "geom_prefix": "Test-and-Evaluation/Modeling/_Modeling_Tile_Scheme/Modeling_Tile_Scheme",
        "tile_prefix": "Test-and-Evaluation/Modeling",
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "prefix_listing",
        "file_layout": "dual_file",
        "catalog_table": "tileset",
        "catalog_pk": "tilescheme",
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty", "Contributor"],
        # RAT
        "has_rat": True,
        "rat_open_method": "direct",
        "rat_band": 3,
        "rat_fields": {
            "value": [int, gdal.GFU_MinMax],
            "count": [int, gdal.GFU_PixelCount],
            "data_assessment": [int, gdal.GFU_Generic],
            "feature_least_depth": [float, gdal.GFU_Generic],
            "significant_features": [float, gdal.GFU_Generic],
            "feature_size": [float, gdal.GFU_Generic],
            "coverage": [int, gdal.GFU_Generic],
            "bathy_coverage": [int, gdal.GFU_Generic],
            "horizontal_uncert_fixed": [float, gdal.GFU_Generic],
            "horizontal_uncert_var": [float, gdal.GFU_Generic],
            "vertical_uncert_fixed": [float, gdal.GFU_Generic],
            "vertical_uncert_var": [float, gdal.GFU_Generic],
            "license_name": [str, gdal.GFU_Generic],
            "license_url": [str, gdal.GFU_Generic],
            "source_survey_id": [str, gdal.GFU_Generic],
            "source_institution": [str, gdal.GFU_Generic],
            "survey_date_start": [str, gdal.GFU_Generic],
            "survey_date_end": [str, gdal.GFU_Generic],
        },
        # Tilescheme field mapping
        "tilescheme_field_map": None,
    },
    # -------------------------------------------------------------------------
    # BAG -- Bathymetric Attributed Grid (single-file, no RAT)
    # -------------------------------------------------------------------------
    "bag": {
        "canonical_name": "BAG",
        "min_gdal_version": 3040000,
        "required_gdal_drivers": ["BAG"],
        # AWS
        "geom_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/_Navigation_Tile_Scheme/Navigation_Tile_Scheme",
        "tile_prefix": None,
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "direct_link",
        "file_layout": "single_file",
        "catalog_table": "catalog",
        "catalog_pk": "file",
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty"],
        # RAT
        "has_rat": False,
        "rat_open_method": None,
        "rat_band": None,
        "rat_fields": None,
        # Tilescheme field mapping
        "tilescheme_field_map": {
            "tile": "tile_id",
            "file_link": "bag",
            "file_sha256_checksum": "bag_sha256",
            "delivered_date": "issuance",
            "utm": "utm",
            "resolution": "resolution",
        },
    },
    # -------------------------------------------------------------------------
    # S102 v2.1 -- IHO S-102 bathymetric surface (single-file, no RAT)
    # -------------------------------------------------------------------------
    "s102v21": {
        "canonical_name": "S102V21",
        "min_gdal_version": 3090000,
        "required_gdal_drivers": ["S102"],
        # AWS
        "geom_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/_Navigation_Tile_Scheme/Navigation_Tile_Scheme",
        "tile_prefix": None,
        "xml_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V21/_CATALOG",
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "direct_link",
        "file_layout": "single_file",
        "catalog_table": "catalog",
        "catalog_pk": "file",
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty"],
        # RAT
        "has_rat": False,
        "rat_open_method": None,
        "rat_band": None,
        "rat_fields": None,
        # Tilescheme field mapping
        "tilescheme_field_map": {
            "tile": "tile_id",
            "file_link": "s102v21",
            "file_sha256_checksum": "s102v21_sha256",
            "delivered_date": "issuance",
            "utm": "utm",
            "resolution": "resolution",
        },
    },
    # -------------------------------------------------------------------------
    # S102 v2.2 -- dual subdatasets (BathymetryCoverage + QualityOfSurvey)
    # -------------------------------------------------------------------------
    "s102v22": {
        "canonical_name": "S102V22",
        "min_gdal_version": 3090000,
        "required_gdal_drivers": ["S102"],
        # AWS
        "geom_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/_Navigation_Tile_Scheme/Navigation_Tile_Scheme",
        "tile_prefix": None,
        "xml_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V22/_CATALOG",
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "direct_link",
        "file_layout": "single_file",
        "catalog_table": "catalog",
        "catalog_pk": "file",
        # Subdatasets
        "subdatasets": [
            {
                "name": "BathymetryCoverage",
                "suffix": "_BathymetryCoverage",
                "band_descriptions": ["Elevation", "Uncertainty"],
                "s102_protocol": False,
            },
            {
                "name": "QualityOfSurvey",
                "suffix": "_QualityOfSurvey",
                "band_descriptions": ["QualityOfSurvey"],
                "s102_protocol": True,
            },
        ],
        "band_descriptions": None,
        # RAT
        "has_rat": True,
        "rat_open_method": "s102_quality",
        "rat_band": 3,
        "rat_fields": {
            "value": [int, gdal.GFU_MinMax],
            "data_assessment": [int, gdal.GFU_Generic],
            "feature_least_depth": [float, gdal.GFU_Generic],
            "significant_features": [float, gdal.GFU_Generic],
            "feature_size": [str, gdal.GFU_Generic],
            "feature_size_var": [int, gdal.GFU_Generic],
            "coverage": [int, gdal.GFU_Generic],
            "bathy_coverage": [int, gdal.GFU_Generic],
            "horizontal_uncert_fixed": [float, gdal.GFU_Generic],
            "horizontal_uncert_var": [float, gdal.GFU_Generic],
            "survey_date_start": [str, gdal.GFU_Generic],
            "survey_date_end": [str, gdal.GFU_Generic],
            "source_survey_id": [str, gdal.GFU_Generic],
            "source_institution": [str, gdal.GFU_Generic],
            "bathymetric_uncertainty_type": [int, gdal.GFU_Generic],
        },
        "rat_zero_fields": ["feature_size_var", "bathymetric_uncertainty_type"],
        # Tilescheme field mapping
        "tilescheme_field_map": {
            "tile": "tile_id",
            "file_link": "s102v22",
            "file_sha256_checksum": "s102v22_sha256",
            "delivered_date": "issuance",
            "utm": "utm",
            "resolution": "resolution",
        },
    },
    # -------------------------------------------------------------------------
    # S102 v3.0 -- dual subdatasets (BathymetryCoverage + QualityOfBathymetryCoverage)
    #
    # Differences from v2.2:
    #   - Quality group renamed: QualityOfSurvey → QualityOfBathymetryCoverage
    #   - RAT field 14 renamed: bathymetricUncertaintyType →
    #     typeOfBathymetricEstimationUncertainty
    #   - GDAL S102 driver accepts both quality group names for both versions
    # -------------------------------------------------------------------------
    "s102v30": {
        "canonical_name": "S102V30",
        "min_gdal_version": 3090000,
        "required_gdal_drivers": ["S102"],
        # AWS
        "geom_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/_Navigation_Tile_Scheme/Navigation_Tile_Scheme",
        "tile_prefix": None,
        "xml_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V30/_CATALOG",
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "direct_link",
        "file_layout": "single_file",
        "catalog_table": "catalog",
        "catalog_pk": "file",
        # Subdatasets
        "subdatasets": [
            {
                "name": "BathymetryCoverage",
                "suffix": "_BathymetryCoverage",
                "band_descriptions": ["Elevation", "Uncertainty"],
                "s102_protocol": False,
            },
            {
                "name": "QualityOfBathymetryCoverage",
                "suffix": "_QualityOfBathymetryCoverage",
                "band_descriptions": ["QualityOfBathymetryCoverage"],
                "s102_protocol": True,
            },
        ],
        "band_descriptions": None,
        # RAT
        "has_rat": True,
        "rat_open_method": "s102_quality",
        "rat_band": 3,
        "rat_fields": {
            "value": [int, gdal.GFU_MinMax],
            "data_assessment": [int, gdal.GFU_Generic],
            "feature_least_depth": [float, gdal.GFU_Generic],
            "significant_features": [float, gdal.GFU_Generic],
            "feature_size": [str, gdal.GFU_Generic],
            "feature_size_var": [int, gdal.GFU_Generic],
            "coverage": [int, gdal.GFU_Generic],
            "bathy_coverage": [int, gdal.GFU_Generic],
            "horizontal_uncert_fixed": [float, gdal.GFU_Generic],
            "horizontal_uncert_var": [float, gdal.GFU_Generic],
            "survey_date_start": [str, gdal.GFU_Generic],
            "survey_date_end": [str, gdal.GFU_Generic],
            "source_survey_id": [str, gdal.GFU_Generic],
            "source_institution": [str, gdal.GFU_Generic],
            "type_of_bathymetric_estimation_uncertainty": [int, gdal.GFU_Generic],
        },
        "rat_zero_fields": ["feature_size_var", "type_of_bathymetric_estimation_uncertainty"],
        # Tilescheme field mapping
        "tilescheme_field_map": {
            "tile": "tile_id",
            "file_link": "s102v30",
            "file_sha256_checksum": "s102v30_sha256",
            "delivered_date": "issuance",
            "utm": "utm",
            "resolution": "resolution",
        },
    },
    # -------------------------------------------------------------------------
    # HSD -- Hydrographic Surveys Division (local-only, extends BlueTopo
    # config with extra RAT fields: catzoc, supercession_score, etc.)
    # Must be used via a local directory path, not by name.
    # -------------------------------------------------------------------------
    "hsd": {
        "canonical_name": "HSD",
        "min_gdal_version": 3040000,
        "required_gdal_drivers": [],
        "geom_prefix": None,   # local-only: overridden by local directory path
        "tile_prefix": None,   # local-only: no S3 prefix
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "download_strategy": "direct_link",
        "file_layout": "dual_file",
        "catalog_table": "tileset",
        "catalog_pk": "tilescheme",
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty", "Contributor"],
        # RAT
        "has_rat": True,
        "rat_open_method": "direct",
        "rat_band": 3,
        "rat_fields": {
            "value": [int, gdal.GFU_MinMax],
            "count": [int, gdal.GFU_PixelCount],
            "data_assessment": [int, gdal.GFU_Generic],
            "feature_least_depth": [float, gdal.GFU_Generic],
            "significant_features": [float, gdal.GFU_Generic],
            "feature_size": [float, gdal.GFU_Generic],
            "coverage": [int, gdal.GFU_Generic],
            "bathy_coverage": [int, gdal.GFU_Generic],
            "horizontal_uncert_fixed": [float, gdal.GFU_Generic],
            "horizontal_uncert_var": [float, gdal.GFU_Generic],
            "vertical_uncert_fixed": [float, gdal.GFU_Generic],
            "vertical_uncert_var": [float, gdal.GFU_Generic],
            "license_name": [str, gdal.GFU_Generic],
            "license_url": [str, gdal.GFU_Generic],
            "source_survey_id": [str, gdal.GFU_Generic],
            "source_institution": [str, gdal.GFU_Generic],
            "survey_date_start": [str, gdal.GFU_Generic],
            "survey_date_end": [str, gdal.GFU_Generic],
            "catzoc": [int, gdal.GFU_Generic],
            "supercession_score": [float, gdal.GFU_Generic],
            "decay_score": [float, gdal.GFU_Generic],
            "unqualified": [int, gdal.GFU_Generic],
            "sensitive": [int, gdal.GFU_Generic],
        },
        # Tilescheme field mapping
        "tilescheme_field_map": None,
    },
}


# Master ordered dict of all known direct-method RAT fields.
# This is the HSD superset: BlueTopo's 18 fields + 5 HSD extras.
# Used as the default for unknown local data sources.
KNOWN_RAT_FIELDS = {
    "value": [int, gdal.GFU_MinMax],
    "count": [int, gdal.GFU_PixelCount],
    "data_assessment": [int, gdal.GFU_Generic],
    "feature_least_depth": [float, gdal.GFU_Generic],
    "significant_features": [float, gdal.GFU_Generic],
    "feature_size": [float, gdal.GFU_Generic],
    "coverage": [int, gdal.GFU_Generic],
    "bathy_coverage": [int, gdal.GFU_Generic],
    "horizontal_uncert_fixed": [float, gdal.GFU_Generic],
    "horizontal_uncert_var": [float, gdal.GFU_Generic],
    "vertical_uncert_fixed": [float, gdal.GFU_Generic],
    "vertical_uncert_var": [float, gdal.GFU_Generic],
    "license_name": [str, gdal.GFU_Generic],
    "license_url": [str, gdal.GFU_Generic],
    "source_survey_id": [str, gdal.GFU_Generic],
    "source_institution": [str, gdal.GFU_Generic],
    "survey_date_start": [str, gdal.GFU_Generic],
    "survey_date_end": [str, gdal.GFU_Generic],
    "catzoc": [int, gdal.GFU_Generic],
    "supercession_score": [float, gdal.GFU_Generic],
    "decay_score": [float, gdal.GFU_Generic],
    "unqualified": [int, gdal.GFU_Generic],
    "sensitive": [int, gdal.GFU_Generic],
}


def _timestamp():
    """Return current time as ``'YYYY-MM-DD HH:MM:SS TZ'``."""
    now = datetime.datetime.now()
    return f"{now.strftime('%Y-%m-%d %H:%M:%S')} {now.astimezone().tzname()}"


def get_config(data_source_key):
    """Case-insensitive lookup. Returns a deep copy of config dict. Raises ValueError for unknown sources."""
    key = data_source_key.lower()
    if key not in DATA_SOURCES:
        raise ValueError(f"Unknown data source: {data_source_key}")
    return copy.deepcopy(DATA_SOURCES[key])


def get_local_config(resolved_name):
    """Build a config for a local directory data source.

    If *resolved_name* matches a known source (e.g. ``"BlueTopo"``,
    ``"S102V21"``), that source's config is used as the base so that
    file_layout, subdatasets, RAT settings, etc. are preserved.
    Otherwise, BlueTopo is used as the base with the full known RAT
    field superset, allowing dynamic detection of which fields are
    actually present at RAT-aggregation time.

    In both cases, S3 prefixes are cleared and the download strategy
    is set to ``"direct_link"`` for local file access.
    """
    key = resolved_name.lower()
    if key in DATA_SOURCES:
        cfg = copy.deepcopy(DATA_SOURCES[key])
        # canonical_name already set from the known source config
    else:
        cfg = copy.deepcopy(DATA_SOURCES["bluetopo"])
        cfg["rat_fields"] = copy.deepcopy(KNOWN_RAT_FIELDS)
        cfg["canonical_name"] = resolved_name
    cfg["geom_prefix"] = None
    cfg["tile_prefix"] = None
    cfg["xml_prefix"] = None
    cfg["download_strategy"] = "direct_link"
    return cfg


def get_catalog_fields(cfg):
    """Return ``{column_name: sql_type}`` for the catalog table.

    dual_file layouts use ``tilescheme`` as the PK; single_file use ``file``.
    """
    if cfg["file_layout"] == "dual_file":
        return {"tilescheme": "text", "location": "text", "downloaded": "text"}
    else:
        return {"file": "text", "location": "text", "downloaded": "text"}


def get_vrt_utm_fields(cfg):
    """Return ``{column_name: sql_type}`` for the ``vrt_utm`` table.

    For single-dataset sources: ``utm_vrt``, ``utm_ovr``, ``built``.
    For multi-subdataset sources: per-subdataset VRT/OVR pairs, a
    ``utm_combined_vrt``, and per-subdataset + combined built flags.
    """
    fields = {"utm": "text"}
    if cfg["subdatasets"]:
        for i in range(len(cfg["subdatasets"])):
            fields[f"utm_subdataset{i+1}_vrt"] = "text"
            fields[f"utm_subdataset{i+1}_ovr"] = "text"
        fields["utm_combined_vrt"] = "text"
        for i in range(len(cfg["subdatasets"])):
            fields[f"built_subdataset{i+1}"] = "integer"
        fields["built_combined"] = "integer"
    else:
        fields["utm_vrt"] = "text"
        fields["utm_ovr"] = "text"
        fields["built"] = "integer"
    return fields


def get_tiles_fields(cfg):
    """Return ``{column_name: sql_type}`` for the ``tiles`` table.

    dual_file tiles track separate GeoTIFF and RAT links, disk paths,
    checksums, and verified flags.  single_file tiles track a single file.
    """
    if cfg["file_layout"] == "dual_file":
        return {
            "tilename": "text",
            "geotiff_link": "text",
            "rat_link": "text",
            "delivered_date": "text",
            "resolution": "text",
            "utm": "text",
            "subregion": "text",
            "geotiff_disk": "text",
            "rat_disk": "text",
            "geotiff_sha256_checksum": "text",
            "rat_sha256_checksum": "text",
            "geotiff_verified": "text",
            "rat_verified": "text",
        }
    else:
        return {
            "tilename": "text",
            "file_link": "text",
            "delivered_date": "text",
            "resolution": "text",
            "utm": "text",
            "subregion": "text",
            "file_disk": "text",
            "file_sha256_checksum": "text",
            "file_verified": "text",
        }


def get_built_flags(cfg):
    """Return built-flag column names (e.g. ``["built"]`` or ``["built_subdataset1", ...]``)."""
    if cfg["subdatasets"]:
        return [f"built_subdataset{i+1}" for i in range(len(cfg["subdatasets"]))]
    return ["built"]


def get_utm_file_columns(cfg):
    """Return VRT/OVR path column names from ``vrt_utm`` (excludes PK and built flags)."""
    fields = get_vrt_utm_fields(cfg)
    return [k for k in fields if k != "utm" and "built" not in k]


def get_disk_field(cfg):
    """Return the primary disk-path column name (``"geotiff_disk"`` or ``"file_disk"``)."""
    if cfg["file_layout"] == "dual_file":
        return "geotiff_disk"
    return "file_disk"


def get_disk_fields(cfg):
    """Return all disk-path column names (e.g. ``["geotiff_disk", "rat_disk"]``)."""
    if cfg["file_layout"] == "dual_file":
        return ["geotiff_disk", "rat_disk"]
    return ["file_disk"]


def get_verified_fields(cfg):
    """Return checksum-verified flag column names (e.g. ``["geotiff_verified", "rat_verified"]``)."""
    if cfg["file_layout"] == "dual_file":
        return ["geotiff_verified", "rat_verified"]
    return ["file_verified"]
