"""
config.py - Configuration-driven data source definitions.

All data source variation is captured here. Adding a new NBS product
requires only a new entry in ``DATA_SOURCES`` -- no new functions in
mosaic_tiles or fetch_tiles.

File slots
----------
Each data source defines one or more **file slots** describing the files
that make up a tile. Each slot maps a geopackage field (where the S3 URL
or local path lives) to a set of DB columns (link, disk path, checksum,
verified flag).  This replaces the old ``file_layout`` / ``tilescheme_field_map``
branching with a single, uniform model.

Examples::

    # BlueTopo: two files per tile
    "file_slots": [
        {"name": "geotiff", "gpkg_link": "GeoTIFF_Link",
         "gpkg_checksum": "GeoTIFF_SHA256_Checksum"},
        {"name": "rat", "gpkg_link": "RAT_Link",
         "gpkg_checksum": "RAT_SHA256_Checksum"},
    ]

    # BAG: one file per tile
    "file_slots": [
        {"name": "file", "gpkg_link": "BAG", "gpkg_checksum": "BAG_SHA256"},
    ]

Each slot generates four DB columns: ``{name}_link``, ``{name}_disk``,
``{name}_sha256_checksum``, ``{name}_verified`` (integer 0/1).

Config key reference
--------------------
canonical_name : str
    Display name used in logs, folder names, and the registry DB filename.
min_gdal_version : int
    Minimum GDAL version encoded as ``major*1_000_000 + minor*10_000``
    (e.g. 3090000 = GDAL 3.9).
required_gdal_drivers : list[str]
    GDAL driver short names that must be available (e.g. ``["S102"]``).
geom_prefix : str | None
    S3 key prefix for the tile-scheme geopackage.  None for local-only sources.
xml_prefix : str | None
    S3 key prefix for the CATALOG.XML (S102 sources only).
bucket : str
    S3 bucket name.
catalog_table : str
    Name of the catalog table in the SQLite registry (always ``"catalog"``).
catalog_pk : str
    Primary key column of the catalog table (always ``"name"``).
gpkg_fields : dict
    Maps standard metadata names (``tile``, ``delivered_date``, ``utm``,
    ``resolution``) to geopackage column names.
file_slots : list[dict]
    Per-file definitions (see above).
subdatasets : list[dict] | None
    For multi-subdataset sources (S102V22, S102V30).
band_descriptions : list[str] | None
    Band labels for single-dataset sources.
has_rat : bool
    Whether to build a Raster Attribute Table on UTM mosaics.
rat_open_method : str | None
    ``"direct"`` or ``"s102_quality"``.
rat_band : int | None
    1-based band index for RAT read/write.
rat_fields : dict | None
    ``{field_name: [python_type, gdal_usage]}`` for RAT columns.
rat_zero_fields : list[str]
    Fields forced to 0 during RAT aggregation.
"""

import copy
import datetime
import os

from osgeo import gdal


# ---------------------------------------------------------------------------
# Geopackage field mappings (metadata only; file fields are in file_slots)
# ---------------------------------------------------------------------------

# BlueTopo / Modeling / HSD use these field names directly
_BLUETOPO_GPKG_FIELDS = {
    "tile": "tile",
    "delivered_date": "Delivered_Date",
    "utm": "UTM",
    "resolution": "Resolution",
}

# Navigation tile scheme (shared by BAG, S102V21, S102V22, S102V30)
_NAVIGATION_GPKG_FIELDS = {
    "tile": "TILE_ID",
    "delivered_date": "ISSUANCE",
    "utm": "UTM",
    "resolution": "Resolution",
}


# ---------------------------------------------------------------------------
# Data source configurations
# ---------------------------------------------------------------------------

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
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _BLUETOPO_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "geotiff", "gpkg_link": "GeoTIFF_Link",
             "gpkg_checksum": "GeoTIFF_SHA256_Checksum"},
            {"name": "rat", "gpkg_link": "RAT_Link",
             "gpkg_checksum": "RAT_SHA256_Checksum"},
        ],
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
        "rat_zero_fields": [],
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": True,
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
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _BLUETOPO_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "geotiff", "gpkg_link": "GeoTIFF_Link",
             "gpkg_checksum": "GeoTIFF_SHA256_Checksum"},
            {"name": "rat", "gpkg_link": "RAT_Link",
             "gpkg_checksum": "RAT_SHA256_Checksum"},
        ],
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
        "rat_zero_fields": [],
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": True,
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
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _NAVIGATION_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "file", "gpkg_link": "BAG", "gpkg_checksum": "BAG_SHA256"},
        ],
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty"],
        # RAT
        "has_rat": False,
        "rat_open_method": None,
        "rat_band": None,
        "rat_fields": None,
        "rat_zero_fields": [],
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": False,
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
        "xml_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V21/_CATALOG",
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _NAVIGATION_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "file", "gpkg_link": "S102V21", "gpkg_checksum": "S102V21_SHA256"},
        ],
        # Subdatasets
        "subdatasets": None,
        "band_descriptions": ["Elevation", "Uncertainty"],
        # RAT
        "has_rat": False,
        "rat_open_method": None,
        "rat_band": None,
        "rat_fields": None,
        "rat_zero_fields": [],
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": False,
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
        "xml_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V22/_CATALOG",
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _NAVIGATION_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "file", "gpkg_link": "S102V22", "gpkg_checksum": "S102V22_SHA256"},
        ],
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
            "feature_size": [float, gdal.GFU_Generic],
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
        # Maps lowercased HDF5 featureAttributeTable field names to
        # output RAT column names (keys of rat_fields).
        "rat_hdf5_to_field": {
            "id": "value",
            "dataassessment": "data_assessment",
            "featuresdetected.leastdepthofdetectedfeaturesmeasured": "feature_least_depth",
            "featuresdetected.significantfeaturesdetected": "significant_features",
            "featuresdetected.sizeoffeaturesdetected": "feature_size",
            "featuresizevar": "feature_size_var",
            "fullseafloorcoverageachieved": "coverage",
            "bathycoverage": "bathy_coverage",
            "zoneofconfidence.horizontalpositionuncertainty.uncertaintyfixed": "horizontal_uncert_fixed",
            "zoneofconfidence.horizontalpositionuncertainty.uncertaintyvariablefactor": "horizontal_uncert_var",
            "surveydaterange.datestart": "survey_date_start",
            "surveydaterange.dateend": "survey_date_end",
            "sourcesurveyid": "source_survey_id",
            "surveyauthority": "source_institution",
            "bathymetricuncertaintytype": "bathymetric_uncertainty_type",
        },
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": False,
    },
    # -------------------------------------------------------------------------
    # S102 v3.0 -- dual subdatasets (BathymetryCoverage +
    #              QualityOfBathymetryCoverage)
    # -------------------------------------------------------------------------
    "s102v30": {
        "canonical_name": "S102V30",
        "min_gdal_version": 3090000,
        "required_gdal_drivers": ["S102"],
        # AWS
        "geom_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/_Navigation_Tile_Scheme/Navigation_Tile_Scheme",
        "xml_prefix": "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V30/_CATALOG",
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _NAVIGATION_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "file", "gpkg_link": "S102V30", "gpkg_checksum": "S102V30_SHA256"},
        ],
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
            "feature_size": [float, gdal.GFU_Generic],
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
        "rat_hdf5_to_field": {
            "id": "value",
            "dataassessment": "data_assessment",
            "featuresdetected.leastdepthofdetectedfeaturesmeasured": "feature_least_depth",
            "featuresdetected.significantfeaturesdetected": "significant_features",
            "featuresdetected.sizeoffeaturesdetected": "feature_size",
            "featuresizevar": "feature_size_var",
            "fullseafloorcoverageachieved": "coverage",
            "bathycoverage": "bathy_coverage",
            "zoneofconfidence.horizontalpositionuncertainty.uncertaintyfixed": "horizontal_uncert_fixed",
            "zoneofconfidence.horizontalpositionuncertainty.uncertaintyvariablefactor": "horizontal_uncert_var",
            "surveydaterange.datestart": "survey_date_start",
            "surveydaterange.dateend": "survey_date_end",
            "sourcesurveyid": "source_survey_id",
            "surveyauthority": "source_institution",
            "typeofbathymetricestimationuncertainty": "type_of_bathymetric_estimation_uncertainty",
        },
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": False,
    },
    # -------------------------------------------------------------------------
    # HSD -- Hydrographic Surveys Division (local-only)
    # -------------------------------------------------------------------------
    "hsd": {
        "canonical_name": "HSD",
        "min_gdal_version": 3040000,
        "required_gdal_drivers": [],
        "geom_prefix": None,
        "xml_prefix": None,
        "bucket": "noaa-ocs-nationalbathymetry-pds",
        # DB schema
        "catalog_table": "catalog",
        "catalog_pk": "name",
        # Geopackage field mapping
        "gpkg_fields": _BLUETOPO_GPKG_FIELDS,
        # File slots
        "file_slots": [
            {"name": "geotiff", "gpkg_link": "GeoTIFF_Link",
             "gpkg_checksum": "GeoTIFF_SHA256_Checksum"},
            {"name": "rat", "gpkg_link": "RAT_Link",
             "gpkg_checksum": "RAT_SHA256_Checksum"},
        ],
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
        "rat_zero_fields": [],
        # Overviews
        "overview_levels": [8, 16, 32, 64, 128, 256, 512],
        "overview_filter_coarsest": True,
    },
}


# Master ordered dict of all known direct-method RAT fields.
# HSD superset: BlueTopo's 18 fields + 5 HSD extras.
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

def parse_resolution(raw):
    """Extract integer meters from a resolution string like ``'4m'``.

    Parameters
    ----------
    raw : str | None
        Resolution value from the geopackage (e.g. ``"4m"``, ``"16"``).

    Returns
    -------
    int | None
        Parsed resolution in meters, or None if *raw* is empty or
        contains no digits.

    Note
    ----
    Only integer resolutions are supported.  All NBS resolutions
    are integers.
    """
    if not raw:
        return None
    digits = ''.join(c for c in str(raw) if c.isdigit())
    if not digits:
        return None
    return int(digits)


def make_resolution_label(resolutions):
    """Build a display label like ``'4m'`` or ``'4m_8m'`` from resolution values.

    Values are sorted ascending before joining (e.g. ``[8, 4]`` → ``'4m_8m'``).
    """
    return "_".join(f"{r}m" for r in sorted(resolutions))


def make_mosaic_dir_name(data_source, tile_resolution_filter=None,
                         mosaic_resolution_target=None, reproject=False,
                         output_dir=None):
    """Build the mosaic output directory name from build parameters.

    When *output_dir* is provided, it is returned directly, overriding
    the auto-generated name.

    Parameters
    ----------
    data_source : str
        Canonical data source name (e.g. ``"BlueTopo"``).
    tile_resolution_filter : list[int] | None
        Active resolution filter, appended as ``_4m_8m``.
    mosaic_resolution_target : float | None
        Target pixel size, appended as ``_tr8m``.
    reproject : bool
        If True, appended as ``_3857`` for Web Mercator output.
    output_dir : str | None
        Custom output directory name. Overrides auto-generated name.

    Returns
    -------
    str
        Directory name, e.g. ``'BlueTopo_Mosaic'``, ``'BlueTopo_Mosaic_4m_8m'``,
        ``'BlueTopo_Mosaic_3857'``, ``'BlueTopo_Mosaic_4m_8m_3857'``, or a custom name.
    """
    if output_dir is not None:
        return output_dir
    name = f"{data_source}_Mosaic"
    if tile_resolution_filter:
        name += f"_{make_resolution_label(tile_resolution_filter)}"
    if mosaic_resolution_target is not None:
        res_str = f"{mosaic_resolution_target:g}".replace(".", "p")
        name += f"_tr{res_str}m"
    if reproject:
        name += "_3857"
    return name


def make_params_key(data_source, tile_resolution_filter=None,
                    mosaic_resolution_target=None, reproject=False):
    """Derive the ``params_key`` string used to partition ``mosaic_utm`` rows.

    The key is the suffix portion of the mosaic directory name. Default
    (unparameterized) builds use ``""``; parameterized builds get a key
    like ``"_4m_8m"`` or ``"_4m_8m_tr8m"`` or ``"_3857"``.
    """
    dir_name = make_mosaic_dir_name(data_source, tile_resolution_filter,
                                 mosaic_resolution_target, reproject)
    return dir_name.removeprefix(f"{data_source}_Mosaic")


def validate_mosaic_resolution_target(value):
    """Raise ``ValueError`` if *value* is not None and not positive."""
    if value is not None and value <= 0:
        raise ValueError(
            f"mosaic_resolution_target must be a positive number, got {value}"
        )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def validate_config(cfg):
    """Validate interdependencies in a data source configuration dict.

    Checks that required keys exist, file_slots are well-formed, RAT
    settings are consistent with ``has_rat``, and subdatasets/band_descriptions
    are mutually exclusive.

    Raises
    ------
    ValueError
        If the config is inconsistent or missing required fields.
    """
    name = cfg.get("canonical_name", "?")

    if not cfg.get("file_slots"):
        raise ValueError(f"{name}: file_slots must be non-empty")

    for slot in cfg["file_slots"]:
        if "name" not in slot or "gpkg_link" not in slot or "gpkg_checksum" not in slot:
            raise ValueError(f"{name}: each file_slot must have 'name', 'gpkg_link', and 'gpkg_checksum'")

    if not cfg.get("gpkg_fields"):
        raise ValueError(f"{name}: gpkg_fields must be defined")

    for required_key in ("tile", "delivered_date", "utm", "resolution"):
        if required_key not in cfg["gpkg_fields"]:
            raise ValueError(f"{name}: gpkg_fields missing required key '{required_key}'")

    if cfg.get("has_rat"):
        for key in ("rat_open_method", "rat_band", "rat_fields"):
            if not cfg.get(key):
                raise ValueError(f"{name}: has_rat=True requires '{key}'")
        if cfg["rat_open_method"] not in ("direct", "s102_quality"):
            raise ValueError(f"{name}: unknown rat_open_method '{cfg['rat_open_method']}'")

    if cfg.get("subdatasets") and cfg.get("band_descriptions"):
        raise ValueError(f"{name}: cannot have both subdatasets and band_descriptions")

    if not cfg.get("subdatasets") and not cfg.get("band_descriptions"):
        raise ValueError(f"{name}: must have either subdatasets or band_descriptions")


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _timestamp():
    """Return current time as ``'YYYY-MM-DD HH:MM:SS TZ'``."""
    now = datetime.datetime.now()
    return f"{now.strftime('%Y-%m-%d %H:%M:%S')} {now.astimezone().tzname()}"



# ---------------------------------------------------------------------------
# Config access
# ---------------------------------------------------------------------------

def get_config(data_source_key):
    """Look up a data source by name and return a validated deep copy.

    Parameters
    ----------
    data_source_key : str
        Data source name (case-insensitive), e.g. ``"bluetopo"``, ``"bag"``.

    Returns
    -------
    dict
        Deep copy of the matching ``DATA_SOURCES`` entry, validated
        via :func:`validate_config`.

    Raises
    ------
    ValueError
        If *data_source_key* does not match any known source.
    """
    key = data_source_key.lower()
    if key not in DATA_SOURCES:
        raise ValueError(f"Unknown data source: {data_source_key}")
    cfg = copy.deepcopy(DATA_SOURCES[key])
    validate_config(cfg)
    return cfg


def get_local_config(resolved_name):
    """Build a config for a local directory data source.

    If *resolved_name* matches a known source, that source's config is
    used as the base.  Otherwise, BlueTopo is used with the full
    ``KNOWN_RAT_FIELDS`` superset for dynamic field detection.

    S3 prefixes (``geom_prefix``, ``xml_prefix``) are cleared so the
    pipeline skips S3 operations and reads from local files instead.

    Parameters
    ----------
    resolved_name : str
        Source name extracted from the tile-scheme geopackage filename
        (e.g. ``"HSD"`` from ``HSD_Tile_Scheme_2024.gpkg``).

    Returns
    -------
    dict
        Validated config dict with S3 prefixes set to None.
    """
    key = resolved_name.lower()
    if key in DATA_SOURCES:
        cfg = copy.deepcopy(DATA_SOURCES[key])
    else:
        cfg = copy.deepcopy(DATA_SOURCES["bluetopo"])
        cfg["rat_fields"] = copy.deepcopy(KNOWN_RAT_FIELDS)
        cfg["canonical_name"] = resolved_name
    cfg["geom_prefix"] = None
    cfg["xml_prefix"] = None
    validate_config(cfg)
    return cfg


def resolve_data_source(data_source):
    """Resolve a data source name or local directory path into a config.

    Returns
    -------
    tuple[dict, str | None]
        ``(cfg, local_dir)`` where *local_dir* is None for S3 sources.
    """
    if data_source is None:
        data_source = "bluetopo"
    try:
        cfg = get_config(data_source)
        if cfg["geom_prefix"] is None:
            raise ValueError(
                f"{data_source} is a local-only data source. "
                "Please provide a local directory path instead of the source name."
            )
        return cfg, None
    except ValueError:
        if not os.path.isdir(data_source):
            raise
        files = os.listdir(data_source)
        files = [f for f in files if f.endswith(".gpkg") and "Tile_Scheme" in f]
        files.sort(reverse=True)
        if not files:
            raise ValueError(
                "Please pass in directory which contains a tile scheme "
                "file if you're using a local data source."
            )
        resolved_name = os.path.basename(files[0]).split("_")[0]
        cfg = get_local_config(resolved_name)
        return cfg, data_source


# ---------------------------------------------------------------------------
# Schema helpers (derived from file_slots — no branching)
# ---------------------------------------------------------------------------

def get_catalog_fields(cfg):
    """Return ``{column_name: sql_type}`` for the catalog table.

    Catalog tracks downloaded tessellation and XML assets, not tiles.
    """
    return {cfg["catalog_pk"]: "text", "location": "text", "downloaded": "text"}


def get_mosaic_fields(cfg):
    """Return ``{column_name: sql_type}`` for the ``mosaic_utm`` table.

    Schema varies by source: subdataset sources get per-subdataset mosaic/OVR
    columns plus a combined mosaic column; single-dataset sources get one
    mosaic/OVR pair.
    """
    fields = {"utm": "text", "params_key": "text", "output_dir": "text"}
    if cfg["subdatasets"]:
        for i in range(len(cfg["subdatasets"])):
            fields[f"utm_subdataset{i+1}_mosaic"] = "text"
            fields[f"utm_subdataset{i+1}_mosaic_disk_file_size"] = "integer"
            fields[f"utm_subdataset{i+1}_ovr"] = "text"
            fields[f"utm_subdataset{i+1}_ovr_disk_file_size"] = "integer"
        fields["utm_combined_mosaic"] = "text"
        fields["utm_combined_mosaic_disk_file_size"] = "integer"
    else:
        fields["utm_mosaic"] = "text"
        fields["utm_mosaic_disk_file_size"] = "integer"
        fields["utm_ovr"] = "text"
        fields["utm_ovr_disk_file_size"] = "integer"
    fields["utm_aux_xml"] = "text"
    fields["utm_aux_xml_disk_file_size"] = "integer"
    fields["hillshade"] = "text"
    fields["hillshade_disk_file_size"] = "integer"
    fields["hillshade_resolution"] = "real"
    # Build metadata (not file paths)
    fields["tile_count"] = "integer"
    fields["tile_count_plus_overviews"] = "integer"
    fields["mosaic_resolution"] = "real"
    fields["overview_count"] = "integer"
    fields["overview_resolutions"] = "text"
    for res in (2, 4, 8, 16, 32, 64):
        fields[f"tiles_{res}m"] = "integer"
    fields["built_timestamp"] = "text"
    fields["build_duration_seconds"] = "real"
    # Built flags
    if cfg["subdatasets"]:
        for i in range(len(cfg["subdatasets"])):
            fields[f"built_subdataset{i+1}"] = "integer"
        fields["built_combined"] = "integer"
    else:
        fields["built"] = "integer"
    fields["built_hillshade"] = "integer"
    return fields


def get_tiles_fields(cfg):
    """Return ``{column_name: sql_type}`` for the ``tiles`` table.

    Columns are derived from file_slots.  Verified flags use integer (0/1).
    """
    fields = {
        "tilename": "text",
        "delivered_date": "text",
        "resolution": "text",
        "utm": "text",
    }
    for slot in cfg["file_slots"]:
        name = slot["name"]
        fields[f"{name}_link"] = "text"
        fields[f"{name}_disk"] = "text"
        fields[f"{name}_sha256_checksum"] = "text"
        fields[f"{name}_verified"] = "integer"
        fields[f"{name}_disk_file_size"] = "integer"
    fields["downloaded_timestamp"] = "text"
    fields["geometry"] = "text"
    return fields


def get_mosaic_built_flags(cfg):
    """Return built-flag column names from ``mosaic_utm`` (e.g. ``["built"]``)."""
    if cfg["subdatasets"]:
        flags = [f"built_subdataset{i+1}" for i in range(len(cfg["subdatasets"]))]
        flags.append("built_combined")
        return flags
    return ["built"]


def get_all_reset_flags(cfg):
    """Return all built-flag column names including hillshade for invalidation."""
    return get_mosaic_built_flags(cfg) + ["built_hillshade"]


def get_utm_file_columns(cfg):
    """Return mosaic/OVR path column names from ``mosaic_utm``, excluding PK, built flags, and metadata."""
    fields = get_mosaic_fields(cfg)
    exclude = {
        "utm", "params_key", "output_dir",
        "hillshade", "hillshade_disk_file_size", "hillshade_resolution",
        "tile_count", "tile_count_plus_overviews",
        "mosaic_resolution", "overview_count", "overview_resolutions",
        "tiles_2m", "tiles_4m", "tiles_8m", "tiles_16m",
        "tiles_32m", "tiles_64m", "built_timestamp",
        "build_duration_seconds",
    }
    exclude.update(get_all_reset_flags(cfg))
    exclude.update(k for k in fields if k.endswith("_disk_file_size"))
    return [k for k in fields if k not in exclude]


def get_disk_field(cfg):
    """Return the primary disk-path column name (first file slot)."""
    return f"{cfg['file_slots'][0]['name']}_disk"


def get_disk_fields(cfg):
    """Return all disk-path column names."""
    return [f"{slot['name']}_disk" for slot in cfg["file_slots"]]


def get_verified_fields(cfg):
    """Return all verified-flag column names."""
    return [f"{slot['name']}_verified" for slot in cfg["file_slots"]]


def get_link_fields(cfg):
    """Return all link column names."""
    return [f"{slot['name']}_link" for slot in cfg["file_slots"]]


def get_checksum_fields(cfg):
    """Return all checksum column names."""
    return [f"{slot['name']}_sha256_checksum" for slot in cfg["file_slots"]]
