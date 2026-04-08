# Data Sources

This package supports six S3-hosted data sources from the NOAA National Bathymetric Source (NBS) project that can be fetched by name.

## S3 sources

These sources are fetched from NOAA's public S3 bucket. Pass the source key as the `data_source` argument.

| Source | Format | Bands | Min GDAL | RAT | Description |
|---|---|---|---|---|---|
| `bluetopo` | GeoTIFF | Elevation, Uncertainty, Contributor | 3.4 | Yes | National bathymetric compilation (public) |
| `modeling` | GeoTIFF | Elevation, Uncertainty, Contributor | 3.4 | Yes | Bathymetric compilation for modeling (low water datum) |
| `bag` | BAG (.bag) | Elevation, Uncertainty | 3.4 | No | Bathymetric Attributed Grid |
| `s102v21` | S-102 HDF5 (.h5) | Elevation, Uncertainty | 3.9 | No | IHO S-102 v2.1 bathymetric surface |
| `s102v22` | S-102 HDF5 (.h5) | Elevation, Uncertainty + QualityOfSurvey | 3.9 | Yes | IHO S-102 v2.2 with quality subdataset |
| `s102v30` | S-102 HDF5 (.h5) | Elevation, Uncertainty + QualityOfBathymetryCoverage | 3.9 | Yes | IHO S-102 v3.0 with quality subdataset |

---

## BlueTopo

[BlueTopo](https://www.nauticalcharts.noaa.gov/data/bluetopo.html) is a compilation of the best available public bathymetric data of U.S. waters. Created by [NOAA Office of Coast Survey's](https://www.nauticalcharts.noaa.gov/) National Bathymetric Source project, it provides depth information nationwide with the vertical uncertainty tied to that depth estimate as well as information on the survey source.

It is the default and most commonly used data source.

**Source key:** `bluetopo`

**File slots:**

| Slot | Geopackage field | File type |
|---|---|---|
| `geotiff` | `GeoTIFF_Link` | `.tiff` |
| `rat` | `RAT_Link` | `.tiff.aux.xml` |

**Bands:** Elevation, Uncertainty, Contributor

**RAT fields** (on band 3 — Contributor):

| Field | Type | Description |
|---|---|---|
| `value` | int | Contributor class value |
| `count` | int | Pixel count for this class |
| `data_assessment` | int | Data quality assessment code |
| `feature_least_depth` | float | Least depth of feature |
| `significant_features` | float | Significant feature indicator |
| `feature_size` | float | Feature size |
| `coverage` | int | Coverage flag |
| `bathy_coverage` | int | Bathymetric coverage flag |
| `horizontal_uncert_fixed` | float | Fixed horizontal uncertainty |
| `horizontal_uncert_var` | float | Variable horizontal uncertainty |
| `vertical_uncert_fixed` | float | Fixed vertical uncertainty |
| `vertical_uncert_var` | float | Variable vertical uncertainty |
| `license_name` | str | Source data license name |
| `license_url` | str | Source data license URL |
| `source_survey_id` | str | Source survey identifier |
| `source_institution` | str | Source institution name |
| `survey_date_start` | str | Survey start date |
| `survey_date_end` | str | Survey end date |

---

## Modeling

Test-and-evaluation bathymetric compilation for coastal and ocean modeling. Uses the same file structure as BlueTopo, but data is on a **low water datum** instead of NAVD88/mean sea level.

**Source key:** `modeling`

**File slots:** Same as BlueTopo (GeoTIFF + RAT auxiliary file)

**Bands:** Elevation, Uncertainty, Contributor

**RAT fields:** Same as BlueTopo

---

## BAG

Bathymetric Attributed Grid — a single-file format with elevation and uncertainty bands.

**Source key:** `bag`

**Required GDAL drivers:** `BAG`

**File slots:**

| Slot | Geopackage field | File type |
|---|---|---|
| `file` | `BAG` | `.bag` |

**Bands:** Elevation, Uncertainty

**RAT:** None

> **Note:** BAG files require GDAL built with HDF5 support. Install `libgdal-hdf5` via conda if the BAG driver is not available.

---

> **Note:** The S-102 data available through this package are for test and evaluation and should not be used for navigation. For official S-102 data, see the [data](https://noaa-s102-pds.s3.amazonaws.com/index.html) available from [Precision Marine Navigation](https://oceanservice.noaa.gov/navigation/precision-navigation/).

## S102 v2.1

IHO S-102 bathymetric surface, version 2.1. Single-file HDF5 format with two bands.

**Source key:** `s102v21`

**Required GDAL drivers:** `S102`

**Min GDAL version:** 3.9

**File slots:**

| Slot | Geopackage field | File type |
|---|---|---|
| `file` | `S102V21` | `.h5` |

**Bands:** Elevation, Uncertainty

**RAT:** None

Requires a CATALOG.XML file downloaded from S3 (handled automatically by `fetch_tiles`).

---

## S102 v2.2

IHO S-102 v2.2 with dual subdatasets: bathymetry coverage and a quality-of-survey layer. The mosaic build produces separate subdataset VRTs plus a combined VRT.

**Source key:** `s102v22`

**Required GDAL drivers:** `S102`

**Min GDAL version:** 3.9

**File slots:**

| Slot | Geopackage field | File type |
|---|---|---|
| `file` | `S102V22` | `.h5` |

**Subdatasets:**

| Subdataset | Bands | VRT suffix |
|---|---|---|
| BathymetryCoverage | Elevation, Uncertainty | `_BathymetryCoverage` |
| QualityOfSurvey | QualityOfSurvey | `_QualityOfSurvey` |

A combined VRT is also created with all three bands (Elevation, Uncertainty, QualityOfSurvey).

**RAT fields** (on band 3 of the combined VRT):

| Field | Type | Description |
|---|---|---|
| `value` | int | Quality class value |
| `data_assessment` | int | Data quality assessment code |
| `feature_least_depth` | float | Least depth of feature |
| `significant_features` | float | Significant feature indicator |
| `feature_size` | str | Feature size |
| `feature_size_var` | int | Feature size variance (set to 0 in mosaic) |
| `coverage` | int | Coverage flag |
| `bathy_coverage` | int | Bathymetric coverage flag |
| `horizontal_uncert_fixed` | float | Fixed horizontal uncertainty |
| `horizontal_uncert_var` | float | Variable horizontal uncertainty |
| `survey_date_start` | str | Survey start date |
| `survey_date_end` | str | Survey end date |
| `source_survey_id` | str | Source survey identifier |
| `source_institution` | str | Source institution name |
| `bathymetric_uncertainty_type` | int | Uncertainty type classification (set to 0 in mosaic) |

> **Note:** `feature_size_var` and `bathymetric_uncertainty_type` are unsupported fields and are set to 0 in mosaics.

---

## S102 v3.0

IHO S-102 v3.0 — similar to v2.2, but with a renamed quality subdataset.

**Source key:** `s102v30`

**Required GDAL drivers:** `S102`

**Min GDAL version:** 3.9

**File slots:**

| Slot | Geopackage field | File type |
|---|---|---|
| `file` | `S102V30` | `.h5` |

**Subdatasets:**

| Subdataset | Bands | VRT suffix |
|---|---|---|
| BathymetryCoverage | Elevation, Uncertainty | `_BathymetryCoverage` |
| QualityOfBathymetryCoverage | QualityOfBathymetryCoverage | `_QualityOfBathymetryCoverage` |

A combined VRT is also created with all three bands.

**RAT fields** (on band 3 of the combined VRT):

| Field | Type | Description |
|---|---|---|
| `value` | int | Quality class value |
| `data_assessment` | int | Data quality assessment code |
| `feature_least_depth` | float | Least depth of feature |
| `significant_features` | float | Significant feature indicator |
| `feature_size` | str | Feature size |
| `feature_size_var` | int | Feature size variance (set to 0 in mosaic) |
| `coverage` | int | Coverage flag |
| `bathy_coverage` | int | Bathymetric coverage flag |
| `horizontal_uncert_fixed` | float | Fixed horizontal uncertainty |
| `horizontal_uncert_var` | float | Variable horizontal uncertainty |
| `survey_date_start` | str | Survey start date |
| `survey_date_end` | str | Survey end date |
| `source_survey_id` | str | Source survey identifier |
| `source_institution` | str | Source institution name |
| `type_of_bathymetric_estimation_uncertainty` | int | Uncertainty type classification (set to 0 in mosaic) |

> **Note:** `feature_size_var` and `type_of_bathymetric_estimation_uncertainty` are unsupported fields and are set to 0 in mosaics.

