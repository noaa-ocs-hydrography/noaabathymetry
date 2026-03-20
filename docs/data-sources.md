# Data Sources

BlueTopo supports six S3-hosted data sources from the NOAA National Bathymetric Source (NBS) project that can be fetched by name. It also supports local tile directories, including HSD and custom data.

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

The default and most commonly used source. A publicly available national bathymetric compilation on NOAA's S3 bucket.

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

IHO S-102 v2.2 with dual subdatasets: bathymetry coverage and a quality-of-survey layer. The VRT build produces separate subdataset VRTs plus a combined VRT.

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
| `feature_size_var` | int | Feature size variance |
| `coverage` | int | Coverage flag |
| `bathy_coverage` | int | Bathymetric coverage flag |
| `horizontal_uncert_fixed` | float | Fixed horizontal uncertainty |
| `horizontal_uncert_var` | float | Variable horizontal uncertainty |
| `survey_date_start` | str | Survey start date |
| `survey_date_end` | str | Survey end date |
| `source_survey_id` | str | Source survey identifier |
| `source_institution` | str | Source institution name |
| `bathymetric_uncertainty_type` | int | Uncertainty type classification |

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
| `feature_size_var` | int | Feature size variance |
| `coverage` | int | Coverage flag |
| `bathy_coverage` | int | Bathymetric coverage flag |
| `horizontal_uncert_fixed` | float | Fixed horizontal uncertainty |
| `horizontal_uncert_var` | float | Variable horizontal uncertainty |
| `survey_date_start` | str | Survey start date |
| `survey_date_end` | str | Survey end date |
| `source_survey_id` | str | Source survey identifier |
| `source_institution` | str | Source institution name |
| `type_of_bathymetric_estimation_uncertainty` | int | Uncertainty type classification |

---

## Local sources

Instead of fetching from S3, you can pass a local directory path as `data_source`. The directory must contain a geopackage with `Tile_Scheme` in its filename. BlueTopo detects the source type from the filename prefix and applies the matching configuration.

```python
result = fetch_tiles('/path/to/project',
                     geometry='aoi.gpkg',
                     data_source='/path/to/local/tiles')
```

How resolution works:

1. BlueTopo scans the directory for `*_Tile_Scheme*.gpkg` files.
2. It extracts the prefix before `_Tile_Scheme` from the filename (e.g. `HSD_Tile_Scheme_2024.gpkg` resolves to `HSD`).
3. If that name matches a known source (`bluetopo`, `modeling`, `bag`, `s102v21`, `s102v22`, `s102v30`, `hsd`), that source's config is used.
4. If it doesn't match, the BlueTopo config is used as a base with an extended RAT field set for dynamic detection.

In all cases, S3 operations are bypassed — tile files are copied from the local directory.

> **Note:** You cannot pass local-only source names (like `hsd`) as the `data_source` argument directly. The name `hsd` exists in the config registry so that local directories with HSD-named geopackages get the correct settings, but `data_source='hsd'` will raise an error because HSD has no S3 prefix.

### HSD

Hydrographic Surveys Division tiles use the same GeoTIFF + RAT structure as BlueTopo, with additional RAT fields for survey quality metadata.

**File slots:** Same as BlueTopo (GeoTIFF + RAT auxiliary file)

**Bands:** Elevation, Uncertainty, Contributor

**RAT fields:** Same as BlueTopo, plus:

| Field | Type | Description |
|---|---|---|
| `catzoc` | int | Category of Zone of Confidence |
| `supercession_score` | float | Survey supercession score |
| `decay_score` | float | Survey decay score |
| `unqualified` | int | Unqualified flag |
| `sensitive` | int | Sensitive data flag |

### Custom sources

Any directory with a geopackage whose filename doesn't match a known source (e.g. `MyProject_Tile_Scheme.gpkg`) is treated as a custom source. It inherits the BlueTopo configuration with the full RAT field superset (all BlueTopo + HSD fields) so that RAT columns can be detected dynamically from the actual tile data.
