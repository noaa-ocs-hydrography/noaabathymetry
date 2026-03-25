[BlueTopo](https://www.nauticalcharts.noaa.gov/data/bluetopo.html) is a compilation of the best available public bathymetric data of U.S. waters. Created by [NOAA Office of Coast Survey's](https://www.nauticalcharts.noaa.gov/) National Bathymetric Source project, it provides depth information nationwide with the vertical uncertainty tied to that depth estimate as well as information on the survey source.

This package simplifies downloading bathymetric tiles from NOAA's public S3 bucket and assembling them into per-UTM-zone GDAL Virtual Rasters for use in GIS applications. It supports six S3-hosted data sources (BlueTopo, Modeling, BAG, S-102 v2.1/v2.2/v3.0).

> **Note:** The S-102 data available through this package are for test and evaluation and should not be used for navigation. For official S-102 data, see the [data](https://noaa-s102-pds.s3.amazonaws.com/index.html) available from [Precision Marine Navigation](https://oceanservice.noaa.gov/navigation/precision-navigation/).

## Who this package is for

This package gives you a straightforward way to get high-resolution bathymetric data from NOAA's National Bathymetric Source.

Point the package at your area of interest and it handles discovery, download, checksum verification, and optional VRT assembly, ready to open in QGIS, ArcGIS, or any GDAL-compatible tool.

Common use cases include:

- **Hydrographic surveying and chart production.** Access the latest compiled depths and survey source metadata.
- **Coastal and ocean modeling.** Pull seamless bathymetry grids as input to storm surge, tsunami, or circulation models.
- **Marine construction and engineering.** Get site-specific depth data for port design, cable routing, or dredging planning.
- **Environmental and habitat research.** Combine bathymetry with other datasets to study benthic environments.
- **Education and exploration.** Visualize the seafloor for teaching, outreach, or personal curiosity.

## Documentation

- **[User Guide](user-guide.md)** — the fetch-then-build workflow, what gets created on disk, and resolution filtering.
- **[Data Sources](data-sources.md)** — each data source with its file format, bands, and GDAL requirements.
- **[API Reference](api-reference.md)** — complete reference for the Python API and CLI commands.
- **[Troubleshooting](troubleshooting.md)** — common errors and how to fix them.
- **[Quickstart Helper](quickstart-helper.md)** — draw your area of interest on a map and generate usage examples.

## Installation

Install conda if you haven't already: [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

Create an environment with the required packages:

```
conda create -n bluetopo_env -c conda-forge 'gdal>=3.9'
conda activate bluetopo_env
pip install bluetopo
```

> **Note:** The `libgdal-hdf5` package is required for BAG and S-102 data sources. If you only need BlueTopo or Modeling data, `gdal>=3.4` alone is sufficient.

## Quick start

After installation, you have access to a Python API and two matching CLI commands: `fetch_tiles` for downloading tiles and `build_vrt` for assembling them into VRTs.

You can use the [Quickstart Helper](quickstart-helper.md) to draw your area of interest on a map and generate usage examples.

### Python API

```python
from nbs.bluetopo import fetch_tiles, build_vrt

result = fetch_tiles('/path/to/project', geometry='area_of_interest.gpkg')
vrt_result = build_vrt('/path/to/project')
```

Both functions return structured result objects ([`FetchResult`](api-reference.md#fetchresult), [`BuildResult`](api-reference.md#buildresult)) you can inspect:

```python
result = fetch_tiles('/path/to/project', geometry='area_of_interest.gpkg')
print(f"Downloaded: {len(result.downloaded)}")
print(f"Already up to date: {len(result.existing)}")
print(f"Failed: {len(result.failed)}")

vrt_result = build_vrt('/path/to/project')
print(f"Built {len(vrt_result.built)} UTM zone VRTs")
print(f"Skipped {len(vrt_result.skipped)} already up-to-date zones")
```

### CLI

The same workflow is available from the command line:

```
fetch_tiles -d /path/to/project -g area_of_interest.gpkg
build_vrt -d /path/to/project
```

### Geometry formats

The `geometry` parameter accepts four formats. File inputs use the CRS defined in the file. All other formats assume EPSG:4326 (WGS 84).

**File** — any GDAL-compatible vector file (shapefile, geopackage, GeoJSON file, etc.):
```python
result = fetch_tiles('/path/to/project', geometry='/path/to/area_of_interest.gpkg')
```

**Bounding box** — `xmin,ymin,xmax,ymax` as longitude/latitude:
```python
result = fetch_tiles('/path/to/project', geometry='-76.1,36.9,-75.9,37.1')
```

**WKT** — Well-Known Text geometry:
```python
result = fetch_tiles('/path/to/project', geometry='POLYGON((-76.1 36.9, -75.9 36.9, -75.9 37.1, -76.1 37.1, -76.1 36.9))')
```

**GeoJSON** — geometry or Feature object:
```python
result = fetch_tiles('/path/to/project', geometry='{"type":"Polygon","coordinates":[[[-76.1,36.9],[-75.9,36.9],[-75.9,37.1],[-76.1,37.1],[-76.1,36.9]]]}')
```

### Other data sources

You can specify any S3-hosted source by name with the `data_source` parameter:

```python
result = fetch_tiles('/path/to/project', geometry='aoi.gpkg', data_source='bag')
vrt_result = build_vrt('/path/to/project', data_source='bag')
```

```
fetch_tiles -d /path/to/project -g aoi.gpkg -s modeling
build_vrt -d /path/to/project -s modeling
```

See [Data Sources](data-sources.md) for details on all available sources.
