[![alt text](https://www.nauticalcharts.noaa.gov/data/images/bluetopo/logo.png)](https://www.nauticalcharts.noaa.gov/data/bluetopo.html)

---

<p align="center">
    <a href="https://noaa-ocs-hydrography.github.io/BlueTopo/">Documentation</a> •
    <a href="#background">Background</a> •
    <a href="#requirements">Requirements</a> •
    <a href="#installation">Installation</a> •
    <a href="#quickstart">Quickstart</a> •
    <a href="#cli">CLI</a> •
    <a href="#notes">Notes</a> •
    <a href="#authors">Contact</a>
</p>

## Overview

This package simplifies getting BlueTopo data in your area of interest.

## Background

[BlueTopo](https://www.nauticalcharts.noaa.gov/data/bluetopo.html) is a compilation of the best available public bathymetric data of U.S. waters.

Created by [NOAA Office of Coast Survey's](https://www.nauticalcharts.noaa.gov/) National Bathymetric Source project, [BlueTopo data](https://www.nauticalcharts.noaa.gov/data/bluetopo_specs.html) intends to provide depth information nationwide with the vertical uncertainty tied to that depth estimate as well as information on the survey source that it originated from.

This data is presented in a multiband high resolution GeoTIFF with an associated raster attribute table.

For answers to frequently asked questions, visit the [FAQ](https://www.nauticalcharts.noaa.gov/data/bluetopo_faq.html).

## Requirements

This codebase is written for Python 3 and relies on the following python
packages:

- gdal / ogr
- boto3
- tqdm

## Installation

Install conda (If you have not already): [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

In the command line, create an environment with the required packages:

```
conda create -n bluetopo_env -c conda-forge 'gdal>=3.9' libgdal-hdf5
```

```
conda activate bluetopo_env
```

```
pip install bluetopo
```

## Quickstart

After installation, you have access to a Python API and two matching CLI commands: `fetch_tiles` for downloading tiles and `build_vrt` for assembling them into VRTs.

```python
from nbs.bluetopo import fetch_tiles, build_vrt
```

Define your area of interest using any of the formats below, then fetch and build:

```python
result = fetch_tiles('/path/to/project', geometry='area_of_interest.gpkg')
result = build_vrt('/path/to/project')
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

## CLI

To fetch the latest BlueTopo data, pass a directory path and a geometry input:

```
fetch_tiles -d /path/to/project -g area_of_interest.gpkg
```

The `-g` flag accepts the same formats as the Python `geometry` parameter (file path, bounding box, WKT, or GeoJSON).

Pass the same directory path to `build_vrt` to create a VRT from the fetched data:

```
build_vrt -d /path/to/project
```

Use `-h` for help and to see additional arguments.

For most usecases, reusing the commands above to stay up to date in your area of interest is adequate.

## Notes

In addition to BlueTopo, several other data sources are available: **Modeling**, **BAG**, **S102 v2.1**, **S102 v2.2**, and **S102 v3.0**. Use the `source` argument in the CLI commands or the `data_source` argument in the Python API (e.g. `data_source='bag'`, `data_source='s102v30'`).

The primary difference between BlueTopo and Modeling data is the vertical datum — Modeling data is on a low water datum. BAG and S102 sources provide navigation-grade bathymetry. S102 sources require GDAL 3.9+ with HDF5 support (`libgdal-hdf5`). See the [data sources reference](https://noaa-ocs-hydrography.github.io/BlueTopo/data-sources.html) for details.

## Authors

- Glen Rice (NOAA), <ocs.nbs@noaa.gov>

- Tashi Geleg (Lynker / NOAA), <ocs.nbs@noaa.gov>

## License

This work, as a whole, falls under Creative Commons Zero (see
[LICENSE](LICENSE)).

## Disclaimer

This repository is a scientific product and is not official
communication of the National Oceanic and Atmospheric Administration, or
the United States Department of Commerce. All NOAA GitHub project code
is provided on an 'as is' basis and the user assumes responsibility for
its use. Any claims against the Department of Commerce or Department of
Commerce bureaus stemming from the use of this GitHub project will be
governed by all applicable Federal law. Any reference to specific
commercial products, processes, or services by service mark, trademark,
manufacturer, or otherwise, does not constitute or imply their
endorsement, recommendation or favoring by the Department of Commerce.
The Department of Commerce seal and logo, or the seal and logo of a DOC
bureau, shall not be used in any manner to imply endorsement of any
commercial product or activity by DOC or the United States Government.
