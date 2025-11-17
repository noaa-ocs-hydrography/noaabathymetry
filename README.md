[![alt text](https://www.nauticalcharts.noaa.gov/data/images/bluetopo/logo.png)](https://www.nauticalcharts.noaa.gov/data/bluetopo.html)

---

<p align="center">
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
- numpy
- boto3
- tqdm

## Installation

Install conda (If you have not already): [conda installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/)

In the command line, create an environment with the required packages:

```
conda create -n bluetopo_env -c conda-forge 'gdal>=3.4,<3.11'
```

```
conda activate bluetopo_env
```

```
pip install bluetopo
```

## Quickstart

To download the desired files, first create a geometry file (such as a geopackage) with a polygon depicting the area of interest. Then run the following commands inside of a Python shell:

```python
from nbs.bluetopo import fetch_tiles
```

```python
fetch_tiles(r'C:\download_path', 'area_of_interest.gpkg')
```

To build a GDAL VRT of the downloaded tiles:

```python
from nbs.bluetopo import build_vrt
```

```python
build_vrt(r'C:\download_path')
```

## CLI

You can also use the command line. Confirm the environment we created during installation is activated.

To fetch the latest BlueTopo data, use `fetch_tiles` passing a directory path and a geometry file path with a polygon depicting your area of interest:

```
fetch_tiles -d [DIRECTORY PATH] -g [GEOMETRY FILE PATH]
```

Pass the same directory path to `build_vrt` to create a VRT from the fetched data:

```
build_vrt -d [DIRECTORY PATH]
```

Use `-h` for help and to see additional arguments.

For most usecases, reusing the commands above to stay up to date in your area of interest is adequate.

## Notes

In addition to BlueTopo, modeling data is available. You can work with modeling data using the `source` argument in the CLI commands or the `data_source` argument if you're calling the function directly.

The primary difference between BlueTopo and modeling data is the vertical datum. Modeling data is on a low water datum.

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
