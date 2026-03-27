# API Reference

## Python API

Both public functions are importable from `nbs.bluetopo`:

```python
from nbs.bluetopo import fetch_tiles, build_vrt
```

---

### fetch_tiles

```python
fetch_tiles(
    project_dir: str,
    geometry: str = None,
    data_source: str = None,
    tile_resolution_filter: list = None,
    debug: bool = False,
) -> FetchResult
```

Discover, download, and update NBS tiles.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_dir` | `str` | *required* | Absolute path to the project directory. Created if it does not exist. |
| `geometry` | `str \| None` | `None` | Geometry input defining the area of interest. Accepts a file path, bounding box (`xmin,ymin,xmax,ymax`), WKT, or GeoJSON string. String inputs assume EPSG:4326. Pass `None` to skip tile discovery (useful for re-downloading existing tiles). |
| `data_source` | `str \| None` | `None` | An S3 source name (e.g. `"bluetopo"`, `"bag"`, `"s102v30"`), or `None` (defaults to `"bluetopo"`). |
| `tile_resolution_filter` | `list[int] \| None` | `None` | Only fetch tiles at these resolutions (meters). Example: `[4, 8]`. |
| `debug` | `bool` | `False` | If `True`, writes a diagnostic report to the project directory. |

**Returns:** [`FetchResult`](#fetchresult)

**Raises**

| Exception | Condition |
|---|---|
| `ValueError` | `project_dir` is not an absolute path. |
| `ValueError` | `geometry` path is not absolute. |
| `ValueError` | Unknown `data_source` name and path is not a directory. |
| `ValueError` | Local directory has no tile scheme geopackage. |
| `RuntimeError` | No tile scheme found on S3 after retry. |

**Example**

```python
from nbs.bluetopo import fetch_tiles

result = fetch_tiles(
    '/home/user/bathymetry',
    geometry='-76.1,36.9,-75.9,37.1',
    data_source='bluetopo',
)

print(f"Tiles in AOI: {result.available_tiles_intersecting_aoi}")
print(f"Newly tracked: {result.new_tiles_tracked}")
print(f"Downloaded: {len(result.downloaded)}")
print(f"Not found on S3: {len(result.not_found)}")
print(f"Already existing: {len(result.existing)}")
for failure in result.failed:
    print(f"  Failed: {failure['tile']} - {failure['reason']}")
```

---

### build_vrt

```python
build_vrt(
    project_dir: str,
    data_source: str = None,
    relative_to_vrt: bool = True,
    vrt_resolution_target: float = None,
    tile_resolution_filter: list = None,
    hillshade: bool = False,
    workers: int = None,
    reproject: bool = False,
    output_dir: str = None,
    debug: bool = False,
) -> BuildResult
```

Build a flat GDAL VRT per UTM zone from all source tiles.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_dir` | `str` | *required* | Absolute path to the project directory. |
| `data_source` | `str \| None` | `None` | A known source name, or `None` (defaults to `"bluetopo"`). |
| `relative_to_vrt` | `bool` | `True` | Store referenced file paths as relative to the VRT's directory. Set to `False` for absolute paths. |
| `vrt_resolution_target` | `float \| None` | `None` | Force output pixel size in meters. Must be a positive number. |
| `tile_resolution_filter` | `list[int] \| None` | `None` | Only include tiles at these resolutions (meters). Outputs to a separate VRT directory. |
| `hillshade` | `bool` | `False` | If `True`, generate a hillshade GeoTIFF from the elevation band. |
| `workers` | `int \| None` | `None` | Number of parallel worker processes for building UTM zones. `None` or `1` = sequential. Must not exceed `os.cpu_count()`. |
| `reproject` | `bool` | `False` | If `True`, reproject to EPSG:3857 (Web Mercator) GeoTIFFs instead of native UTM VRTs. Outputs to a separate directory (e.g. `BlueTopo_VRT_3857/`). |
| `output_dir` | `str \| None` | `None` | Custom output directory name within the project directory. Overrides the auto-generated name. Each directory can only be used by one build configuration. |
| `debug` | `bool` | `False` | If `True`, writes a diagnostic report to the project directory. |

**Returns:** [`BuildResult`](#buildresult)

**Raises**

| Exception | Condition |
|---|---|
| `ValueError` | `project_dir` is not an absolute path. |
| `ValueError` | Project directory does not exist. |
| `ValueError` | Registry database not found (`fetch_tiles` must run first). |
| `ValueError` | Tile downloads folder not found (`fetch_tiles` must run first). |
| `ValueError` | `vrt_resolution_target` is not positive. |
| `ValueError` | `workers` is not a positive integer or exceeds `os.cpu_count()`. |
| `ValueError` | `reproject` is used with a data source other than BlueTopo. |
| `ValueError` | `output_dir` contains a path separator (must be a single directory name). |
| `RuntimeError` | GDAL version is too old for the data source. |
| `RuntimeError` | GDAL is missing required drivers (e.g. S102, BAG). |
| `RuntimeError` | Project was created with an incompatible internal version. |
| `ValueError` | No parseable tile resolutions found for a UTM zone. |

**Example**

```python
from nbs.bluetopo import build_vrt

result = build_vrt(
    '/home/user/bathymetry',
    data_source='bluetopo',
    vrt_resolution_target=8,
    tile_resolution_filter=[4, 8],
)

for entry in result.built:
    print(f"Built UTM {entry['utm']}: {entry['vrt']}")
print(f"Skipped (up to date): {len(result.skipped)}")
print(f"Missing VRTs reset: {len(result.missing_reset)}")
```

---

### FetchResult

Dataclass returned by `fetch_tiles`.

| Attribute | Type | Description |
|---|---|---|
| `downloaded` | `list[str]` | Tile names successfully downloaded in this run. |
| `failed` | `list[dict]` | Tiles that failed download. Each dict has `tile` (str) and `reason` (str) keys. |
| `not_found` | `list[str]` | Tile names whose files could not be located on S3. |
| `existing` | `list[str]` | Tile names already downloaded, verified, and up to date. |
| `filtered_out` | `list[str]` | Tiles excluded by the resolution filter. Empty when no filter is active. |
| `missing_reset` | `list[str]` | Tiles previously downloaded but missing from disk. |
| `available_tiles_intersecting_aoi` | `int` | Number of tiles with valid metadata intersecting the area of interest geometry. Includes tiles already tracked. |
| `new_tiles_tracked` | `int` | Number of tiles actually newly added to tracking in this run. Tiles already in the database are not counted. |
| `tile_resolution_filter` | `list[int] \| None` | Resolution filter that was active, or `None` if unfiltered. |

**Example**

```python
result = fetch_tiles('/home/user/bathymetry', geometry='aoi.gpkg')
print(result)

# FetchResult(
#     downloaded=['BlueTopo_BC25L4NW_20240315', 'BlueTopo_BC25L4NE_20240315',
#                 'BlueTopo_BC25L6SW_20240315'],
#     failed=[{'tile': 'BlueTopo_BC25L6SE_20240315',
#              'reason': 'incorrect hash for geotiff (expected=a1b2c3d4e5f6... got=9f8e7d6c5b4a...)'}],
#     not_found=['BlueTopo_BC25L8NW_20240315'],
#     existing=['BlueTopo_BC25M4NW_20240301', 'BlueTopo_BC25M4NE_20240301'],
#     filtered_out=[],
#     missing_reset=[],
#     available_tiles_intersecting_aoi=8,
#     new_tiles_tracked=6,
#     tile_resolution_filter=None
# )
```

---

### BuildResult

Dataclass returned by `build_vrt`.

| Attribute | Type | Description |
|---|---|---|
| `built` | `list[dict]` | UTM zones that were built. Each dict has `utm` (str), `vrt` (str), `ovr` (str or None), and `hillshade` (str or None) keys. |
| `skipped` | `list[str]` | UTM zone identifiers that were already up to date, or had no matching tiles after resolution filtering. |
| `failed` | `list[dict]` | UTM zones that failed during the build. Each dict has `utm` (str) and `reason` (str) keys. |
| `missing_reset` | `list[str]` | UTM zones reset due to VRT files missing on disk. |
| `tile_resolution_filter` | `list[int] \| None` | Resolution filter that was active, or `None` if unfiltered. |
| `vrt_resolution_target` | `float \| None` | VRT pixel size override that was active, or `None` for native resolution. |

**Example**

```python
result = build_vrt('/home/user/bathymetry')
print(result)

# BuildResult(
#     built=[
#         {'utm': '18', 'vrt': '/home/user/bathymetry/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt',
#          'ovr': '/home/user/bathymetry/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt.ovr',
#          'hillshade': None},
#         {'utm': '19', 'vrt': '/home/user/bathymetry/BlueTopo_VRT/BlueTopo_Fetched_UTM19.vrt',
#          'ovr': '/home/user/bathymetry/BlueTopo_VRT/BlueTopo_Fetched_UTM19.vrt.ovr',
#          'hillshade': None}
#     ],
#     skipped=['17'],
#     failed=[],
#     missing_reset=[],
#     tile_resolution_filter=None,
#     vrt_resolution_target=None
# )
```

---

## CLI Reference

Two commands are installed when you `pip install bluetopo`.

### fetch_tiles command

```
fetch_tiles -d DIR [-g GEOMETRY] [-s SOURCE] [--tile-resolution-filter N [N ...]] [--debug]
```

| Short form | Long form | Description |
|---|---|---|
| `-d` | `--dir`, `--directory` | **Required.** Absolute path to the project directory. |
| `-g` | `--geom`, `--geometry` | Geometry input (file path, bounding box, WKT, or GeoJSON). String inputs assume EPSG:4326. |
| `-s` | `--source`, `--data-source` | Data source identifier. Default: `bluetopo`. |
| | `--tile-resolution-filter` | Only fetch tiles at these resolutions (meters). Multiple values allowed. |
| | `--debug` | Write a diagnostic report to the project directory. |
| `-v` | `--version` | Show version and exit. |

**Examples**

```bash
# Discover tiles within a bounding box and download tracked tiles
fetch_tiles -d /home/user/bathymetry -g "-76.1,36.9,-75.9,37.1"

# Fetch from a geopackage, only 4m and 8m tiles
fetch_tiles -d /home/user/bathymetry -g /path/to/aoi.gpkg --tile-resolution-filter 4 8

# Fetch BAG data
fetch_tiles -d /home/user/bathymetry -g aoi.gpkg -s bag

# Re-download/update without discovering new tiles (no geometry)
fetch_tiles -d /home/user/bathymetry
```

### build_vrt command

```
build_vrt -d DIR [-s SOURCE] [-r BOOL] [-t RESOLUTION] [--tile-resolution-filter N [N ...]] [--hillshade] [--workers N] [--reproject] [-o OUTPUT_DIR] [--debug]
```

| Short form | Long form | Description |
|---|---|---|
| `-d` | `--dir`, `--directory` | **Required.** Absolute path to the project directory. |
| `-s` | `--source`, `--data-source` | Data source identifier. Default: `bluetopo`. |
| `-r` | `--rel`, `--relative_to_vrt` | Store VRT file paths as relative. Default: `true`. |
| `-t` | `--vrt-resolution-target` | Force output pixel size in meters (any positive number). |
| | `--tile-resolution-filter` | Only include tiles at these resolutions (meters). Multiple values allowed. |
| | `--hillshade` | Generate a hillshade GeoTIFF from the elevation band. |
| | `--workers` | Number of parallel worker processes for building UTM zones. |
| | `--reproject` | Reproject to EPSG:3857 (Web Mercator) GeoTIFFs instead of native UTM VRTs. |
| `-o` | `--output-dir` | Custom output directory name within the project directory. |
| | `--debug` | Write a diagnostic report to the project directory. |
| `-v` | `--version` | Show version and exit. |

**Examples**

```bash
# Build VRTs from fetched BlueTopo tiles
build_vrt -d /home/user/bathymetry

# Build at 8m resolution target
build_vrt -d /home/user/bathymetry -t 8

# Build only from 4m tiles
build_vrt -d /home/user/bathymetry --tile-resolution-filter 4

# Build with 4 parallel workers
build_vrt -d /home/user/bathymetry --workers 4

# Build with hillshade generation
build_vrt -d /home/user/bathymetry --hillshade

# Build into a custom output directory
build_vrt -d /home/user/bathymetry -o my_custom_vrts

# Build Modeling VRTs
build_vrt -d /home/user/bathymetry -s modeling
```
