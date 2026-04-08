# API Reference

## Python API

All public functions are importable from `nbs.noaabathymetry`:

```python
from nbs.noaabathymetry import fetch_tiles, mosaic_tiles, status_tiles
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
| `geometry` | `str \| None` | `None` | Geometry input defining the area of interest. Accepts a file path, GeoJSON string, bounding box (`xmin,ymin,xmax,ymax`), or WKT. String inputs assume EPSG:4326. **Required on the first fetch** to initialize a project. Pass `None` on subsequent runs to skip tile discovery (useful for re-downloading existing tiles). |
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
from nbs.noaabathymetry import fetch_tiles

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

### mosaic_tiles

```python
mosaic_tiles(
    project_dir: str,
    data_source: str = None,
    relative_to_vrt: bool = True,
    mosaic_resolution_target: float = None,
    tile_resolution_filter: list = None,
    hillshade: bool = False,
    workers: int = None,
    reproject: bool = False,
    output_dir: str = None,
    debug: bool = False,
) -> MosaicResult
```

Build a per-UTM-zone mosaic from all source tiles.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_dir` | `str` | *required* | Absolute path to the project directory. |
| `data_source` | `str \| None` | `None` | A known source name, or `None` (defaults to `"bluetopo"`). |
| `relative_to_vrt` | `bool` | `True` | Store referenced file paths as relative to the VRT's directory. Set to `False` for absolute paths. |
| `mosaic_resolution_target` | `float \| None` | `None` | Force output pixel size in meters. Must be a positive number. |
| `tile_resolution_filter` | `list[int] \| None` | `None` | Only include tiles at these resolutions (meters). Outputs to a separate mosaic directory. |
| `hillshade` | `bool` | `False` | If `True`, generate a hillshade GeoTIFF from the elevation band. |
| `workers` | `int \| None` | `None` | Number of parallel worker processes for building UTM zones. `None` or `1` = sequential. Must not exceed `os.cpu_count()`. |
| `reproject` | `bool` | `False` | If `True`, reproject to EPSG:3857 (Web Mercator) GeoTIFFs. Outputs to a separate directory. |
| `output_dir` | `str \| None` | `None` | Custom output directory name within the project directory. Overrides the auto-generated name. Each directory can only be used by one build configuration. |
| `debug` | `bool` | `False` | If `True`, writes a diagnostic report to the project directory. |

**Returns:** [`MosaicResult`](#mosaicresult)

**Raises**

| Exception | Condition |
|---|---|
| `ValueError` | `project_dir` is not an absolute path. |
| `ValueError` | Project directory does not exist. |
| `ValueError` | Registry database not found (`fetch_tiles` must run first). |
| `ValueError` | Tile downloads folder not found (`fetch_tiles` must run first). |
| `ValueError` | `mosaic_resolution_target` is not positive. |
| `ValueError` | `workers` is not a positive integer or exceeds `os.cpu_count()`. |
| `ValueError` | `reproject` is used with a data source other than BlueTopo. |
| `ValueError` | `output_dir` contains a path separator (must be a single directory name). |
| `RuntimeError` | GDAL version is too old for the data source. |
| `RuntimeError` | GDAL is missing required drivers (e.g. S102, BAG). |
| `RuntimeError` | Project was created with an incompatible internal version. |
| `ValueError` | No parseable tile resolutions found for a UTM zone. |

**Example**

```python
from nbs.noaabathymetry import mosaic_tiles

result = mosaic_tiles(
    '/home/user/bathymetry',
    data_source='bluetopo',
    mosaic_resolution_target=8,
    tile_resolution_filter=[4, 8],
)

for entry in result.built:
    print(f"Built UTM {entry['utm']}: {entry['mosaic']}")
print(f"Skipped (up to date): {len(result.skipped)}")
print(f"Missing mosaics reset: {len(result.missing_reset)}")
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

### MosaicResult

Dataclass returned by `mosaic_tiles`.

| Attribute | Type | Description |
|---|---|---|
| `built` | `list[dict]` | UTM zones that were built. Each dict has `utm` (str), `mosaic` (str), `ovr` (str or None), and `hillshade` (str or None) keys. |
| `skipped` | `list[str]` | UTM zone identifiers that were already up to date, or had no matching tiles after resolution filtering. |
| `failed` | `list[dict]` | UTM zones that failed during the build. Each dict has `utm` (str) and `reason` (str) keys. |
| `missing_reset` | `list[str]` | UTM zones reset due to mosaic files missing on disk. |
| `hillshades` | `list[dict]` | UTM zones where a hillshade was generated. Each dict has `utm` (str) and `hillshade` (str, absolute path) keys. |
| `tile_resolution_filter` | `list[int] \| None` | Resolution filter that was active, or `None` if unfiltered. |
| `mosaic_resolution_target` | `float \| None` | Output pixel size override that was active, or `None` for native resolution. |

**Example**

```python
result = mosaic_tiles('/home/user/bathymetry')
print(result)

# MosaicResult(
#     built=[
#         {'utm': '18', 'mosaic': '/home/user/bathymetry/BlueTopo_Mosaic/BlueTopo_Fetched_UTM18.vrt',
#          'ovr': '/home/user/bathymetry/BlueTopo_Mosaic/BlueTopo_Fetched_UTM18.vrt.ovr',
#          'hillshade': None},
#         {'utm': '19', 'mosaic': '/home/user/bathymetry/BlueTopo_Mosaic/BlueTopo_Fetched_UTM19.vrt',
#          'ovr': '/home/user/bathymetry/BlueTopo_Mosaic/BlueTopo_Fetched_UTM19.vrt.ovr',
#          'hillshade': None}
#     ],
#     skipped=['17'],
#     failed=[],
#     missing_reset=[],
#     hillshades=[],
#     tile_resolution_filter=None,
#     mosaic_resolution_target=None
# )
```

---

### status_tiles

```python
status_tiles(
    project_dir: str,
    data_source: str = None,
    verbosity: str = "normal",
) -> StatusResult
```

Check local project freshness against the remote tile scheme.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_dir` | `str` | *required* | Absolute path to the project directory. |
| `data_source` | `str \| None` | `None` | A known source name, or `None` (defaults to `"bluetopo"`). |
| `verbosity` | `str` | `"normal"` | Logging verbosity: `"quiet"` suppresses all log output, `"normal"` shows UTM/resolution counts, `"verbose"` shows individual tiles. |

**Returns:** [`StatusResult`](#statusresult)

**Raises**

| Exception | Condition |
|---|---|
| `ValueError` | `project_dir` is not an absolute path. |
| `ValueError` | Project directory does not exist. |
| `ValueError` | Registry database not found (`fetch_tiles` must run first). |
| `ValueError` | Rate limit exceeded. |
| `RuntimeError` | Remote tile scheme cannot be read from S3. |

**Example**

```python
from nbs.noaabathymetry import status_tiles

result = status_tiles('/home/user/bathymetry')

print(f"Up to date: {len(result.up_to_date)}")
print(f"Updates available: {len(result.updates_available)}")
for entry in result.updates_available:
    print(f"  {entry['tile']}: {entry['local_datetime']} -> {entry['remote_datetime']}")
```

---

### StatusResult

Dataclass returned by `status_tiles`.

| Attribute | Type | Description |
|---|---|---|
| `up_to_date` | `list[dict]` | Tiles whose delivery datetime matches the remote and files exist on disk. Each dict has `tile`, `utm`, `resolution`, `local_datetime`, and `geometry` keys. |
| `updates_available` | `list[dict]` | Tiles with a newer delivery datetime on S3. Each dict has `tile`, `utm`, `resolution`, `local_datetime`, `remote_datetime`, and `geometry` keys. |
| `missing_from_disk` | `list[dict]` | Tiles whose delivery datetime matches the remote but files are missing from disk. Each dict has `tile`, `utm`, `resolution`, `local_datetime`, and `geometry` keys. |
| `removed_from_nbs` | `list[dict]` | Tiles tracked locally that no longer appear in the remote geopackage. Each dict has `tile`, `utm`, `resolution`, `local_datetime`, and `geometry` keys. |
| `total_tracked` | `int` | Total number of tiles in the local database. |

---

## CLI Reference

The `nbs` command is installed when you `pip install noaabathymetry`.

### nbs fetch

```
nbs fetch -d DIR [-g GEOMETRY] [-s SOURCE] [--tile-resolution-filter N [N ...]] [--debug] [--json]
```

| Short form | Long form | Description |
|---|---|---|
| `-d` | `--dir`, `--directory` | **Required.** Absolute path to the project directory. |
| `-g` | `--geom`, `--geometry` | Geometry input (file path, bounding box, WKT, or GeoJSON). String inputs assume EPSG:4326. |
| `-s` | `--source`, `--data-source` | Data source identifier. Default: `bluetopo`. |
| | `--tile-resolution-filter` | Only fetch tiles at these resolutions (meters). Multiple values allowed. |
| | `--debug` | Write a diagnostic report to the project directory. |
| | `--json` | Print result as JSON to stdout. |
| `-v` | `--version` | Show version and exit. |

**Examples**

```bash
# Discover tiles within a bounding box and download tracked tiles
nbs fetch -d /home/user/bathymetry -g "-76.1,36.9,-75.9,37.1"

# Fetch from a geopackage, only 4m and 8m tiles
nbs fetch -d /home/user/bathymetry -g /path/to/aoi.gpkg --tile-resolution-filter 4 8

# Fetch BAG data
nbs fetch -d /home/user/bathymetry -g aoi.gpkg -s bag

# Re-download/update without discovering new tiles (existing project, no geometry)
nbs fetch -d /home/user/bathymetry
```

### nbs mosaic

```
nbs mosaic -d DIR [-s SOURCE] [-r BOOL] [-t RESOLUTION] [--tile-resolution-filter N [N ...]] [--hillshade] [--workers N] [--reproject] [-o OUTPUT_DIR] [--debug] [--json]
```

| Short form | Long form | Description |
|---|---|---|
| `-d` | `--dir`, `--directory` | **Required.** Absolute path to the project directory. |
| `-s` | `--source`, `--data-source` | Data source identifier. Default: `bluetopo`. |
| `-r` | `--relative-to-vrt` | Store VRT file paths as relative. Default: `true`. |
| `-t` | `--mosaic-resolution-target` | Force output pixel size in meters (any positive number). |
| | `--tile-resolution-filter` | Only include tiles at these resolutions (meters). Multiple values allowed. |
| | `--hillshade` | Generate a hillshade GeoTIFF from the elevation band. |
| | `--workers` | Number of parallel worker processes for building UTM zones. |
| | `--reproject` | Reproject to EPSG:3857 (Web Mercator) GeoTIFFs. |
| `-o` | `--output-dir` | Custom output directory name within the project directory. |
| | `--debug` | Write a diagnostic report to the project directory. |
| | `--json` | Print result as JSON to stdout. |
| `-v` | `--version` | Show version and exit. |

**Examples**

```bash
# Build mosaics from fetched BlueTopo tiles
nbs mosaic -d /home/user/bathymetry

# Build at 8m resolution target
nbs mosaic -d /home/user/bathymetry -t 8

# Build only from 4m tiles
nbs mosaic -d /home/user/bathymetry --tile-resolution-filter 4

# Build with 4 parallel workers
nbs mosaic -d /home/user/bathymetry --workers 4

# Build with hillshade generation
nbs mosaic -d /home/user/bathymetry --hillshade

# Build into a custom output directory
nbs mosaic -d /home/user/bathymetry -o my_custom_mosaics

# Build Modeling mosaics
nbs mosaic -d /home/user/bathymetry -s modeling
```

### nbs status

```
nbs status -d DIR [-s SOURCE] [--verbosity quiet|normal|verbose] [--json]
```

| Short form | Long form | Description |
|---|---|---|
| `-d` | `--dir`, `--directory` | **Required.** Absolute path to the project directory. |
| `-s` | `--source`, `--data-source` | Data source identifier. Default: `bluetopo`. |
| | `--verbosity` | Logging verbosity: `quiet`, `normal` (default), or `verbose`. |
| | `--json` | Print result as JSON to stdout. |

**Examples**

```bash
# Check for updates
nbs status -d /home/user/bathymetry

# Check with verbose tile listing
nbs status -d /home/user/bathymetry --verbosity verbose

# Quiet mode (return object only, no log output)
nbs status -d /home/user/bathymetry --verbosity quiet

# Check a different data source
nbs status -d /home/user/bathymetry -s bag
```
