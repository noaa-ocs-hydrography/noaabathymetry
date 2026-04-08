# User Guide

## Project directory structure

After running fetch and mosaic, your project directory will contain:

```
/path/to/project/
├── BlueTopo_Tessellation/            # Tile scheme geopackage
│   └── BlueTopo_Tile_Scheme_*.gpkg
├── BlueTopo_Data/                    # Downloaded tile files
│   ├── tile_name.tiff
│   └── tile_name.tiff.aux.xml
├── BlueTopo_Mosaic/                  # Built mosaic files
│   ├── BlueTopo_Fetched_UTM18.vrt
│   ├── BlueTopo_Fetched_UTM18.vrt.ovr
│   ├── BlueTopo_Fetched_UTM18_hillshade.tif   # Optional (--hillshade)
│   ├── BlueTopo_Fetched_UTM19.vrt
│   └── ...
├── BlueTopo_Mosaic_3857/             # Optional (--reproject)
│   ├── BlueTopo_Fetched_UTM18.tif
│   └── ...
└── bluetopo_registry.db              # SQLite tracking database
```

The folder and file names change based on the data source. For example, with `data_source='bag'`:

```
/path/to/project/
├── BAG_Tessellation/
│   └── Navigation_Tile_Scheme_*.gpkg
├── BAG_Data/
│   └── tile_name.bag
├── BAG_Mosaic/
│   └── BAG_Fetched_UTM18.vrt
└── bag_registry.db
```

S-102 sources also include a CATALOG.XML file alongside tiles and produce subdataset VRTs:

```
/path/to/project/
├── S102V22_Tessellation/
│   └── Navigation_Tile_Scheme_*.gpkg
├── S102V22_Data/
│   ├── CATALOG.XML
│   └── tile_name.h5
├── S102V22_Mosaic/
│   ├── S102V22_Fetched_UTM18_BathymetryCoverage.vrt
│   ├── S102V22_Fetched_UTM18_QualityOfSurvey.vrt
│   └── S102V22_Fetched_UTM18.vrt           # Combined VRT
└── s102v22_registry.db
```

## Fetch-then-build lifecycle

The package operates in two distinct steps.

### Step 1: fetch

1. **Resolves the data source** — looks up the configuration for the named source.
2. **Downloads the tile scheme** — fetches the latest geopackage from S3. This file defines all available tiles, their UTM zones, resolutions, and file locations.
3. **Discovers tiles** — if you provide a geometry, it intersects your area of interest with the tile scheme to find overlapping tiles.
4. **Synchronizes records** — compares the current tile scheme against the tracking database. If a tile has a newer delivery date, the old files are removed and the tile is queued for re-download.
5. **Downloads tiles** — fetches all pending tiles from S3 in parallel with checksum verification.

### Step 2: mosaic

1. **Checks prerequisites** — verifies the project directory, registry database, and tile folder all exist. Checks GDAL version and driver availability.
2. **Detects missing mosaics** — scans the tracking database for UTM zones that need building (newly downloaded tiles, or mosaic files deleted from disk).
3. **Builds per-UTM mosaics** — creates a GDAL Virtual Raster for each UTM zone, referencing the downloaded tile files. Adds overview files (`.ovr`) for efficient display at multiple zoom levels.
4. **Aggregates RATs** — for sources with Raster Attribute Tables (BlueTopo, Modeling, S102V22, S102V30), combines per-tile RAT data into the UTM mosaic.

### Understanding `FetchResult`

Fetch returns a [`FetchResult`](api-reference.md#fetchresult) with per-tile status lists and run metadata.

**Tile statuses:**

- **existing** — tiles already downloaded, with verified checksum at download time, and present on disk. These are skipped.
- **downloaded** — tiles successfully fetched in this run (new tiles or re-downloads after an update).
- **not_found** — tiles whose files couldn't be located on S3. This can happen temporarily when NBS is updating tiles.
- **failed** — tiles where download or checksum verification failed. Each entry is a dict with `tile` and `reason` keys.

**Run metadata:**

- **filtered_out** — tiles excluded by the resolution filter. Empty when no filter is active.
- **missing_reset** — tiles previously downloaded but missing from disk.
- **available_tiles_intersecting_aoi** — number of tiles with valid metadata intersecting the area of interest geometry (includes tiles already tracked).
- **new_tiles_tracked** — number of tiles actually newly added to tracking in this run (tiles already in the database are not counted).
- **tile_resolution_filter** — the resolution filter that was active, or `None` if unfiltered.

### Understanding `MosaicResult`

Mosaic returns a [`MosaicResult`](api-reference.md#mosaicresult) with per-zone status lists and run metadata.

**Zone statuses:**

- **built** — UTM zones that were built in this run. Each entry includes paths to the mosaic, overview, and optional hillshade files.
- **skipped** — UTM zones already up to date, or zones with no matching tiles after resolution filtering.
- **failed** — UTM zones that failed during the build. Each entry includes the zone identifier and failure reason.
- **hillshades** — UTM zones where a hillshade was generated. Each entry includes the zone identifier and hillshade file path.

**Run metadata:**

- **missing_reset** — UTM zones reset because their mosaic files were missing on disk.
- **tile_resolution_filter** — the resolution filter that was active, or `None` if unfiltered.
- **mosaic_resolution_target** — Output pixel size override that was active, or `None` for native resolution.

### Understanding `StatusResult`

Status returns a [`StatusResult`](api-reference.md#statusresult) with per-tile freshness information.

**Tile categories** (no overlap):

- **up_to_date** — tiles whose delivery datetime matches the remote and whose files exist on disk.
- **updates_available** — tiles with a newer delivery datetime on S3. Each entry includes local and remote datetimes.
- **missing_from_disk** — tiles whose delivery datetime matches the remote but whose files are missing from disk.
- **removed_from_nbs** — tiles tracked locally that no longer appear in the remote geopackage.

**Run metadata:**

- **total_tracked** — total number of tiles in the local database.

### How geometry works

The `geometry` parameter controls **tile discovery**, not downloading. When you pass a geometry, fetch intersects it with the tile scheme and adds any overlapping tiles to a persistent tracking list in the project database. **A geometry is required on the first fetch** to initialize a project.

This tracking is additive. Tiles are never removed from tracking. Passing a new geometry adds new tiles without affecting previously tracked ones. All tracked tiles are downloaded and kept up to date on every fetch run, regardless of whether a geometry is provided.

On subsequent runs, passing a geometry discovers and tracks any new tiles that intersect it. This is useful both for expanding your area of interest and for picking up entirely new tiles that NBS has published since your last discovery run. Omitting the geometry still updates all previously tracked tiles, but won't discover new ones. Re-running with your geometry periodically ensures newly published tiles in your area of interest are picked up.

## Re-fetch and update behavior

Running `nbs fetch` again on the same project directory is safe and efficient:

- Tiles that are already downloaded and verified are skipped.
- The tile scheme geopackage is always re-downloaded to pick up the latest delivery information.
- If a tile's delivery date is newer than what's in the database, the old files are removed and the tile is re-downloaded.
- New tiles discovered by a new or updated geometry are added to tracking.

Running `nbs mosaic` again:

- UTM zones whose mosaics are already built and up to date are skipped.
- If you delete a mosaic file, the next `nbs mosaic` run will detect it's missing and rebuild.
- If new tiles were downloaded since the last build, only the affected UTM zones are rebuilt.

## Checking for updates

Use `nbs status` (or `status_tiles()` in Python) to check if your local project has updates available on S3 without downloading anything:

```python
from nbs.noaabathymetry import status_tiles

result = status_tiles('/path/to/project')
print(f"Updates available: {len(result.updates_available)}")
```

```
nbs status -d /path/to/project
```

The status check reads the remote tile scheme and compares delivery datetimes against your local database. It reports tiles that have updates, tiles missing from disk, and tiles removed from NBS. Use `--verbosity verbose` for per-tile detail, or `--verbosity quiet` to suppress all log output.

## Resolution filtering

BlueTopo tiles are available at multiple resolutions (e.g. 4m, 8m, 16m). Two parameters let you control resolution.

### tile_resolution_filter

Restricts which tiles are fetched or built by their native resolution. Pass one or more integer values (meters).

```python
# Only fetch 4m and 8m tiles
result = fetch_tiles('/path/to/project', geometry='aoi.gpkg',
                     tile_resolution_filter=[4, 8])

# Only build mosaics from those tiles
mosaic_result = mosaic_tiles('/path/to/project', tile_resolution_filter=[4, 8])
```

```
nbs fetch -d /path/to/project -g aoi.gpkg --tile-resolution-filter 4 8
nbs mosaic -d /path/to/project --tile-resolution-filter 4 8
```

When a resolution filter is used with mosaic, the output goes to a **separate directory** to avoid overwriting the default mosaics:

```
BlueTopo_Mosaic_4m_8m/
├── BlueTopo_Fetched_UTM18_4m_8m.vrt
└── ...
```

### mosaic_resolution_target

Forces the output mosaic pixel size to a specific value in meters. This resamples all tiles to a uniform resolution. Only applies to mosaic.

```python
mosaic_result = mosaic_tiles('/path/to/project', mosaic_resolution_target=8)
```

```
nbs mosaic -d /path/to/project -t 8
```

Output directory:

```
BlueTopo_Mosaic_tr8m/
├── BlueTopo_Fetched_UTM18_tr8m.vrt
└── ...
```

### Combining both

You can combine the tile filter with a resolution target:

```python
mosaic_result = mosaic_tiles('/path/to/project',
                       tile_resolution_filter=[4, 8],
                       mosaic_resolution_target=8)
```

Output directory:

```
BlueTopo_Mosaic_4m_8m_tr8m/
├── BlueTopo_Fetched_UTM18_4m_8m_tr8m.vrt
└── ...
```

> **Note:** Parameterized builds never touch the default mosaic directory. If you have both `BlueTopo_Mosaic/` and `BlueTopo_Mosaic_4m_8m/`, mosaic will note the other directory's existence but won't modify it.

## Web Mercator reprojection

Pass `reproject=True` (or `--reproject` on the CLI) to produce EPSG:3857 (Web Mercator) GeoTIFFs . This is useful for serving tiles through GeoServer or other web mapping platforms that require a single CRS across UTM zone boundaries.

```python
mosaic_result = mosaic_tiles('/path/to/project', reproject=True)
```

```
nbs mosaic -d /path/to/project --reproject
```

The 3857 output is stored in a separate directory (e.g. `BlueTopo_Mosaic_3857/`) and tracked independently from the default mosaics. The output is a GeoTIFF (`.tif`) rather than a VRT, since the reprojection requires pixel computation. Only UTM zones with new or updated tiles are reprojected on subsequent runs.

> **Note:** Reprojection is currently only supported for the BlueTopo data source.

This can be combined with other parameters:

```python
# With resolution filtering
mosaic_result = mosaic_tiles('/path/to/project', tile_resolution_filter=[4, 8], reproject=True)

# With a target resolution, parallel workers, and hillshade
mosaic_result = mosaic_tiles('/path/to/project', reproject=True,
                       mosaic_resolution_target=16, workers=3, hillshade=True)
```

```
nbs mosaic -d /path/to/project --reproject -t 16 --workers 3 --hillshade
```

Output directories follow the same naming pattern: `BlueTopo_Mosaic_4m_8m_3857/`, `BlueTopo_Mosaic_tr16m_3857/`, etc.

## Parallel processing

By default, mosaic processes UTM zones sequentially. Pass `workers` to build multiple zones in parallel:

```python
result = mosaic_tiles('/path/to/project', workers=4)
```

```
nbs mosaic -d /path/to/project --workers 4
```

The maximum is `os.cpu_count()`. Each worker loads tile data into RAM independently, so memory usage scales with the number of workers. **Run with the default (1 worker) first to gauge memory usage before scaling up.** If a zone fails, other zones continue and the failure is reported in `MosaicResult.failed`.

## Hillshade generation

Pass `hillshade=True` (or `--hillshade` on the CLI) to generate a hillshade GeoTIFF alongside each mosaic:

```python
result = mosaic_tiles('/path/to/project', hillshade=True)
```

```
nbs mosaic -d /path/to/project --hillshade
```

This produces a `_hillshade.tif` Cloud Optimized GeoTIFF (COG) next to each mosaic, built from band 1 (Elevation) at 16m resolution with azimuth 315°, altitude 45°, and 4× vertical exaggeration. The COG format embeds BILINEAR overviews for efficient display.

## Custom output directory

By default, mosaic creates an auto-named directory based on the build parameters (e.g. `BlueTopo_Mosaic`, `BlueTopo_Mosaic_3857`). Pass `output_dir` to use a custom name:

```python
result = mosaic_tiles('/path/to/project', output_dir='my_mosaics')
```

```
nbs mosaic -d /path/to/project -o my_mosaics
```

Each output directory can only be used by one build configuration. If you try to use a directory that's already in use by a different configuration (e.g. different resolution parameters), mosaic will raise an error. To reassign a directory, delete it first. The system will detect it's gone and allow reassignment on the next run.

## Debug mode

Pass `debug=True` (or `--debug` on the CLI) to generate a diagnostic report:

```python
result = fetch_tiles('/path/to/project', geometry='aoi.gpkg', debug=True)
```

```
nbs fetch -d /path/to/project -g aoi.gpkg --debug
```

This writes a timestamped log file (e.g. `noaabathymetry_debug_20240315_143022.log`) to the project directory containing:

1. **Environment** — package version, Python version, GDAL version, platform
2. **Configuration** — active data source settings, file slots, gpkg field mappings
3. **Filesystem** — existence and size of registry DB, tile folder, mosaic folder
4. **Database schema** — column definitions for all tables
5. **Database summary** — tile counts (verified, unverified, pending), UTM zone build status
6. **Tile details** — per-tile anomalies (missing links, files missing on disk, unverified downloads)
7. **UTM zone details** — Mosaic/OVR paths and build status per zone

The report contains only technical information and does not include credentials, environment variables, or personal data beyond the project directory path.
