# User Guide

## Project directory structure

After running `fetch_tiles` and `build_vrt`, your project directory will contain:

```
/path/to/project/
├── BlueTopo/                      # Downloaded tile files
│   ├── Tessellation/              # Tile scheme geopackage
│   │   └── BlueTopo_Tile_Scheme_*.gpkg
│   ├── UTM18/                     # Tiles grouped by UTM zone
│   │   ├── tile_name.tiff
│   │   └── tile_name.tiff.aux.xml
│   ├── UTM19/
│   │   └── ...
│   └── ...
├── BlueTopo_VRT/                  # Built VRT files
│   ├── BlueTopo_Fetched_UTM18.vrt
│   ├── BlueTopo_Fetched_UTM18.vrt.ovr
│   ├── BlueTopo_Fetched_UTM18_hillshade.tif   # Optional (--hillshade)
│   ├── BlueTopo_Fetched_UTM19.vrt
│   └── ...
├── BlueTopo_VRT_3857/             # Optional (--reproject)
│   ├── BlueTopo_Fetched_UTM18.tif
│   └── ...
└── bluetopo_registry.db           # SQLite tracking database
```

The folder and file names change based on the data source. For example, with `data_source='bag'`:

```
/path/to/project/
├── BAG/
│   ├── Tessellation/
│   ├── UTM18/
│   │   └── tile_name.bag
│   └── ...
├── BAG_VRT/
│   └── BAG_Fetched_UTM18.vrt
└── bag_registry.db
```

S-102 sources also create a `Data/` subdirectory for the CATALOG.XML file and produce subdataset VRTs:

```
/path/to/project/
├── S102V22/
│   ├── Tessellation/
│   ├── Data/
│   │   └── CATALOG.XML
│   └── UTM18/
│       └── tile_name.h5
├── S102V22_VRT/
│   ├── S102V22_Fetched_UTM18_BathymetryCoverage.vrt
│   ├── S102V22_Fetched_UTM18_QualityOfSurvey.vrt
│   └── S102V22_Fetched_UTM18.vrt           # Combined VRT
└── s102v22_registry.db
```

## Fetch-then-build lifecycle

BlueTopo operates in two distinct steps.

### Step 1: fetch_tiles

1. **Resolves the data source** — looks up the configuration for the named source, or inspects a local directory for a tile scheme geopackage.
2. **Downloads the tile scheme** — fetches the latest geopackage from S3 (or copies from a local directory). This file defines all available tiles, their UTM zones, resolutions, and file locations.
3. **Discovers tiles** — if you provide a geometry, it intersects your area of interest with the tile scheme to find overlapping tiles.
4. **Synchronizes records** — compares the current tile scheme against the tracking database. If a tile has a newer delivery date, the old files are removed and the tile is queued for re-download.
5. **Downloads tiles** — fetches all pending tiles from S3 in parallel with checksum verification.

### Step 2: build_vrt

1. **Checks prerequisites** — verifies the project directory, registry database, and tile folder all exist. Checks GDAL version and driver availability.
2. **Detects missing VRTs** — scans the tracking database for UTM zones that need building (newly downloaded tiles, or VRT files deleted from disk).
3. **Builds per-UTM VRTs** — creates a GDAL Virtual Raster for each UTM zone, referencing the downloaded tile files. Adds overview files (`.ovr`) for efficient display at multiple zoom levels.
4. **Aggregates RATs** — for sources with Raster Attribute Tables (BlueTopo, Modeling, HSD, S102V22, S102V30), combines per-tile RAT data into the UTM VRT.

### Understanding `FetchResult`

`fetch_tiles` returns a `FetchResult` with per-tile status lists and run metadata.

**Tile statuses:**

- **existing** — tiles already downloaded, with verified checksum at download time, and present on disk. These are skipped.
- **downloaded** — tiles successfully fetched in this run (new tiles or re-downloads after an update).
- **not_found** — tiles whose files couldn't be located on S3. This can happen temporarily when NBS is updating tiles.
- **failed** — tiles where download or checksum verification failed. Each entry is a dict with `tile` and `reason` keys.

**Run metadata:**

- **new_tiles_tracked** — number of new tiles added to tracking via geometry intersection.
- **tile_resolution_filter** — the resolution filter that was active, or `None` if unfiltered.

### How geometry works

The `geometry` parameter controls **tile discovery**, not downloading. When you pass a geometry, `fetch_tiles` intersects it with the tile scheme and adds any overlapping tiles to a persistent tracking list in the project database.

This tracking is additive — tiles are never removed from tracking. Passing a new geometry adds new tiles without affecting previously tracked ones. All tracked tiles are downloaded and kept up to date on every `fetch_tiles` run, regardless of whether a geometry is provided.

You only need to pass a geometry when you want to expand your area of interest. Omitting it still updates all previously tracked tiles — if NBS updates a tracked tile (new delivery date, revised data), the next `fetch_tiles` run picks up the change automatically. However, if NBS publishes *entirely new* tiles in your area — tiles that didn't exist in the tile scheme when you first ran — they won't be discovered unless you run with the geometry again. Re-running with your geometry periodically ensures newly published tiles in your area of interest are picked up.

## Re-fetch and update behavior

Running `fetch_tiles` again on the same project directory is safe and efficient:

- Tiles that are already downloaded and verified are skipped.
- The tile scheme geopackage is always re-downloaded to pick up the latest delivery information.
- If a tile's delivery date is newer than what's in the database, the old files are removed and the tile is re-downloaded.
- New tiles discovered by a new or updated geometry are added to tracking.

Running `build_vrt` again:

- UTM zones whose VRTs are already built and up to date are skipped.
- If you delete a VRT file, the next `build_vrt` run will detect it's missing and rebuild.
- If new tiles were downloaded since the last build, only the affected UTM zones are rebuilt.

## Resolution filtering

BlueTopo tiles are available at multiple resolutions (e.g. 4m, 8m, 16m). Two parameters let you control resolution.

### tile_resolution_filter

Restricts which tiles are fetched or built by their native resolution. Pass one or more integer values (meters).

```python
# Only fetch 4m and 8m tiles
result = fetch_tiles('/path/to/project', geometry='aoi.gpkg',
                     tile_resolution_filter=[4, 8])

# Only build VRTs from those tiles
vrt_result = build_vrt('/path/to/project', tile_resolution_filter=[4, 8])
```

```
fetch_tiles -d /path/to/project -g aoi.gpkg --tile-resolution-filter 4 8
build_vrt -d /path/to/project --tile-resolution-filter 4 8
```

When a resolution filter is used with `build_vrt`, the output goes to a **separate directory** to avoid overwriting the default VRTs:

```
BlueTopo_VRT_4m_8m/
├── BlueTopo_Fetched_UTM18_4m_8m.vrt
└── ...
```

### vrt_resolution_target

Forces the output VRT pixel size to a specific value in meters. This resamples all tiles to a uniform resolution. Only applies to `build_vrt`.

```python
vrt_result = build_vrt('/path/to/project', vrt_resolution_target=8)
```

```
build_vrt -d /path/to/project -t 8
```

Output directory:

```
BlueTopo_VRT_tr8m/
├── BlueTopo_Fetched_UTM18_tr8m.vrt
└── ...
```

### Combining both

You can combine the tile filter with a resolution target:

```python
vrt_result = build_vrt('/path/to/project',
                       tile_resolution_filter=[4, 8],
                       vrt_resolution_target=8)
```

Output directory:

```
BlueTopo_VRT_4m_8m_tr8m/
├── BlueTopo_Fetched_UTM18_4m_8m_tr8m.vrt
└── ...
```

> **Note:** Parameterized builds never touch the default VRT directory. If you have both `BlueTopo_VRT/` and `BlueTopo_VRT_4m_8m/`, `build_vrt` will note the other directory's existence but won't modify it.

## Web Mercator reprojection

Pass `reproject=True` (or `--reproject` on the CLI) to produce EPSG:3857 (Web Mercator) GeoTIFFs instead of native UTM VRTs. This is useful for serving tiles through GeoServer or other web mapping platforms that require a single CRS across UTM zone boundaries.

```python
vrt_result = build_vrt('/path/to/project', reproject=True)
```

```
build_vrt -d /path/to/project --reproject
```

The 3857 output is stored in a separate directory (e.g. `BlueTopo_VRT_3857/`) and tracked independently from the default UTM VRTs. The output is a GeoTIFF (`.tif`) rather than a VRT, since the reprojection requires pixel computation. Only UTM zones with new or updated tiles are reprojected on subsequent runs.

This can be combined with other parameters:

```python
# With resolution filtering
vrt_result = build_vrt('/path/to/project', tile_resolution_filter=[4, 8], reproject=True)

# With a target resolution, parallel workers, and hillshade
vrt_result = build_vrt('/path/to/project', reproject=True,
                       vrt_resolution_target=16, workers=3, hillshade=True)
```

```
build_vrt -d /path/to/project --reproject -t 16 --workers 3 --hillshade
```

Output directories follow the same naming pattern: `BlueTopo_VRT_4m_8m_3857/`, `BlueTopo_VRT_tr16m_3857/`, etc.

## Parallel processing

By default, `build_vrt` processes UTM zones sequentially. Pass `workers` to build multiple zones in parallel:

```python
result = build_vrt('/path/to/project', workers=4)
```

```
build_vrt -d /path/to/project --workers 4
```

The maximum is `os.cpu_count()`. Each worker loads tile data into RAM independently, so memory usage scales with the number of workers. **Run with the default (1 worker) first to gauge memory usage before scaling up.** If a zone fails during parallel builds, other zones continue and the failure is reported in `BuildResult.failed`.

## Hillshade generation

Pass `hillshade=True` (or `--hillshade` on the CLI) to generate a hillshade GeoTIFF alongside each VRT:

```python
result = build_vrt('/path/to/project', hillshade=True)
```

```
build_vrt -d /path/to/project --hillshade
```

This produces a `_hillshade.tif` file next to each VRT, built from band 1 (Elevation) at 16m resolution with azimuth 315°, altitude 45°, and 4× vertical exaggeration. The hillshade includes BILINEAR overviews for efficient display.

## Debug mode

Pass `debug=True` (or `--debug` on the CLI) to generate a diagnostic report:

```python
result = fetch_tiles('/path/to/project', geometry='aoi.gpkg', debug=True)
```

```
fetch_tiles -d /path/to/project -g aoi.gpkg --debug
```

This writes a timestamped log file (e.g. `bluetopo_debug_20240315_143022.log`) to the project directory containing:

1. **Environment** — BlueTopo version, Python version, GDAL version, platform
2. **Configuration** — active data source settings, file slots, gpkg field mappings
3. **Filesystem** — existence and size of registry DB, tile folder, VRT folder
4. **Database schema** — column definitions for all tables
5. **Database summary** — tile counts (verified, unverified, pending), UTM zone build status
6. **Tile details** — per-tile anomalies (missing links, files missing on disk, unverified downloads)
7. **UTM zone details** — VRT/OVR paths and build status per zone

The report contains only technical information — no credentials, environment variables, or personal data beyond the project directory path.
