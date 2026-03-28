# Troubleshooting

## "Please use an absolute path"

**Full message:**

> Please use an absolute path for your project folder.
> Typically for non windows systems this means starting with '/'

**Cause:** Both `fetch_tiles` and `build_vrt` require the `project_dir` to be an absolute path. Relative paths like `./my_project` or `my_project` are rejected.

**Fix:** Use a full path:

```python
# Wrong
result = fetch_tiles('my_project', geometry='aoi.gpkg')

# Right
result = fetch_tiles('/home/user/my_project', geometry='aoi.gpkg')
```

On the CLI:
```bash
fetch_tiles -d /home/user/my_project -g aoi.gpkg
```

The same requirement applies to geometry file paths. If you pass a file path as the geometry and it contains a path separator or starts with `~`, it must be absolute.

---

## "fetch_tiles must be run at least once prior to build_vrt"

**Full message:**

> SQLite database not found. Confirm correct folder. Note: fetch_tiles must be run at least once prior to build_vrt

**Cause:** `build_vrt` could not find the registry database (`<source>_registry.db`) in the project directory. This happens when:

- `fetch_tiles` was never run on this directory.
- You're pointing `build_vrt` at the wrong directory.
- The `data_source` argument doesn't match what was used with `fetch_tiles` (each source has its own database).

**Fix:** Run `fetch_tiles` first, then `build_vrt` with the same directory and data source:

```python
fetch_tiles('/path/to/project', geometry='aoi.gpkg', data_source='bag')
build_vrt('/path/to/project', data_source='bag')
```

---

## GDAL version too old

**Full message:**

> Please update GDAL to >=3.9 to run build_vrt.
> Some users have encountered issues with conda's installation of GDAL 3.4. Try more recent versions of GDAL if you also encounter issues in your conda environment.

**Cause:** The installed GDAL version is older than what the data source requires. S-102 sources (s102v21, s102v22, s102v30) need GDAL 3.9+. BlueTopo, Modeling, BAG, and HSD need GDAL 3.4+.

**Fix:** Update GDAL in your conda environment:

```bash
conda install -c conda-forge 'gdal>=3.9'
```

> **Note:** Some conda environments have had issues with GDAL 3.4 specifically. If you encounter unexpected errors even with GDAL 3.4 installed, try upgrading to a newer version.

---

## Missing GDAL drivers (S102, BAG)

**Full message:**

> GDAL is missing required driver(s) for S102V22: S102. Reinstall GDAL with HDF5 support to use this data source.

**Cause:** The GDAL installation doesn't include the S102 or BAG driver. These drivers require HDF5 support, which isn't always included in default GDAL builds.

**Fix:** Install the HDF5-enabled GDAL package:

```bash
conda install -c conda-forge libgdal-hdf5
```

To verify drivers are available:

```python
from osgeo import gdal
print(gdal.GetDriverByName('BAG'))    # Should not be None
print(gdal.GetDriverByName('S102'))   # Should not be None
```

---

## S3 temporarily unavailable / tiles not found

**Console output:**

> No geometry found in BlueTopo/..., retrying in 5 seconds...

or

> \* Some tiles we wanted to fetch were not found in the S3 bucket.
> \* The NBS may be actively updating the tiles in question.
> \* You can rerun fetch_tiles at a later time to download these tiles.

**Cause:** The NBS S3 bucket is temporarily unavailable, or tiles are being updated. When fetching the tile scheme geopackage or XML catalog, the package retries once after 5 seconds. For individual tiles, links from the geopackage may temporarily point to files being replaced.

**Fix:** Wait and rerun `fetch_tiles`. If the problem persists across multiple runs over several hours, contact the NBS team at ocs.nbs@noaa.gov.

Tiles that couldn't be found are reported in `FetchResult.not_found`:

```python
result = fetch_tiles('/path/to/project', geometry='aoi.gpkg')
if result.not_found:
    print(f"{len(result.not_found)} tiles not found, try again later")
```

---

## Checksum verification failures

**Console output:**

> N tiles failed checksum verification: ['tile_A', 'tile_B']
> Please contact the NBS if this issue does not fix itself on subsequent runs.

**Cause:** The SHA-256 hash of a downloaded file doesn't match the expected checksum from the tile scheme. This can happen due to:

- Interrupted download (network issues)
- Tile being actively updated on S3
- Corrupted file

**Fix:** Rerun `fetch_tiles`. The failed tiles will be re-downloaded. If the issue persists, contact the NBS team at ocs.nbs@noaa.gov.

---

## "Please close all files and attempt again"

**Full message:**

> Failed to download tile scheme. Possibly due to conflict with an open existing file. Please close all files and attempt again

**Cause:** The tile scheme geopackage, XML catalog, or tile files cannot be written because another process has them locked. This is most common on Windows when:

- QGIS has the VRT or geopackage file open
- Another Python process is using the database
- A file explorer preview pane has the file locked

**Fix:** Close the file in the other application and retry. On Windows, check Task Manager for processes that might have file locks.

---

## Stale VRT directories

**Console output:**

> Note: 2 other VRT director(ies) exist that may contain stale data:
>   BlueTopo_VRT/
>   BlueTopo_VRT_4m_8m/

**Cause:** This is an informational warning, not an error. When running `build_vrt` with resolution parameters (`--tile-resolution-filter` or `--vrt-resolution-target`), the output goes to a parameterized directory (e.g. `BlueTopo_VRT_tr8m/`). If other VRT directories exist from previous builds with different parameters, the package notes them.

**What to do:** This warning is safe to ignore. The other directories are not modified. If you no longer need them, you can delete them manually. Deleting the default `BlueTopo_VRT/` directory will cause the next default `build_vrt` run to rebuild it from scratch.

---

## Parallel worker failures

**Console output:**

> 2 zone(s) failed: utm18, utm19

**Cause:** When using `--workers N`, individual UTM zones can fail while others succeed. Common causes include insufficient RAM (each worker loads tile data independently), disk space exhaustion, or GDAL errors on specific tile combinations.

**Fix:** Check your system's available memory. Each worker can consume significant RAM depending on the number and size of tiles in the UTM zone. Run with the default (no `--workers` flag) first to gauge memory usage for a single zone, then scale up. Failed zones can be retried by running `build_vrt` again. Only the failed zones will be rebuilt.

```python
result = build_vrt('/path/to/project', workers=4)
for failure in result.failed:
    print(f"UTM {failure['utm']} failed: {failure['reason']}")
```

---

## Overview failed to create

**Full message:**

> Overview failed to create for utmXX. Please try again. If error persists, please contact NBS.

**Cause:** GDAL failed to create the `.ovr` overview file for a UTM zone VRT. This can happen due to disk space issues, file permission problems, or GDAL bugs.

**Fix:** Check that you have sufficient disk space and write permissions. Retry the build. If the error persists, run with `--debug` and share the diagnostic report with the NBS team.
