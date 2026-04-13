"""
mosaic.py - GDAL Virtual Raster creation, overviews, and RAT aggregation.

Builds flat mosaics per UTM zone from source tiles, with adaptive overviews
targeting config-driven output resolutions, optionally filtered to above
the coarsest source.  For multi-subdataset
sources (S102V22, S102V30), one mosaic is built per subdataset and then combined.
"""

import copy
import logging
import os
import uuid

from osgeo import gdal

from nbs.noaabathymetry._internal.config import (
    parse_resolution,
    validate_mosaic_resolution_target,
    get_mosaic_built_flags,
    get_all_reset_flags,
    get_disk_field,
    get_disk_fields,
    get_utm_file_columns,
    get_mosaic_fields,
)

logger = logging.getLogger("noaabathymetry")

# Process-global GDAL settings applied at import time.
# These affect all GDAL usage in the process. If noaabathymetry is used
# as a library alongside other GDAL code, these settings will apply
# to that code as well (especially UseExceptions). Revisit this later.
gdal.UseExceptions()
gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")
gdal.SetConfigOption("GDAL_TIFF_OVR_BLOCKSIZE", "512")
gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")

# Bump GDAL block cache to 15% of physical memory (if higher than default).
# Reduces tile re-reads during large warp and overview operations.
try:
    _phys_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    _target_cache = int(_phys_mem * 0.15)
    if _target_cache > gdal.GetCacheMax():
        gdal.SetCacheMax(_target_cache)
        logger.debug("GDAL cache max: %.1f GB", gdal.GetCacheMax() / 1024**3)
except (AttributeError, ValueError, OSError):
    pass  # Windows or unsupported platform — keep GDAL's default


def configure_gdal_for_worker(total_workers):
    """Scale GDAL thread count and cache size for a multiprocessing worker.

    When multiple worker processes each run GDAL operations, the default
    ``ALL_CPUS`` thread setting causes oversubscription (N workers ×
    cpu_count threads).  This function scales both thread count and
    cache size proportionally.
    """
    cpus = os.cpu_count() or 1
    threads = max(1, cpus // total_workers)
    gdal.SetConfigOption("GDAL_NUM_THREADS", str(threads))
    try:
        phys_mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        target = int(phys_mem * 0.15 / total_workers)
        if target > 0:
            gdal.SetCacheMax(target)
    except (AttributeError, ValueError, OSError):
        pass


# ---------------------------------------------------------------------------
# VRT creation
# ---------------------------------------------------------------------------

def create_vrt(files, vrt_path, levels, relative_to_vrt,
               band_descriptions=None, separate=False,
               mosaic_resolution_target=None, resolution="highest"):
    """Build a single GDAL VRT file with optional overviews.

    Any existing VRT and ``.ovr`` at *vrt_path* are removed first.
    When *relative_to_vrt* is True, source file references are stored
    as paths relative to the VRT's directory.

    Parameters
    ----------
    files : list[str]
        Absolute paths to source raster files (or S102 protocol URIs).
    vrt_path : str
        Absolute output path for the VRT file.
    levels : list[int] | None
        Overview factors (e.g. ``[8, 16, 32]``).  None skips overviews.
    relative_to_vrt : bool
        Store source paths relative to the VRT's directory.
    band_descriptions : list[str] | None
        Labels to assign to each band (e.g. ``["Elevation", "Uncertainty"]``).
    separate : bool
        If True, stack inputs as separate bands (used for combined VRTs).
    mosaic_resolution_target : float | None
        Force output pixel size in meters.  Overrides *resolution* when set.
    resolution : str
        GDAL resolution strategy (``"highest"``, ``"lowest"``, ``"average"``).
        Used when *mosaic_resolution_target* is None.
    """
    files = copy.deepcopy(files)
    try:
        if os.path.isfile(vrt_path):
            os.remove(vrt_path)
        if os.path.isfile(vrt_path + ".ovr"):
            os.remove(vrt_path + ".ovr")
    except (OSError, PermissionError) as e:
        raise OSError(f"Failed to remove older vrt files for {vrt_path}\n"
                      "Please close all files and attempt again") from e
    opts_str = '-separate -allow_projection_difference' if separate else '-allow_projection_difference'
    if mosaic_resolution_target is not None:
        validate_mosaic_resolution_target(mosaic_resolution_target)
        opts_str += f' -resolution user -tr {mosaic_resolution_target} {mosaic_resolution_target}'
    else:
        opts_str += f' -resolution {resolution}'
    opts_str += ' -r near'
    vrt_options = gdal.BuildVRTOptions(options=opts_str)
    # chdir so GDAL resolves relative source paths correctly.
    # basename avoids case-insensitive filesystem issues where
    # os.getcwd() returns different case than the input path.
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                if 'S102:' in files[idx]:
                    continue  # S102 URIs have their own path format
                files[idx] = os.path.relpath(files[idx], os.path.dirname(vrt_path))
        vrt = gdal.BuildVRT(os.path.basename(vrt_path), files, options=vrt_options)
        if band_descriptions:
            for i, desc in enumerate(band_descriptions):
                band = vrt.GetRasterBand(i + 1)
                band.SetDescription(desc)
        vrt = None
    except Exception as e:
        raise RuntimeError(f"VRT failed to build for {vrt_path}") from e
    finally:
        os.chdir(cwd)
    if levels:
        vrt = gdal.Open(vrt_path, 0)
        vrt.BuildOverviews("NEAREST", levels)
        vrt = None
        _compute_approximate_stats(vrt_path)


def _compute_approximate_stats(path):
    """Compute approximate statistics for all bands from overviews.

    Stores min/max/mean/stddev in the dataset metadata so GIS tools
    can render the data without scanning the full resolution raster.
    """
    ds = gdal.Open(path, 1)
    if ds is None:
        return
    for i in range(1, ds.RasterCount + 1):
        ds.GetRasterBand(i).ComputeStatistics(True)
    ds = None


def generate_hillshade(mosaic_path, hillshade_path, hillshade_resolution=None):
    """Generate a hillshade COG from band 1 (Elevation) of a source raster.

    Uses an in-memory VRT with resolution override so GDAL reads from
    the source's existing overviews when the target is coarser than
    full resolution.

    Uses azimuth 315, altitude 45, vertical exaggeration 4x.
    Output is a Cloud Optimized GeoTIFF (COG) with embedded overviews.

    Parameters
    ----------
    mosaic_path : str
        Path to the source mosaic (VRT or GeoTIFF).
    hillshade_path : str
        Output path for the hillshade COG.
    hillshade_resolution : int | float | None
        Pixel size in meters for the hillshade.  None (default) uses
        the source raster's native resolution.
    """
    if hillshade_resolution is None:
        ds = gdal.Open(mosaic_path)
        gt = ds.GetGeoTransform()
        hillshade_resolution = abs(gt[1])
        ds = None
    if os.path.isfile(hillshade_path):
        os.remove(hillshade_path)
    # Create an in-memory VRT at the target resolution. GDAL reads from
    # the source's overview levels instead of full resolution.
    uid = uuid.uuid4().hex[:8]
    mem_vrt = f"/vsimem/_hillshade_input_{uid}.vrt"
    gdal.Translate(mem_vrt, mosaic_path, format="VRT",
                   xRes=hillshade_resolution, yRes=hillshade_resolution)
    # DEMProcessing to a temp in-memory GeoTIFF, then convert to COG
    mem_tmp = f"/vsimem/_hillshade_tmp_{uid}.tif"
    opts = gdal.DEMProcessingOptions(
        options="-az 315 -alt 45 -z 4 -compute_edges "
                "-of GTiff -co COMPRESS=DEFLATE -co TILED=YES -co BIGTIFF=YES"
    )
    gdal.DEMProcessing(mem_tmp, mem_vrt, "hillshade", options=opts)
    gdal.Unlink(mem_vrt)
    # Convert to COG — embeds overviews, tiling, and compression in one file
    gdal.Translate(
        hillshade_path, mem_tmp, format="COG",
        creationOptions=[
            "COMPRESS=DEFLATE",
            "BIGTIFF=YES",
            "OVERVIEW_RESAMPLING=BILINEAR",
        ],
    )
    gdal.Unlink(mem_tmp)
    return hillshade_path


def reproject_to_web_mercator(sources, output_path, overview_factors=None,
                              target_resolution=None):
    """Reproject source raster(s) to an EPSG:3857 GeoTIFF using gdal.Warp.

    Produces a GeoTIFF with DEFLATE compression and 512x512 tiling.
    Uses nearest neighbor resampling to preserve categorical Contributor
    band values for RAT compatibility.

    When multiple sources are provided, they are mosaicked in order
    (later sources overlay earlier ones).

    The RAT is not preserved through Warp. Call ``add_mosaic_rat()`` on the
    output file after this function to attach the aggregated RAT.

    Parameters
    ----------
    sources : str | list[str]
        Path(s) to source raster(s) (VRTs or GeoTIFFs) in UTM projection.
    output_path : str
        Path for the output GeoTIFF (will be in EPSG:3857).
    overview_factors : list[int] | None
        Overview factors (e.g. ``[2, 4, 8, 16]``).  If None
        or empty, no overviews are built.
    target_resolution : float | None
        Output pixel size in EPSG:3857 meters.  When None, GDAL
        auto-determines from the source(s).

    Returns
    -------
    str
        The *output_path*.
    """
    if isinstance(sources, str):
        sources = [sources]
    if os.path.isfile(output_path):
        os.remove(output_path)
    thread_str = gdal.GetConfigOption("GDAL_NUM_THREADS") or "ALL_CPUS"
    warp_kwargs = dict(
        dstSRS="EPSG:3857",
        format="GTiff",
        resampleAlg="near",
        multithread=True,
        warpOptions=[f"NUM_THREADS={thread_str}", "SOURCE_EXTRA=5"],
        creationOptions=[
            "COMPRESS=DEFLATE",
            "TILED=YES",
            "BLOCKXSIZE=512",
            "BLOCKYSIZE=512",
            "BIGTIFF=YES",
            f"NUM_THREADS={thread_str}",
        ],
    )
    if target_resolution is not None:
        warp_kwargs["xRes"] = target_resolution
        warp_kwargs["yRes"] = target_resolution
        warp_kwargs["targetAlignedPixels"] = True
    opts = gdal.WarpOptions(**warp_kwargs)
    gdal.Warp(output_path, sources, options=opts)
    if overview_factors:
        ds = gdal.Open(output_path, 0)
        ds.BuildOverviews("NEAREST", overview_factors)
        ds = None
        _compute_approximate_stats(output_path)
    return output_path


def compute_overview_factors(resolutions, mosaic_resolution_target=None,
                             overview_levels=None, filter_coarsest=True):
    """Compute overview factors from source tile resolutions.

    Parameters
    ----------
    resolutions : set[int]
        Tile resolutions in meters (from DB ``resolution`` column via
        ``parse_resolution``).
    mosaic_resolution_target : float | None
        Override native resolution for factor calculation.
    overview_levels : list[int]
        Candidate overview resolutions.  Must be provided.
    filter_coarsest : bool
        When True, only resolutions above the coarsest source
        are kept.  When False, all listed levels are candidates.
    """
    if overview_levels is None:
        raise ValueError("overview_levels must be provided")

    if not resolutions:
        return []

    native_res = mosaic_resolution_target if mosaic_resolution_target else min(resolutions)
    coarsest_res = max(resolutions)

    if filter_coarsest:
        targets = [r for r in overview_levels if r > coarsest_res]
    else:
        targets = list(overview_levels)

    factors = [round(t / native_res) for t in targets if native_res > 0]
    factors = [f for f in factors if f >= 2]
    return sorted(set(factors))


# ---------------------------------------------------------------------------
# Tile selection and path building
# ---------------------------------------------------------------------------

def select_tiles_by_utm(project_dir, conn, utm, cfg, tile_resolution_filter=None):
    """Return tiles in a UTM zone whose files exist on disk, sorted coarse-to-fine.

    Tiles missing from disk are counted and a warning is printed.
    When *tile_resolution_filter* is set, only tiles at those resolutions
    (in meters) are included.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    conn : sqlite3.Connection
        Database connection.
    utm : str
        UTM zone identifier (e.g. ``"18"``).
    cfg : dict
        Data source configuration.
    tile_resolution_filter : list[int] | None
        Only include tiles at these resolutions.

    Returns
    -------
    list[dict]
        Tile rows sorted by resolution descending (coarsest first), so
        finer tiles overlay coarser ones in the VRT.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    tiles = [dict(row) for row in cursor.fetchall()]
    disk_fields = get_disk_fields(cfg)

    def tile_exists(tile):
        return all(
            tile.get(df) and os.path.isfile(os.path.join(project_dir, tile[df]))
            for df in disk_fields
        )

    existing_tiles = [tile for tile in tiles if tile_exists(tile)]
    missing_count = len(tiles) - len(existing_tiles)
    if missing_count:
        logger.warning("[UTM%s] Did not find files for %d registered tile(s). "
                       "Run fetch to retrieve files "
                       "or correct the directory path if incorrect.",
                       utm, missing_count)

    if tile_resolution_filter:
        res_set = set(tile_resolution_filter)
        null_res = [t for t in existing_tiles if parse_resolution(t.get("resolution")) is None]
        if null_res:
            logger.warning("[UTM%s] %d tile(s) have no parseable resolution and were "
                           "excluded by the resolution filter.", utm, len(null_res))
        existing_tiles = [
            t for t in existing_tiles
            if parse_resolution(t.get("resolution")) in res_set
        ]

    def _res_sort_key(tile):
        val = parse_resolution(tile.get("resolution"))
        if val is None:
            raise ValueError(
                f"Tile '{tile.get('tilename', '?')}' has non-numeric or empty "
                f"resolution '{tile.get('resolution', '')}'.")
        return val

    existing_tiles.sort(key=_res_sort_key, reverse=True)
    return existing_tiles


def build_tile_paths(tiles, project_dir, cfg, subdataset=None):
    """Build absolute file paths for source tiles.

    For S102 subdatasets with ``s102_protocol=True``, paths are wrapped in
    the ``S102:"path":SubdatasetName`` URI format that GDAL's S102 driver
    requires.

    Parameters
    ----------
    tiles : list[dict]
        Tile rows from the database.
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration.
    subdataset : dict | None
        Subdataset definition from ``cfg["subdatasets"]``, or None for
        single-dataset sources.

    Returns
    -------
    list[str]
        Paths (or S102 URIs) suitable for passing to :func:`create_vrt`.
    """
    disk_field = get_disk_field(cfg)
    paths = []
    for tile in tiles:
        fpath = os.path.join(project_dir, tile[disk_field])
        if subdataset and subdataset.get("s102_protocol"):
            fpath = fpath.replace("\\", "/")
            # On Unix, absolute paths get an extra "/" producing S102:"//path".
            # POSIX normalizes "//" to "/" so this works fine. Note that
            # _read_rat_data (line ~232) constructs the URI without the extra
            # "/" — both forms are equivalent on POSIX, but the extra "/" here
            # ensures Windows paths (no leading /) remain correct too.
            if fpath.startswith('/') and not fpath.startswith('//'):
                paths.append(f'S102:"/{fpath}":{subdataset["name"]}')
            else:
                paths.append(f'S102:"{fpath}":{subdataset["name"]}')
        else:
            paths.append(fpath)
    return paths


# ---------------------------------------------------------------------------
# RAT aggregation
# ---------------------------------------------------------------------------

def _discover_and_read_rat_data_direct(tiles, project_dir, cfg, expected_fields):
    """Discover common RAT fields and read data in a single pass (direct method).

    Opens each tile exactly once: caches column names and row data,
    progressively intersects ``expected_fields`` with actual columns,
    then reads cached data using the finalized field mapping.

    Returns
    -------
    tuple[dict, set[str], list[list]]
        ``(filtered_expected_fields, dropped_field_names, survey_rows)``
    """
    rat_band = cfg.get("rat_band", 3)
    disk_fields = get_disk_fields(cfg)
    dropped = set()

    # Phase 1: open each tile once, cache RAT data, intersect fields
    tile_cache = []  # list of (actual_names, rows)
    for tile in tiles:
        if any(tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df]))
               for df in disk_fields):
            missing = [df for df in disk_fields
                       if tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df]))]
            raise FileNotFoundError(
                f"Tile '{tile.get('tilename', '?')}' is missing file(s) for "
                f"field(s) {missing}. Tiles must be filtered for disk existence "
                f"before RAT aggregation.")
        gtiff = os.path.join(project_dir, tile[disk_fields[0]])
        ds = gdal.Open(gtiff)
        contrib = ds.GetRasterBand(rat_band)
        rat_n = contrib.GetDefaultRAT()
        if rat_n is None:
            ds = None
            continue
        actual_names = [
            rat_n.GetNameOfCol(i).lower()
            for i in range(rat_n.GetColumnCount())
        ]
        rows = [
            [rat_n.GetValueAsString(r, c) for c in range(len(actual_names))]
            for r in range(rat_n.GetRowCount())
        ]
        ds = None

        actual_set = set(actual_names)
        before = set(expected_fields.keys())
        expected_fields = {
            k: v for k, v in expected_fields.items() if k in actual_set
        }
        dropped |= before - set(expected_fields.keys())

        tile_cache.append((actual_names, rows))

    # Phase 2: read cached data with finalized field mapping
    exp_fields = list(expected_fields.keys())
    rat_zero_fields = cfg.get("rat_zero_fields", [])
    zero_indices = {i for i, name in enumerate(exp_fields) if name in rat_zero_fields}
    count_idx = exp_fields.index("count") if "count" in exp_fields else None

    surveys = []
    survey_index = {}  # value column -> index into surveys list
    for actual_names, rows in tile_cache:
        if not exp_fields:
            continue
        col_map = [actual_names.index(name) for name in exp_fields]
        for row in rows:
            key = row[col_map[0]]
            if key in survey_index:
                if count_idx is not None:
                    idx = survey_index[key]
                    surveys[idx][count_idx] = int(surveys[idx][count_idx]) + int(row[col_map[count_idx]])
                    if surveys[idx][count_idx] > 2147483647:
                        surveys[idx][count_idx] = 2147483647
                continue
            curr = []
            for out_idx, mapped_col in enumerate(col_map):
                entry_val = row[mapped_col]
                if out_idx in zero_indices:
                    entry_val = 0
                curr.append(entry_val)
            survey_index[key] = len(surveys)
            surveys.append(curr)

    return expected_fields, dropped, surveys


def _build_s102_col_map(rat, cfg, exp_fields):
    """Build a name-based column mapping for an S102 featureAttributeTable.

    Uses ``cfg["rat_hdf5_to_field"]`` to map each HDF5 column name to
    its output field name, then resolves the GDAL column index for each
    output field in *exp_fields* order.  This means the output order is
    always determined by ``rat_fields``, regardless of the HDF5 column
    order.

    Parameters
    ----------
    rat : gdal.RasterAttributeTable
        GDAL RAT exposing the S102 featureAttributeTable.
    cfg : dict
        Data source configuration (must contain ``rat_hdf5_to_field``).
    exp_fields : list[str]
        Output field names in order (keys of ``rat_fields``).

    Returns
    -------
    list[int]
        GDAL column indices, one per entry in *exp_fields*.
    """
    hdf5_to_field = cfg.get("rat_hdf5_to_field", {})
    # Invert: output field name -> expected HDF5 name
    field_to_hdf5 = {v: k for k, v in hdf5_to_field.items()}

    actual_names = [
        rat.GetNameOfCol(c).lower()
        for c in range(rat.GetColumnCount())
    ]

    col_map = []
    for out_field in exp_fields:
        hdf5_name = field_to_hdf5.get(out_field)
        if hdf5_name is None:
            raise ValueError(
                f"No HDF5 field mapping for output field '{out_field}'. "
                f"Add it to rat_hdf5_to_field in the data source config.")
        if hdf5_name not in actual_names:
            raise ValueError(
                f"S102 featureAttributeTable missing expected field "
                f"'{hdf5_name}' (mapped to '{out_field}'). "
                f"Actual fields: {actual_names}. "
                f"The HDF5 field layout may have changed. "
                f"Please update noaabathymetry.")
        col_map.append(actual_names.index(hdf5_name))
    return col_map


def _read_rat_data_s102(tiles, project_dir, cfg, exp_fields, expected_fields):
    """Read featureAttributeTable data from S102 tiles using name-based
    column mapping.

    Builds a column mapping on the first tile by matching HDF5 field
    names from ``cfg["rat_hdf5_to_field"]`` against the actual GDAL
    column names.  Deduplicates surveys by the ``value``
    column (first field).  Duplicate rows are skipped (no count
    summing — S102 has no count column).

    Parameters
    ----------
    tiles : list[dict]
        Tile rows from the database.
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration.
    exp_fields : list[str]
        Ordered field names (keys of *expected_fields*).
    expected_fields : dict
        ``{field_name: [python_type, gdal_usage]}`` mapping.

    Returns
    -------
    list[list]
        Survey rows, each a list of values matching *exp_fields* order.
    """
    rat_zero_fields = cfg.get("rat_zero_fields", [])
    zero_indices = {i for i, name in enumerate(expected_fields) if name in rat_zero_fields}
    disk_fields = get_disk_fields(cfg)

    quality_sd = next(sd for sd in cfg["subdatasets"] if sd.get("s102_protocol"))
    quality_name = quality_sd["name"]

    col_map = None

    surveys = []
    survey_index = {}  # value column -> index into surveys list
    for tile in tiles:
        if any(tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df]))
               for df in disk_fields):
            missing = [df for df in disk_fields
                       if tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df]))]
            raise FileNotFoundError(
                f"Tile '{tile.get('tilename', '?')}' is missing file(s) for "
                f"field(s) {missing}. Tiles must be filtered for disk existence "
                f"before RAT aggregation.")
        gtiff = os.path.join(project_dir, tile[disk_fields[0]]).replace('\\', '/')
        ds = gdal.Open(f'S102:"{gtiff}":{quality_name}')
        contrib = ds.GetRasterBand(1)
        rat_n = contrib.GetDefaultRAT()
        if rat_n is None:
            ds = None
            continue

        if col_map is None:
            col_map = _build_s102_col_map(rat_n, cfg, exp_fields)

        for row in range(rat_n.GetRowCount()):
            key = rat_n.GetValueAsString(row, col_map[0])
            if key in survey_index:
                continue
            curr = []
            for out_idx, mapped_col in enumerate(col_map):
                entry_val = rat_n.GetValueAsString(row, mapped_col)
                if out_idx in zero_indices:
                    entry_val = 0
                curr.append(entry_val)
            survey_index[key] = len(surveys)
            surveys.append(curr)
        ds = None

    return surveys


def _write_rat(mosaic_path, surveys, expected_fields, rat_band):
    """Pass 3: create a GDAL RasterAttributeTable and attach it to the mosaic.

    Columns are typed according to *expected_fields*.  Boolean strings
    (``"true"``/``"false"``) are coerced to 0/1 for int/float columns.

    Parameters
    ----------
    mosaic_path : str
        Path to the mosaic file to modify.
    surveys : list[list]
        Survey rows from :func:`_read_rat_data`.
    expected_fields : dict
        ``{field_name: [python_type, gdal_usage]}`` mapping.
    rat_band : int
        1-based band index to attach the RAT to.
    """
    rat = gdal.RasterAttributeTable()
    for entry in expected_fields:
        field_type, usage = expected_fields[entry]
        if field_type == str:
            col_type = gdal.GFT_String
        elif field_type == int:
            col_type = gdal.GFT_Integer
        elif field_type == float:
            col_type = gdal.GFT_Real
        else:
            raise TypeError("Unknown data type for RAT column.")
        rat.CreateColumn(entry, col_type, usage)
    rat.SetRowCount(len(surveys))
    for row_idx, survey in enumerate(surveys):
        for col_idx, entry in enumerate(expected_fields):
            field_type, usage = expected_fields[entry]
            val = survey[col_idx]
            if field_type in (int, float) and isinstance(val, str) and val.lower() in ('true', 'false'):
                val = 1 if val.lower() == 'true' else 0
            if field_type == str:
                rat.SetValueAsString(row_idx, col_idx, val)
            elif field_type == int:
                rat.SetValueAsInt(row_idx, col_idx, int(val))
            elif field_type == float:
                rat.SetValueAsDouble(row_idx, col_idx, float(val))
    ds = gdal.Open(mosaic_path, 1)
    contributor_band = ds.GetRasterBand(rat_band)
    contributor_band.SetDefaultRAT(rat)
    contributor_band = None
    ds = None


def add_mosaic_rat(tiles, project_dir, mosaic_path, cfg, utm=None):
    """Build and attach an aggregated RAT to a mosaic from per-tile RATs.

    Runs the RAT pipeline: discover common fields, read data from all
    tiles, write combined RAT.  No-op if ``cfg["has_rat"]`` is False.

    Parameters
    ----------
    tiles : list[dict]
        Tile rows already filtered for disk existence (from
        ``select_tiles_by_utm``).
    project_dir : str
        Absolute path to the project directory.
    mosaic_path : str
        Path to the mosaic file to attach the RAT to.
    cfg : dict
        Data source configuration.
    """
    if not cfg["has_rat"]:
        return
    expected_fields = dict(cfg["rat_fields"])
    rat_open_method = cfg["rat_open_method"]
    rat_band = cfg.get("rat_band", 3)

    if rat_open_method == "direct":
        expected_fields, dropped_fields, surveys = \
            _discover_and_read_rat_data_direct(
                tiles, project_dir, cfg, expected_fields)
    else:
        # s102_quality: name-based mapping, no field discovery needed.
        # _build_s102_col_map raises if any expected field is missing.
        dropped_fields = set()
        exp_fields = list(expected_fields.keys())
        surveys = _read_rat_data_s102(
            tiles, project_dir, cfg, exp_fields, expected_fields)

    if dropped_fields:
        if utm:
            logger.warning("[UTM%s] RAT field(s) %s were not present "
                           "in all tiles and have been excluded from the "
                           "aggregated RAT.", utm, sorted(dropped_fields))
        else:
            logger.warning("RAT field(s) %s were not present "
                           "in all tiles and have been excluded from the "
                           "aggregated RAT.", sorted(dropped_fields))

    # Write RAT
    _write_rat(mosaic_path, surveys, expected_fields, rat_band)


# ---------------------------------------------------------------------------
# UTM zone management
# ---------------------------------------------------------------------------

def select_unbuilt_utms(conn, cfg, params_key=""):
    """Return ``mosaic_utm`` rows where any built flag is 0 for *params_key*."""
    built_flags = get_mosaic_built_flags(cfg)
    where_clause = " or ".join(f"{f} = 0" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM mosaic_utm WHERE params_key = ? AND ({where_clause})",
        (params_key,),
    )
    return [dict(row) for row in cursor.fetchall()]


def update_utm(conn, fields, cfg):
    """Update a ``mosaic_utm`` row with mosaic/OVR paths, metadata, and built flags.

    Mosaic built flags (e.g. ``built``, ``built_combined``) are always set to 1.
    ``built_hillshade`` is set from the caller's value (default 0).
    """
    all_fields = get_mosaic_fields(cfg)
    built_flags = get_mosaic_built_flags(cfg)
    exclude = {"utm", "params_key", "output_dir", "built_hillshade"}
    exclude.update(built_flags)
    data_cols = [k for k in all_fields if k not in exclude]
    set_parts = [f"{col} = ?" for col in data_cols]
    for f in built_flags:
        set_parts.append(f"{f} = 1")
    set_parts.append("built_hillshade = ?")
    values = [fields.get(col) for col in data_cols]
    values.append(fields.get("built_hillshade", 0))
    params_key = fields.get("params_key", "")
    values.extend([fields["utm"], params_key])
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE mosaic_utm SET {', '.join(set_parts)} WHERE utm = ? AND params_key = ?",
        values,
    )
    conn.commit()


def missing_utms(project_dir, conn, cfg, params_key=""):
    """Detect and reset UTM zones whose mosaic files are missing from disk.

    Scans all built rows for *params_key*.  If any mosaic path is absent
    (or any non-None OVR path is absent), the row is reset to unbuilt
    with all file columns set to NULL.

    Returns
    -------
    list[str]
        UTM zone identifiers that were reset.
    """
    built_flags = get_mosaic_built_flags(cfg)
    where_built = " or ".join(f"{f} = 1" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM mosaic_utm WHERE params_key = ? AND ({where_built})",
        (params_key,),
    )
    utms = [dict(row) for row in cursor.fetchall()]
    missing_utm_list = []
    utm_cols = get_utm_file_columns(cfg)

    for utm in utms:
        missing = False
        for col in utm_cols:
            if "ovr" in col or "aux_xml" in col:
                if utm[col] is not None and not os.path.isfile(os.path.join(project_dir, utm[col])):
                    missing = True
                    break
            else:
                if utm[col] is None or not os.path.isfile(os.path.join(project_dir, utm[col])):
                    missing = True
                    break
        if missing:
            missing_utm_list.append(utm["utm"])
            all_flags = get_all_reset_flags(cfg)
            set_parts = [f"{col} = ?" for col in utm_cols]
            for f in all_flags:
                set_parts.append(f"{f} = 0")
            set_clause = ", ".join(set_parts)
            values = [None] * len(utm_cols) + [utm["utm"], params_key]
            cursor.execute(
                f"UPDATE mosaic_utm SET {set_clause} WHERE utm = ? AND params_key = ?",
                values,
            )
    if missing_utm_list:
        conn.commit()
    return missing_utm_list


def ensure_params_rows(conn, cfg, params_key, output_dir=None):
    """Seed ``mosaic_utm`` rows for a parameterized build partition.

    Copies UTM zones from the default partition (``params_key=''``) into
    the target partition if they don't yet exist, initializing built
    flags to 0 and mosaic/OVR paths to NULL.  This allows parameterized
    builds (e.g. resolution-filtered) to track state independently
    from the default build.

    When *output_dir* is provided, it is stored in the ``output_dir``
    column of each new row.
    """
    all_flags = get_all_reset_flags(cfg)
    utm_cols = get_utm_file_columns(cfg)

    cursor = conn.cursor()
    cursor.execute("SELECT utm FROM mosaic_utm WHERE params_key = ''")
    default_utms = {row["utm"] for row in cursor.fetchall()}

    cursor.execute("SELECT utm FROM mosaic_utm WHERE params_key = ?", (params_key,))
    existing_utms = {row["utm"] for row in cursor.fetchall()}

    new_utms = default_utms - existing_utms
    if not new_utms:
        return

    col_names = ["utm", "params_key", "output_dir"] + utm_cols + all_flags

    col_str = ", ".join(col_names)
    placeholders = ", ".join(["?"] * len(col_names))

    rows = []
    for utm in new_utms:
        values = [utm, params_key, output_dir]
        values.extend([None] * len(utm_cols))
        values.extend([0] * len(all_flags))
        rows.append(tuple(values))

    cursor.executemany(
        f"INSERT OR IGNORE INTO mosaic_utm({col_str}) VALUES({placeholders})",
        rows,
    )
    conn.commit()
