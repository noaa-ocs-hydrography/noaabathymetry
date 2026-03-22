"""
vrt.py - GDAL Virtual Raster creation, overviews, and RAT aggregation.

Builds flat VRTs per UTM zone from source tiles, with adaptive overviews
targeting config-driven output resolutions, optionally filtered to above
the coarsest source.  For multi-subdataset
sources (S102V22, S102V30), one VRT is built per subdataset and then combined.
"""

import copy
import os

from osgeo import gdal

from nbs.bluetopo._internal.config import (
    parse_resolution,
    validate_vrt_resolution_target,
    get_built_flags,
    get_disk_field,
    get_disk_fields,
    get_utm_file_columns,
)

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
        print(f"GDAL cache max: {gdal.GetCacheMax() / 1024**3:.1f} GB")
except (AttributeError, ValueError, OSError):
    pass  # Windows or unsupported platform — keep GDAL's default


# ---------------------------------------------------------------------------
# VRT creation
# ---------------------------------------------------------------------------

def create_vrt(files, vrt_path, levels, relative_to_vrt,
               band_descriptions=None, separate=False,
               vrt_resolution_target=None):
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
    vrt_resolution_target : float | None
        Force output pixel size in meters.  Uses ``-resolution highest``
        when None.
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
    if vrt_resolution_target is not None:
        validate_vrt_resolution_target(vrt_resolution_target)
        opts_str += f' -resolution user -tr {vrt_resolution_target} {vrt_resolution_target}'
    else:
        opts_str += ' -resolution highest'
    vrt_options = gdal.BuildVRTOptions(options=opts_str, resampleAlg="near")
    # BuildVRT resolves relative paths from cwd, so we chdir to the VRT's
    # directory to ensure the stored references are correct.
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                if 'S102:' in files[idx]:
                    continue  # S102 URIs have their own path format
                files[idx] = os.path.relpath(files[idx], os.path.dirname(vrt_path))
        relative_vrt_path = os.path.relpath(vrt_path, os.getcwd())
        vrt = gdal.BuildVRT(relative_vrt_path, files, options=vrt_options)
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


def generate_hillshade(vrt_path, hillshade_path):
    """Generate a hillshade GeoTIFF from band 1 (Elevation) of a source raster.

    Builds from a 16m downsampled view of the source for speed while
    preserving good terrain detail. Uses an in-memory VRT with
    resolution override so GDAL reads from the source's existing
    overviews instead of full resolution.

    Uses azimuth 315, altitude 45, vertical exaggeration 4x, and
    builds BILINEAR overviews at factors 2/4/8.
    """
    if os.path.isfile(hillshade_path):
        os.remove(hillshade_path)
    # Create an in-memory VRT at 16m resolution. GDAL reads from the
    # source's overview levels instead of full resolution.
    mem_vrt = "/vsimem/_hillshade_input.vrt"
    gdal.Translate(mem_vrt, vrt_path, format="VRT", xRes=16, yRes=16)
    opts = gdal.DEMProcessingOptions(
        options="-az 315 -alt 45 -z 4 -compute_edges "
                "-of GTiff -co COMPRESS=DEFLATE -co TILED=YES -co BIGTIFF=YES"
    )
    gdal.DEMProcessing(hillshade_path, mem_vrt, "hillshade", options=opts)
    gdal.Unlink(mem_vrt)
    ds = gdal.Open(hillshade_path, 0)
    ds.BuildOverviews("BILINEAR", [2, 4, 8])
    ds = None
    return hillshade_path


def reproject_to_web_mercator(vrt_path, output_path, overview_factors=None):
    """Reproject a VRT to an EPSG:3857 GeoTIFF using gdal.Warp.

    Produces a GeoTIFF with DEFLATE compression and 512x512 tiling.
    Uses nearest neighbor resampling to preserve categorical Contributor
    band values for RAT compatibility.

    The RAT is not preserved through Warp. Call ``add_vrt_rat()`` on the
    output file after this function to attach the aggregated RAT.

    Parameters
    ----------
    vrt_path : str
        Path to the source VRT (in UTM projection).
    output_path : str
        Path for the output GeoTIFF (will be in EPSG:3857).
    overview_factors : list[int] | None
        Overview factors from ``compute_overview_factors()``.  If None
        or empty, no overviews are built.

    Returns
    -------
    str
        The *output_path*.
    """
    if os.path.isfile(output_path):
        os.remove(output_path)
    opts = gdal.WarpOptions(
        dstSRS="EPSG:3857",
        format="GTiff",
        resampleAlg="near",
        multithread=True,
        warpOptions=["NUM_THREADS=ALL_CPUS"],
        creationOptions=[
            "COMPRESS=DEFLATE",
            "TILED=YES",
            "BLOCKXSIZE=512",
            "BLOCKYSIZE=512",
            "BIGTIFF=YES",
            "NUM_THREADS=ALL_CPUS",
        ],
    )
    gdal.Warp(output_path, vrt_path, options=opts)
    if overview_factors:
        ds = gdal.Open(output_path, 0)
        ds.BuildOverviews("NEAREST", overview_factors)
        ds = None
    return output_path


def compute_overview_factors(tile_paths, vrt_resolution_target=None,
                             overview_levels=None, filter_coarsest=True):
    """Compute overview factors from source tile resolutions.

    Parameters
    ----------
    tile_paths : list[str]
        Paths to source raster files.
    vrt_resolution_target : float | None
        Override native resolution for factor calculation.
    overview_levels : list[int]
        Candidate overview resolutions.  Must be provided.
    filter_coarsest : bool
        When True, only resolutions above the coarsest source
        are kept.  When False, all listed levels are candidates.
    """
    if overview_levels is None:
        raise ValueError("overview_levels must be provided")

    resolutions = set()
    for path in tile_paths:
        ds = gdal.Open(path)
        gt = ds.GetGeoTransform()
        resolutions.add(round(abs(gt[1])))
        ds = None

    if not resolutions:
        return []

    native_res = vrt_resolution_target if vrt_resolution_target else min(resolutions)
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
        print(f"Did not find the files for {missing_count} "
              f"registered tile(s) in utm {utm}. "
              "Run fetch_tiles to retrieve files "
              "or correct the directory path if incorrect.")

    if tile_resolution_filter:
        res_set = set(tile_resolution_filter)
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
# RAT aggregation (split into discovery / read / write)
# ---------------------------------------------------------------------------

def _discover_rat_fields(tiles, project_dir, cfg, expected_fields):
    """Pass 1: determine common RAT field subset across all tiles.

    Only used for the ``"direct"`` RAT open method.  Intersects the
    expected field set with the actual columns found in each tile's RAT,
    so the aggregated output only contains fields present in *all* tiles.

    Returns
    -------
    tuple[dict, set[str]]
        ``(filtered_expected_fields, dropped_field_names)``
    """
    rat_band = cfg.get("rat_band", 3)
    disk_fields = get_disk_fields(cfg)
    dropped = set()

    for tile in tiles:
        if any(tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df]))
               for df in disk_fields):
            continue
        gtiff = os.path.join(project_dir, tile[disk_fields[0]])
        ds = gdal.Open(gtiff)
        contrib = ds.GetRasterBand(rat_band)
        rat_n = contrib.GetDefaultRAT()
        if rat_n is None:
            ds = None
            continue
        actual_set = set(
            rat_n.GetNameOfCol(i).lower()
            for i in range(rat_n.GetColumnCount())
        )
        ds = None
        before = set(expected_fields.keys())
        expected_fields = {
            k: v for k, v in expected_fields.items() if k in actual_set
        }
        dropped |= before - set(expected_fields.keys())

    return expected_fields, dropped


def _read_rat_data(tiles, project_dir, cfg, exp_fields, expected_fields):
    """Pass 2: read RAT data from all tiles using finalized field mapping.

    Deduplicates surveys by the ``value`` column (first field).  For
    the ``"direct"`` method, duplicate rows have their ``count`` field
    summed (capped at INT32_MAX).

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
    rat_open_method = cfg["rat_open_method"]
    rat_band = cfg.get("rat_band", 3)
    rat_zero_fields = cfg.get("rat_zero_fields", [])
    zero_indices = {i for i, name in enumerate(expected_fields) if name in rat_zero_fields}
    disk_fields = get_disk_fields(cfg)

    surveys = []
    survey_index = {}  # value column -> index into surveys list
    for tile in tiles:
        if any(tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df]))
               for df in disk_fields):
            continue

        if rat_open_method == "direct":
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
            col_map = [actual_names.index(name) for name in exp_fields]
        elif rat_open_method == "s102_quality":
            quality_sd = next(sd for sd in cfg["subdatasets"] if sd.get("s102_protocol"))
            quality_name = quality_sd["name"]
            gtiff = os.path.join(project_dir, tile[disk_fields[0]]).replace('\\', '/')
            ds = gdal.Open(f'S102:"{gtiff}":{quality_name}')
            contrib = ds.GetRasterBand(1)
            rat_n = contrib.GetDefaultRAT()
            if rat_n is None:
                ds = None
                continue
            actual_count = rat_n.GetColumnCount()
            usable = min(actual_count, len(exp_fields))
            col_map = list(range(usable))
        else:
            continue

        for row in range(rat_n.GetRowCount()):
            key = rat_n.GetValueAsString(row, col_map[0])
            if key in survey_index:
                if rat_open_method == "direct":
                    idx = survey_index[key]
                    surveys[idx][1] = int(surveys[idx][1]) + rat_n.GetValueAsInt(row, col_map[1])
                    if surveys[idx][1] > 2147483647:
                        surveys[idx][1] = 2147483647
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


def _write_rat(vrt_path, surveys, expected_fields, rat_band):
    """Pass 3: create a GDAL RasterAttributeTable and attach it to the VRT.

    Columns are typed according to *expected_fields*.  Boolean strings
    (``"true"``/``"false"``) are coerced to 0/1 for int/float columns.

    Parameters
    ----------
    vrt_path : str
        Path to the VRT file to modify.
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
    vrt_ds = gdal.Open(vrt_path, 1)
    contributor_band = vrt_ds.GetRasterBand(rat_band)
    contributor_band.SetDefaultRAT(rat)
    contributor_band = None
    vrt_ds = None


def add_vrt_rat(conn, utm, project_dir, vrt_path, cfg):
    """Build and attach an aggregated RAT to a UTM VRT from per-tile RATs.

    Runs the three-pass RAT pipeline: discover common fields, read data
    from all tiles, write combined RAT.  No-op if ``cfg["has_rat"]`` is
    False.
    """
    if not cfg["has_rat"]:
        return
    expected_fields = dict(cfg["rat_fields"])
    rat_open_method = cfg["rat_open_method"]
    rat_band = cfg.get("rat_band", 3)

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    tiles = [dict(row) for row in cursor.fetchall()]

    # Pass 1: discover common fields (direct method only)
    dropped_fields = set()
    if rat_open_method == "direct":
        expected_fields, dropped_fields = _discover_rat_fields(
            tiles, project_dir, cfg, expected_fields)
    exp_fields = list(expected_fields.keys())

    # Pass 2: read RAT data
    surveys = _read_rat_data(tiles, project_dir, cfg, exp_fields, expected_fields)

    # Trim expected_fields if surveys are narrower (s102_quality safety)
    if surveys:
        survey_width = len(surveys[0])
        if survey_width < len(expected_fields):
            expected_fields = dict(list(expected_fields.items())[:survey_width])

    if dropped_fields:
        print(f"Warning: RAT field(s) {sorted(dropped_fields)} were not present "
              f"in all tiles and have been excluded from the aggregated RAT.")

    # Pass 3: write RAT
    _write_rat(vrt_path, surveys, expected_fields, rat_band)


# ---------------------------------------------------------------------------
# UTM zone management
# ---------------------------------------------------------------------------

def select_unbuilt_utms(conn, cfg, params_key=""):
    """Return ``vrt_utm`` rows where any built flag is 0 for *params_key*."""
    built_flags = get_built_flags(cfg)
    if cfg["subdatasets"]:
        all_flags = built_flags + ["built_combined"]
        where_clause = " or ".join(f"{f} = 0" for f in all_flags)
    else:
        where_clause = " or ".join(f"{f} = 0" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM vrt_utm WHERE params_key = ? AND ({where_clause})",
        (params_key,),
    )
    return [dict(row) for row in cursor.fetchall()]


def update_utm(conn, fields, cfg):
    """Update a ``vrt_utm`` row with VRT/OVR paths and set all built flags to 1."""
    utm_cols = get_utm_file_columns(cfg)
    built_flags = get_built_flags(cfg)
    set_parts = [f"{col} = ?" for col in utm_cols]
    for f in built_flags:
        set_parts.append(f"{f} = 1")
    if cfg["subdatasets"]:
        set_parts.append("built_combined = 1")
    set_clause = ", ".join(set_parts)
    values = [fields.get(col) for col in utm_cols]
    params_key = fields.get("params_key", "")
    values.extend([fields["utm"], params_key])
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE vrt_utm SET {set_clause} WHERE utm = ? AND params_key = ?",
        values,
    )
    conn.commit()


def missing_utms(project_dir, conn, cfg, params_key=""):
    """Detect and reset UTM zones whose VRT files are missing from disk.

    Scans all built rows for *params_key*.  If any VRT path is absent
    (or any non-None OVR path is absent), the row is reset to unbuilt
    with all file columns set to NULL.

    Returns
    -------
    int
        Number of UTM zones reset.
    """
    built_flags = get_built_flags(cfg)
    if cfg["subdatasets"]:
        where_built = " or ".join(f"{f} = 1" for f in built_flags + ["built_combined"])
    else:
        where_built = " or ".join(f"{f} = 1" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM vrt_utm WHERE params_key = ? AND ({where_built})",
        (params_key,),
    )
    utms = [dict(row) for row in cursor.fetchall()]
    missing_utm_count = 0
    utm_cols = get_utm_file_columns(cfg)

    for utm in utms:
        missing = False
        for col in utm_cols:
            if "ovr" in col:
                if utm[col] is not None and not os.path.isfile(os.path.join(project_dir, utm[col])):
                    missing = True
                    break
            else:
                if utm[col] is None or not os.path.isfile(os.path.join(project_dir, utm[col])):
                    missing = True
                    break
        if missing:
            missing_utm_count += 1
            set_parts = [f"{col} = ?" for col in utm_cols]
            for f in built_flags:
                set_parts.append(f"{f} = 0")
            if cfg["subdatasets"]:
                set_parts.append("built_combined = 0")
            set_clause = ", ".join(set_parts)
            values = [None] * len(utm_cols) + [utm["utm"], params_key]
            cursor.execute(
                f"UPDATE vrt_utm SET {set_clause} WHERE utm = ? AND params_key = ?",
                values,
            )
    if missing_utm_count:
        conn.commit()
    return missing_utm_count


def ensure_params_rows(conn, cfg, params_key):
    """Seed ``vrt_utm`` rows for a parameterized build partition.

    Copies UTM zones from the default partition (``params_key=''``) into
    the target partition if they don't yet exist, initializing built
    flags to 0 and VRT/OVR paths to NULL.  This allows parameterized
    builds (e.g. resolution-filtered) to track state independently
    from the default build.
    """
    built_flags = get_built_flags(cfg)
    utm_cols = get_utm_file_columns(cfg)

    cursor = conn.cursor()
    cursor.execute("SELECT utm FROM vrt_utm WHERE params_key = ''")
    default_utms = {row["utm"] for row in cursor.fetchall()}

    cursor.execute("SELECT utm FROM vrt_utm WHERE params_key = ?", (params_key,))
    existing_utms = {row["utm"] for row in cursor.fetchall()}

    new_utms = default_utms - existing_utms
    if not new_utms:
        return

    col_names = ["utm", "params_key"] + utm_cols + built_flags
    if cfg["subdatasets"]:
        col_names.append("built_combined")

    col_str = ", ".join(col_names)
    placeholders = ", ".join(["?"] * len(col_names))

    rows = []
    for utm in new_utms:
        values = [utm, params_key]
        values.extend([None] * len(utm_cols))
        values.extend([0] * len(built_flags))
        if cfg["subdatasets"]:
            values.append(0)
        rows.append(tuple(values))

    cursor.executemany(
        f"INSERT OR IGNORE INTO vrt_utm({col_str}) VALUES({placeholders})",
        rows,
    )
    conn.commit()
