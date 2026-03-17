"""
vrt.py - GDAL Virtual Raster creation, overviews, and RAT aggregation.

Builds flat VRTs per UTM zone from source tiles, with adaptive overviews
targeting standard output resolutions (32m, 64m, 128m).  For multi-subdataset
sources (S102V22, S102V30), one VRT is built per subdataset and then combined.
"""

import copy
import os

from osgeo import gdal

from nbs.bluetopo._internal.config import (
    VALID_TARGET_RESOLUTIONS,
    get_built_flags,
    get_disk_field,
    get_disk_fields,
    get_utm_file_columns,
)

gdal.UseExceptions()
gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")
gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")


# ---------------------------------------------------------------------------
# VRT creation
# ---------------------------------------------------------------------------

def create_vrt(files, vrt_path, levels, relative_to_vrt,
               band_descriptions=None, separate=False,
               target_resolution=None):
    """Build a single GDAL VRT file with optional overviews.

    Any existing VRT and .ovr at *vrt_path* are removed first.
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
    if target_resolution is not None:
        if target_resolution not in VALID_TARGET_RESOLUTIONS:
            raise ValueError(
                f"target_resolution must be one of {sorted(VALID_TARGET_RESOLUTIONS)}, "
                f"got {target_resolution}"
            )
        opts_str += f' -resolution user -tr {target_resolution} {target_resolution}'
    else:
        opts_str += ' -resolution highest'
    vrt_options = gdal.BuildVRTOptions(options=opts_str, resampleAlg="near")
    cwd = os.getcwd()
    try:
        os.chdir(os.path.dirname(vrt_path))
        if relative_to_vrt is True:
            for idx in range(len(files)):
                if 'S102:' in files[idx]:
                    continue
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


def compute_overview_factors(tile_paths, target_resolution=None):
    """Compute adaptive overview factors based on source tile resolutions."""
    resolutions = set()
    for path in tile_paths:
        ds = gdal.Open(path)
        gt = ds.GetGeoTransform()
        resolutions.add(round(abs(gt[1])))
        ds = None

    if not resolutions:
        return []

    native_res = target_resolution if target_resolution else min(resolutions)
    coarsest_res = max(resolutions)

    target_output_resolutions = [32, 64, 128]
    targets = [r for r in target_output_resolutions if r > coarsest_res]
    factors = [round(t / native_res) for t in targets if native_res > 0]
    factors = [f for f in factors if f >= 2]
    return sorted(factors)


# ---------------------------------------------------------------------------
# Tile selection and path building
# ---------------------------------------------------------------------------

def select_tiles_by_utm(project_dir, conn, utm, cfg):
    """Return tiles in a UTM zone whose files exist on disk, sorted coarse-to-fine."""
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

    def _res_sort_key(tile):
        raw = tile.get("resolution") or ""
        digits = ''.join(c for c in raw if c.isdigit())
        if not digits:
            raise ValueError(
                f"Tile '{tile.get('tilename', '?')}' has non-numeric or empty "
                f"resolution '{raw}'.")
        return int(digits)

    existing_tiles.sort(key=_res_sort_key, reverse=True)
    return existing_tiles


def build_tile_paths(tiles, project_dir, cfg, subdataset=None):
    """Build file paths for source tiles, applying S102 protocol URIs if needed."""
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
    """Pass 1: determine common RAT field subset across all tiles (direct method only).

    Returns (filtered_expected_fields, dropped_field_names).
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

    Returns list of survey rows (each a list of values).
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
    """Pass 3: create and attach RAT to the VRT."""
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
    """Build and attach a RAT to a UTM VRT by aggregating tile RATs.

    No-op if cfg["has_rat"] is False.
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

def select_unbuilt_utms(conn, cfg):
    """Retrieve all unbuilt UTM records."""
    built_flags = get_built_flags(cfg)
    if cfg["subdatasets"]:
        all_flags = built_flags + ["built_combined"]
        where_clause = " or ".join(f"{f} = 0" for f in all_flags)
    else:
        where_clause = " or ".join(f"{f} = 0" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM vrt_utm WHERE {where_clause}")
    return [dict(row) for row in cursor.fetchall()]


def update_utm(conn, fields, cfg):
    """Update a UTM record with VRT/OVR paths and set built flags to 1."""
    utm_cols = get_utm_file_columns(cfg)
    built_flags = get_built_flags(cfg)
    set_parts = [f"{col} = ?" for col in utm_cols]
    for f in built_flags:
        set_parts.append(f"{f} = 1")
    if cfg["subdatasets"]:
        set_parts.append("built_combined = 1")
    set_clause = ", ".join(set_parts)
    values = [fields.get(col) for col in utm_cols]
    values.append(fields["utm"])
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE vrt_utm SET {set_clause} WHERE utm = ?",
        values,
    )
    conn.commit()


def missing_utms(project_dir, conn, cfg):
    """Reset UTM zones whose VRT files are missing from disk.

    OVR columns with None are treated as "no overview needed" and not
    considered missing.

    Returns the number of UTM zones reset.
    """
    built_flags = get_built_flags(cfg)
    if cfg["subdatasets"]:
        where_built = " or ".join(f"{f} = 1" for f in built_flags + ["built_combined"])
    else:
        where_built = " or ".join(f"{f} = 1" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM vrt_utm WHERE {where_built}")
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
            values = [None] * len(utm_cols) + [utm["utm"]]
            cursor.execute(
                f"UPDATE vrt_utm SET {set_clause} WHERE utm = ?",
                values,
            )
    if missing_utm_count:
        conn.commit()
    return missing_utm_count
