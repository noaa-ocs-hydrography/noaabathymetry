"""
build_vrt.py - Build GDAL Virtual Rasters from downloaded NBS tiles.

Creates a flat VRT per UTM zone directly from source tiles, with adaptive
overviews targeting standard output resolutions (32m, 64m, 128m) above
the coarsest source tile resolution.

For multi-subdataset sources (S102V22, S102V30), one VRT is built per
subdataset (BathymetryCoverage, QualityOfSurvey /
QualityOfBathymetryCoverage) and then combined into a single UTM VRT
using the ``-separate`` flag.

State is tracked in a SQLite registry DB (``<source>_registry.db``) with
``built`` flags on the ``vrt_utm`` table.  Only unbuilt records are
processed on each run, making the operation resumable.
"""

import copy
import datetime
import os
import platform
import sqlite3

from osgeo import gdal

from nbs.bluetopo.core.datasource import (
    _timestamp,
    get_config,
    get_local_config,
    get_catalog_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_built_flags,
    get_utm_file_columns,
    get_disk_field,
    get_disk_fields,
)

gdal.UseExceptions()
gdal.SetConfigOption("COMPRESS_OVERVIEW", "DEFLATE")
gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS")


def connect_to_survey_registry(project_dir: str, cfg: dict) -> sqlite3.Connection:
    """
    Create or connect to the SQLite survey registry database.

    The registry contains three tables whose schemas are driven by *cfg*:

    - **catalog** (or **tileset**) -- tracks downloaded tessellation and XML files.
    - **tiles** -- one row per tile with links, disk paths, checksums, and verified flags.
    - **vrt_utm** -- VRT/OVR paths and built flags per UTM zone.

    On first run, tables are created.  On subsequent runs, any new columns
    required by the config (e.g. after a schema change) are added via
    ``ALTER TABLE ADD COLUMN``.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration from ``datasource.get_config()``.

    Returns
    -------
    conn : sqlite3.Connection
        Connection with ``row_factory = sqlite3.Row``.
    """
    data_source = cfg["canonical_name"]
    catalog_fields = get_catalog_fields(cfg)
    vrt_utm_fields = get_vrt_utm_fields(cfg)
    vrt_tiles = get_tiles_fields(cfg)
    catalog_table = cfg["catalog_table"]
    catalog_pk = cfg["catalog_pk"]

    database_path = os.path.join(project_dir, f"{data_source.lower()}_registry.db")
    conn = None
    try:
        conn = sqlite3.connect(database_path)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as e:
        print("Failed to establish SQLite database connection.")
        raise e
    if conn is not None:
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {catalog_table} (
                {catalog_pk} text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS vrt_utm (
                utm text PRIMARY KEY
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tiles (
                tilename text PRIMARY KEY
                );
                """
            )
            conn.commit()
            table_field_pairs = [
                (catalog_table, catalog_fields),
                ("vrt_utm", vrt_utm_fields),
                ("tiles", vrt_tiles),
            ]
            for table_name, field_dict in table_field_pairs:
                cursor.execute(f"SELECT name FROM pragma_table_info('{table_name}')")
                existing = [dict(row)["name"] for row in cursor.fetchall()]
                for field, ftype in field_dict.items():
                    if field not in existing:
                        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {field} {ftype}")
                        conn.commit()
        except sqlite3.Error as e:
            print("Failed to create SQLite tables.")
            raise e
    return conn


def create_vrt(files: list, vrt_path: str, levels: list, relative_to_vrt: bool,
               band_descriptions: list = None, separate: bool = False,
               target_resolution: float = None) -> None:
    """
    Build a single GDAL VRT file with optional overviews.

    Any existing VRT and ``.ovr`` at *vrt_path* are removed first.  File
    paths prefixed with ``S102:`` (the GDAL S-102 driver protocol) are left
    as-is when converting to relative paths, since relpath would break the
    driver URI format.

    Parameters
    ----------
    files : list[str]
        Input file paths (GeoTIFFs, VRTs, or ``S102:"..."`` URIs).
    vrt_path : str
        Absolute output VRT path.
    levels : list[int] | None
        Overview levels to build (e.g. ``[2, 4]``).  None to skip overviews.
    relative_to_vrt : bool
        If True, referenced file paths inside the VRT are stored as relative
        to the VRT's directory.
    band_descriptions : list[str] | None
        Labels to set on each band (e.g. ``["Elevation", "Uncertainty"]``).
    separate : bool
        If True, uses ``-separate`` to stack inputs as separate bands rather
        than mosaicking them spatially.  Used for combining subdataset VRTs
        into a single multi-band UTM VRT.
    target_resolution : float | None
        When set, forces the output pixel size to this value (in meters)
        using ``resolution="user"`` instead of ``resolution="highest"``.
        Per-resolution VRTs are unaffected; this is intended for complete
        and UTM VRTs.
    """
    # not efficient but insignificant
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
        if target_resolution <= 0:
            raise ValueError(f"target_resolution must be positive, got {target_resolution}")
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
    except Exception:
        raise RuntimeError(f"VRT failed to build for {vrt_path}")
    finally:
        os.chdir(cwd)
    if levels:
        vrt = gdal.Open(vrt_path, 0)
        vrt.BuildOverviews("NEAREST", levels)
        vrt = None


def compute_overview_factors(tile_paths: list,
                             target_resolution: float = None) -> list:
    """
    Compute adaptive overview factors based on source tile resolutions.

    Targets standard output resolutions (32m, 64m, 128m) above the coarsest
    source tile resolution.  Factors are relative to the VRT's native
    resolution — either *target_resolution* if set, or the finest source
    tile resolution.

    Parameters
    ----------
    tile_paths : list[str]
        Source file paths (GeoTIFFs, BAGs, or ``S102:"..."`` URIs).
    target_resolution : float | None
        If set, used as the VRT's native resolution for factor computation
        instead of the finest source tile.

    Returns
    -------
    list[int]
        Overview factors (e.g. ``[16, 32, 64]``).  Empty if no useful
        factors can be computed.
    """
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
    # Filter out factors < 2 (no point building an overview at or above native)
    factors = [f for f in factors if f >= 2]
    return sorted(factors)


def select_tiles_by_utm(project_dir: str, conn: sqlite3.Connection,
                        utm: str, cfg: dict) -> list:
    """
    Return tiles in a UTM zone whose files exist on disk.

    Tiles are sorted coarse-to-fine by resolution so that higher-resolution
    data is added last and takes priority in GDAL BuildVRT overlap areas.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    conn : sqlite3.Connection
        Database connection.
    utm : str
        UTM zone identifier.
    cfg : dict
        Data source configuration.

    Returns
    -------
    list[dict]
        Tile records sorted coarse-to-fine, with disk files confirmed.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    tiles = [dict(row) for row in cursor.fetchall()]
    disk_fields = get_disk_fields(cfg)

    def tile_exists(tile):
        for df in disk_fields:
            if not tile.get(df) or not os.path.isfile(os.path.join(project_dir, tile[df])):
                return False
        return True

    existing_tiles = [tile for tile in tiles if tile_exists(tile)]
    if len(tiles) - len(existing_tiles) != 0:
        print(f"Did not find the files for {len(tiles) - len(existing_tiles)} "
              f"registered tile(s) in utm {utm}. "
              "Run fetch_tiles to retrieve files "
              "or correct the directory path if incorrect.")

    def _res_sort_key(tile):
        return int(''.join(c for c in tile["resolution"] if c.isdigit()))

    # Sort coarse-to-fine (descending numeric) so higher-res data is added
    # last and takes priority in GDAL BuildVRT overlap areas.
    existing_tiles.sort(key=_res_sort_key, reverse=True)
    return existing_tiles


def _build_tile_paths(tiles: list, project_dir: str, cfg: dict,
                      subdataset: dict = None) -> list:
    """
    Build file paths for source tiles, applying S102 protocol URIs if needed.

    Parameters
    ----------
    tiles : list[dict]
        Tile records (already sorted).
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration.
    subdataset : dict | None
        If provided, the subdataset config dict.  Tiles using the S102
        protocol will be wrapped as ``S102:"path":SubdatasetName``.

    Returns
    -------
    list[str]
        File paths or S102 URIs ready for ``create_vrt()``.
    """
    disk_field = get_disk_field(cfg)
    paths = []
    for tile in tiles:
        fpath = os.path.join(project_dir, tile[disk_field])
        if subdataset and subdataset.get("s102_protocol"):
            fpath = fpath.replace("\\", "/")
            if fpath.startswith('/') and not fpath.startswith('//'):
                paths.append(f'S102:"/{fpath}":{subdataset["name"]}')
            else:
                paths.append(f'S102:"{fpath}":{subdataset["name"]}')
        else:
            paths.append(fpath)
    return paths


def add_vrt_rat(conn: sqlite3.Connection, utm: str, project_dir: str,
                vrt_path: str, cfg: dict) -> None:
    """
    Build and attach a GDAL Raster Attribute Table (RAT) to a UTM VRT.

    The RAT aggregates per-survey metadata from individual tile RATs into a
    single table on the UTM VRT.  Surveys are identified by their first
    column value (typically a unique ID); if the same survey appears in
    multiple tiles, their pixel counts (column 1) are summed and capped at
    ``2^31 - 1``.

    Two methods for reading tile RATs are supported:

    - ``"direct"`` -- opens the GeoTIFF directly and reads the RAT from
      ``cfg["rat_band"]`` (band 3 for BlueTopo/Modeling/HSD).
    - ``"s102_quality"`` -- opens via ``S102:"path":<quality_group>`` and
      reads band 1, then writes the aggregated RAT to ``cfg["rat_band"]``
      (band 3) of the combined UTM VRT.  The quality group name is taken
      from the second subdataset's ``name`` field (e.g.
      ``QualityOfSurvey`` for S102V22, ``QualityOfBathymetryCoverage``
      for S102V30).

    The RAT schema (columns, types, and GDAL usages) is defined by
    ``cfg["rat_fields"]``.  Fields listed in ``cfg["rat_zero_fields"]`` have
    their values forced to 0 during aggregation.

    No-op if ``cfg["has_rat"]`` is False.

    Parameters
    ----------
    conn : sqlite3.Connection
        Database connection for tile queries.
    utm : str
        UTM zone identifier to select tiles.
    project_dir : str
        Absolute path to the project directory.
    vrt_path : str
        Absolute path to the UTM VRT file to receive the RAT.
    cfg : dict
        Data source configuration from ``datasource.get_config()``.
    """
    if not cfg["has_rat"]:
        return
    expected_fields = dict(cfg["rat_fields"])
    rat_open_method = cfg["rat_open_method"]
    rat_band = cfg.get("rat_band", 3)
    # TODO: clarify why feature_size_var and bathymetric_uncertainty_type
    # (type_of_bathymetric_estimation_uncertainty in v3.0) are forced to 0
    # during aggregation. Inherited from early S102V22 code — needs
    # confirmation on whether this is still required.
    rat_zero_fields = cfg.get("rat_zero_fields", [])
    zero_indices = {i for i, name in enumerate(expected_fields) if name in rat_zero_fields}

    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE utm = ?", (utm,))
    tiles = [dict(row) for row in cursor.fetchall()]
    surveys = []
    disk_fields = get_disk_fields(cfg)
    dropped_fields = set()

    def _tile_on_disk(tile):
        for df in disk_fields:
            if tile.get(df) is None or not os.path.isfile(os.path.join(project_dir, tile[df])):
                return False
        return True

    # Pass 1: determine common field subset across all tiles
    if rat_open_method == "direct":
        for tile in tiles:
            if not _tile_on_disk(tile):
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
            dropped_fields |= before - set(expected_fields.keys())

    exp_fields = list(expected_fields.keys())

    # Pass 2: read data using finalized field mapping
    for tile in tiles:
        if not _tile_on_disk(tile):
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
            quality_name = cfg["subdatasets"][1]["name"]
            gtiff = os.path.join(project_dir, tile[disk_fields[0]]).replace('\\', '/')
            ds = gdal.Open(f'S102:"{gtiff}":{quality_name}')
            contrib = ds.GetRasterBand(1)
            rat_n = contrib.GetDefaultRAT()
            if rat_n is None:
                ds = None
                continue
            col_map = list(range(rat_n.GetColumnCount()))
        else:
            continue

        for row in range(rat_n.GetRowCount()):
            exist = False
            for survey in surveys:
                if survey[0] == rat_n.GetValueAsString(row, col_map[0]):
                    survey[1] = int(survey[1]) + rat_n.GetValueAsInt(row, col_map[1])
                    if survey[1] > 2147483647:
                        survey[1] = 2147483647
                    exist = True
                    break
            if exist:
                continue
            curr = []
            for out_idx, mapped_col in enumerate(col_map):
                entry_val = rat_n.GetValueAsString(row, mapped_col)
                if out_idx in zero_indices:
                    entry_val = 0
                curr.append(entry_val)
            surveys.append(curr)
        ds = None

    if dropped_fields:
        print(f"Warning: RAT field(s) {sorted(dropped_fields)} were not present "
              f"in all tiles and have been excluded from the aggregated RAT.")

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
            raise TypeError("Unknown data type submitted for gdal raster attribute table.")
        rat.CreateColumn(entry, col_type, usage)
    rat.SetRowCount(len(surveys))
    for row_idx, survey in enumerate(surveys):
        for col_idx, entry in enumerate(expected_fields):
            field_type, usage = expected_fields[entry]
            val = survey[col_idx]
            # GDAL 3.12+ returns 'true'/'false' for boolean S102 fields
            # that earlier versions returned as '0'/'1'.
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


def select_unbuilt_utms(conn: sqlite3.Connection, cfg: dict) -> list:
    """
    Retrieve all unbuilt utm records.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    cfg : dict
        data source configuration.

    Returns
    -------
    utms : list
        list of unbuilt utm records.
    """
    built_flags = get_built_flags(cfg)
    if cfg["subdatasets"]:
        all_flags = built_flags + ["built_combined"]
        where_clause = " or ".join(f"{f} = 0" for f in all_flags)
    else:
        where_clause = " or ".join(f"{f} = 0" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM vrt_utm WHERE {where_clause}")
    return [dict(row) for row in cursor.fetchall()]


def update_utm(conn: sqlite3.Connection, fields: dict, cfg: dict) -> None:
    """
    Update utm records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    fields : dict
        dictionary with the name of the UTM zone and paths for its associated
        VRT and OVR files.
    cfg : dict
        data source configuration.
    """
    utm_cols = get_utm_file_columns(cfg)
    built_flags = get_built_flags(cfg)
    set_parts = [f"{col} = ?" for col in utm_cols]
    if cfg["subdatasets"]:
        for f in built_flags:
            set_parts.append(f"{f} = 1")
        set_parts.append("built_combined = 1")
    else:
        for f in built_flags:
            set_parts.append(f"{f} = 1")
    set_clause = ", ".join(set_parts)
    values = [fields.get(col) for col in utm_cols]
    values.append(fields["utm"])
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE vrt_utm SET {set_clause} WHERE utm = ?",
        values,
    )
    conn.commit()


def missing_utms(project_dir: str, conn: sqlite3.Connection, cfg: dict) -> int:
    """
    Reset UTM zones whose VRT/OVR files are missing from disk.

    Scans all UTM zones marked as built and resets any whose VRT/OVR files
    are missing from disk.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    conn : sqlite3.Connection
        Database connection.
    cfg : dict
        Data source configuration.

    Returns
    -------
    int
        Number of UTM zones that were reset.
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
            if utm[col] is None or not os.path.isfile(os.path.join(project_dir, utm[col])):
                missing = True
                break
        if missing:
            missing_utm_count += 1
            set_parts = [f"{col} = ?" for col in utm_cols]
            if cfg["subdatasets"]:
                for f in built_flags:
                    set_parts.append(f"{f} = 0")
                set_parts.append("built_combined = 0")
            else:
                for f in built_flags:
                    set_parts.append(f"{f} = 0")
            set_clause = ", ".join(set_parts)
            values = [None] * len(utm_cols) + [utm["utm"]]
            cursor.execute(
                f"UPDATE vrt_utm SET {set_clause} WHERE utm = ?",
                values,
            )
            conn.commit()
    return missing_utm_count


def main(project_dir: str, data_source: str = None, relative_to_vrt: bool = True,
         target_resolution: float = None) -> None:
    """
    Build a flat GDAL VRT per UTM zone from all source tiles.

    All tiles in a UTM zone are combined into a single VRT with adaptive
    overviews targeting standard output resolutions above the coarsest
    source tile.  For multi-subdataset sources (S102V22, S102V30), one
    VRT is built per subdataset and then combined using ``-separate``.

    Parameters
    ----------
    project_dir
        The directory path to use. Will create if it does not currently exist.
        Required argument.
    data_source : str
        The NBS offers various products to different end-users. Some are available publicly.
        Use this argument to identify which product you want. BlueTopo is the default.
    relative_to_vrt : bool
        Use this argument to set paths of referenced files inside the VRT as relative or absolute paths.
    target_resolution : float | None
        When set, forces the output pixel size (in meters) for the UTM VRT
        instead of using the highest resolution from inputs.

    """
    project_dir = os.path.expanduser(project_dir)
    if os.path.isabs(project_dir) is False:
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    # Resolve data source config
    local_dir = None
    if data_source is None:
        data_source = "bluetopo"

    try:
        cfg = get_config(data_source)
        if cfg["geom_prefix"] is None and cfg["tile_prefix"] is None:
            raise ValueError(
                f"{data_source} is a local-only data source. "
                "Please provide a local directory path instead of the source name."
            )
        data_source = cfg["canonical_name"]
    except ValueError:
        if not os.path.isdir(data_source):
            raise
        local_dir = data_source
        files = os.listdir(local_dir)
        files = [f for f in files if f.endswith(".gpkg") and "Tile_Scheme" in f]
        files.sort(reverse=True)
        resolved_name = None
        for f in files:
            resolved_name = os.path.basename(f).split("_")[0]
            break
        if resolved_name is None:
            raise ValueError("Please pass in directory which contains a tile scheme file if you're using a local data source.")
        cfg = get_local_config(resolved_name)
        data_source = cfg["canonical_name"]

    if int(gdal.VersionInfo()) < cfg["min_gdal_version"]:
        min_ver = cfg["min_gdal_version"]
        raise RuntimeError(
            f"Please update GDAL to >={min_ver // 1000000}.{(min_ver % 1000000) // 10000} to run build_vrt. \n"
            "Some users have encountered issues with "
            "conda's installation of GDAL 3.4. "
            "Try more recent versions of GDAL if you also "
            "encounter issues in your conda environment."
        )

    missing_drivers = [d for d in cfg.get("required_gdal_drivers", [])
                       if gdal.GetDriverByName(d) is None]
    if missing_drivers:
        raise RuntimeError(
            f"GDAL is missing required driver(s) for {data_source}: "
            f"{', '.join(missing_drivers)}. "
            "Reinstall GDAL with HDF5 support to use this data source."
        )

    if not os.path.isdir(project_dir):
        raise ValueError(f"Folder path not found: {project_dir}")

    if not os.path.isfile(os.path.join(project_dir, f"{data_source.lower()}_registry.db")):
        raise ValueError("SQLite database not found. Confirm correct folder. "
                         "Note: fetch_tiles must be run at least once prior "
                         "to build_vrt")

    if not os.path.isdir(os.path.join(project_dir, data_source)):
        raise ValueError(f"Tile downloads folder not found for {data_source}. Confirm correct folder. "
                         "Note: fetch_tiles must be run at least once prior "
                         "to build_vrt")

    start = datetime.datetime.now()
    print(f"[{_timestamp()}] {data_source}: Beginning work in project folder: {project_dir}\n")

    conn = connect_to_survey_registry(project_dir, cfg)

    # Check for missing UTM VRT files
    missing_utm_count = missing_utms(project_dir, conn, cfg)
    if missing_utm_count:
        print(f"{missing_utm_count} utm vrts files missing. Added to build list.")

    # Ensure VRT output directory exists
    vrt_dir = os.path.join(project_dir, f"{data_source}_VRT")
    if not os.path.exists(vrt_dir):
        os.makedirs(vrt_dir)

    # Build UTM VRTs directly from source tiles
    unbuilt_utms = select_unbuilt_utms(conn, cfg)
    if len(unbuilt_utms) > 0:
        print(f"Building {len(unbuilt_utms)} utm vrt(s). This may take minutes "
              "or hours depending on the amount of tiles.")
        for ub_utm in unbuilt_utms:
            utm_start = datetime.datetime.now()
            tiles = select_tiles_by_utm(project_dir, conn, ub_utm["utm"], cfg)
            if len(tiles) < 1:
                continue

            print(f"Building utm{ub_utm['utm']} from {len(tiles)} source tile(s)...")

            if cfg["subdatasets"]:
                # Build per-subdataset UTM VRTs from source tiles
                sd_vrt_paths = []
                fields = {"utm": ub_utm["utm"]}
                for sd_idx, sd in enumerate(cfg["subdatasets"]):
                    suffix_label = f"_subdataset{sd_idx + 1}"
                    tile_paths = _build_tile_paths(tiles, project_dir, cfg, sd)
                    if len(tile_paths) < 1:
                        continue
                    factors = compute_overview_factors(tile_paths, target_resolution)
                    rel_path = os.path.join(f"{data_source}_VRT",
                                            f"{data_source}_Fetched_UTM{ub_utm['utm']}{sd['suffix']}.vrt")
                    utm_sd_vrt = os.path.join(project_dir, rel_path)
                    create_vrt(tile_paths, utm_sd_vrt, factors or None, relative_to_vrt,
                               sd["band_descriptions"], target_resolution=target_resolution)
                    sd_vrt_paths.append(utm_sd_vrt)
                    fields[f"utm{suffix_label}_vrt"] = rel_path
                    fields[f"utm{suffix_label}_ovr"] = None
                    if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                        fields[f"utm{suffix_label}_ovr"] = rel_path + ".ovr"
                    elif factors:
                        raise RuntimeError(
                            f"Overview failed to create for utm{ub_utm['utm']}. "
                            "Please try again. If error persists, please contact NBS."
                        )

                # Build combined VRT
                rel_combined = os.path.join(f"{data_source}_VRT",
                                            f"{data_source}_Fetched_UTM{ub_utm['utm']}.vrt")
                utm_combined_vrt = os.path.join(project_dir, rel_combined)
                combined_bands = []
                for sd in cfg["subdatasets"]:
                    combined_bands.extend(sd["band_descriptions"])
                create_vrt(sd_vrt_paths, utm_combined_vrt, None, relative_to_vrt,
                           combined_bands, separate=True)
                fields["utm_combined_vrt"] = rel_combined

                if cfg["has_rat"]:
                    add_vrt_rat(conn, ub_utm["utm"], project_dir, utm_combined_vrt, cfg)

                update_utm(conn, fields, cfg)
            else:
                # Single dataset UTM VRT from source tiles
                tile_paths = _build_tile_paths(tiles, project_dir, cfg)
                if len(tile_paths) < 1:
                    continue
                factors = compute_overview_factors(tile_paths, target_resolution)
                rel_path = os.path.join(f"{data_source}_VRT",
                                        f"{data_source}_Fetched_UTM{ub_utm['utm']}.vrt")
                utm_vrt = os.path.join(project_dir, rel_path)
                create_vrt(tile_paths, utm_vrt, factors or None, relative_to_vrt,
                           cfg["band_descriptions"], target_resolution=target_resolution)

                if cfg["has_rat"]:
                    add_vrt_rat(conn, ub_utm["utm"], project_dir, utm_vrt, cfg)

                fields = {"utm_vrt": rel_path, "utm_ovr": None, "utm": ub_utm["utm"]}
                if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                    fields["utm_ovr"] = rel_path + ".ovr"
                elif factors:
                    raise RuntimeError(
                        f"Overview failed to create for utm{ub_utm['utm']}. "
                        "Please try again. If error persists, please contact NBS."
                    )
                update_utm(conn, fields, cfg)
            print(f"utm{ub_utm['utm']} complete after {datetime.datetime.now() - utm_start}")
    else:
        print("UTM vrt(s) appear up to date with the most recently "
              f"fetched tiles.\nNote: deleting the {data_source}_VRT folder will "
              "allow you to recreate from scratch if necessary")

    print(f"[{_timestamp()}] {data_source}: Operation complete after {datetime.datetime.now() - start}")
