"""
build_vrt.py - Build GDAL Virtual Rasters from downloaded NBS tiles.

Creates a multi-level VRT hierarchy for each data source:

1. Per-resolution subregion VRTs (2m, 4m, 8m) with overviews
2. Complete subregion VRT combining all resolutions + 16m tiles
3. UTM-zone VRTs combining all subregions, with a Raster Attribute Table

For multi-subdataset sources (S102V22, S102V30), the hierarchy is built
once per subdataset (BathymetryCoverage, QualityOfSurvey /
QualityOfBathymetryCoverage) and then combined into a single UTM VRT
using the ``-separate`` flag.

State is tracked in a SQLite registry DB (``<source>_registry.db``) with
``built`` flags on both ``vrt_subregion`` and ``vrt_utm`` tables.  Only
unbuilt records are processed on each run, making the operation resumable.
"""

import collections
import copy
import datetime
import os
import platform
import shutil
import sqlite3

from osgeo import gdal

from nbs.bluetopo.core.datasource import (
    _timestamp,
    get_config,
    get_local_config,
    get_catalog_fields,
    get_vrt_subregion_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_built_flags,
    get_vrt_file_columns,
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

    The registry contains four tables whose schemas are driven by *cfg*:

    - **catalog** (or **tileset**) -- tracks downloaded tessellation and XML files.
    - **tiles** -- one row per tile with links, disk paths, checksums, and verified flags.
    - **vrt_subregion** -- VRT/OVR paths and built flags per subregion.
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
    vrt_subregion_fields = get_vrt_subregion_fields(cfg)
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
                CREATE TABLE IF NOT EXISTS vrt_subregion (
                region text PRIMARY KEY
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
                ("vrt_subregion", vrt_subregion_fields),
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


def build_sub_vrts(subregion: dict, subregion_tiles: list, project_dir: str,
                   cfg: dict, relative_to_vrt: bool,
                   target_resolution: float = None) -> dict:
    """
    Build all per-resolution and complete VRTs for a single subregion.

    Tiles are grouped by resolution.  For each resolution a VRT with
    overviews is created.  A "complete" VRT then combines all resolution
    VRTs plus any raw 16m tiles.

    Resolution keys are sorted coarse-to-fine (16m → 8m → 4m → 2m) so that
    higher-resolution data takes priority in GDAL BuildVRT overlap areas
    (GDAL gives priority to later files).

    For multi-subdataset sources (e.g. S102V22, S102V30), this process runs
    once per subdataset, producing separate VRT files named with the
    subdataset suffix (e.g. ``_BathymetryCoverage.vrt``,
    ``_QualityOfSurvey.vrt`` / ``_QualityOfBathymetryCoverage.vrt``).
    Subdatasets using the S102 driver protocol have their file paths
    wrapped as ``S102:"path":SubdatasetName``.

    Parameters
    ----------
    subregion : dict
        Subregion record from ``vrt_subregion`` (must have ``"region"`` key).
    subregion_tiles : list[dict]
        Tile records belonging to this subregion (must have ``"resolution"``
        and the appropriate disk-path field).
    project_dir : str
        Absolute path to the project directory.
    cfg : dict
        Data source configuration from ``datasource.get_config()``.
    relative_to_vrt : bool
        If True, file paths inside VRTs are stored relative to VRT location.
    target_resolution : float | None
        When set, forces the output pixel size for the complete VRT.
        Per-resolution VRTs are unaffected.

    Returns
    -------
    fields : dict
        Keys are ``"region"`` plus all VRT/OVR column names from the
        ``vrt_subregion`` schema, with values set to relative paths (or None
        if the file was not created for that resolution).
    """
    data_source = cfg["canonical_name"]
    subdatasets = cfg["subdatasets"]
    disk_field = get_disk_field(cfg)

    # Initialize fields dict
    fields = {"region": subregion["region"]}
    subregion_field_cols = get_vrt_file_columns(cfg)
    for col in subregion_field_cols:
        fields[col] = None

    rel_dir = os.path.join(f"{data_source}_VRT", subregion["region"])
    subregion_dir = os.path.join(project_dir, rel_dir)
    try:
        if os.path.isdir(subregion_dir):
            shutil.rmtree(subregion_dir)
    except (OSError, PermissionError) as e:
        raise OSError(f"Failed to remove older vrt files for {subregion['region']}\n"
                      "Please close all files and attempt again") from e
    if not os.path.exists(subregion_dir):
        os.makedirs(subregion_dir)

    resolution_tiles = collections.defaultdict(list)
    for tile in subregion_tiles:
        resolution_tiles[tile["resolution"]].append(tile)

    def _res_sort_key(res_str):
        """Extract numeric value from resolution string for sorting."""
        return int(''.join(c for c in res_str if c.isdigit()))

    # Sort coarse-to-fine (descending numeric) so higher-res data is added
    # last and takes priority in GDAL BuildVRT overlap areas.
    sorted_resolutions = sorted(resolution_tiles.keys(),
                                key=_res_sort_key, reverse=True)

    if subdatasets:
        # Multiple subdatasets (e.g. S102V22/V30 BathymetryCoverage + quality coverage)
        vrt_lists = {i: [] for i in range(len(subdatasets))}
        for res in sorted_resolutions:
            tiles = resolution_tiles[res]
            print(f"Building {subregion['region']} band {res}...")
            for sd_idx, sd in enumerate(subdatasets):
                suffix_label = f"_subdataset{sd_idx + 1}"
                rel_path = os.path.join(rel_dir, subregion["region"] + f"_{res}{sd['suffix']}.vrt")
                res_vrt = os.path.join(project_dir, rel_path)
                if sd.get("s102_protocol"):
                    tiffs = []
                    for tile in tiles:
                        fpath = os.path.join(project_dir, tile[disk_field]).replace("\\", "/")
                        if fpath.startswith('/') and not fpath.startswith('//'):
                            tiffs.append(f'S102:"/{fpath}":{sd["name"]}')
                        else:
                            tiffs.append(f'S102:"{fpath}":{sd["name"]}')
                else:
                    tiffs = [os.path.join(project_dir, tile[disk_field]) for tile in tiles]

                if "2" in res:
                    create_vrt(tiffs, res_vrt, [2, 4], relative_to_vrt, sd["band_descriptions"])
                    vrt_lists[sd_idx].append(res_vrt)
                    fields[f"res_2{suffix_label}_vrt"] = rel_path
                    if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                        fields[f"res_2{suffix_label}_ovr"] = rel_path + ".ovr"
                if "4" in res:
                    create_vrt(tiffs, res_vrt, [4, 8], relative_to_vrt, sd["band_descriptions"])
                    vrt_lists[sd_idx].append(res_vrt)
                    fields[f"res_4{suffix_label}_vrt"] = rel_path
                    if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                        fields[f"res_4{suffix_label}_ovr"] = rel_path + ".ovr"
                if "8" in res:
                    create_vrt(tiffs, res_vrt, [8], relative_to_vrt, sd["band_descriptions"])
                    vrt_lists[sd_idx].append(res_vrt)
                    fields[f"res_8{suffix_label}_vrt"] = rel_path
                    if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                        fields[f"res_8{suffix_label}_ovr"] = rel_path + ".ovr"
                if "16" in res:
                    vrt_lists[sd_idx].extend(tiffs)

        # Build complete VRTs per subdataset
        for sd_idx, sd in enumerate(subdatasets):
            suffix_label = f"_subdataset{sd_idx + 1}"
            rel_path = os.path.join(rel_dir, subregion["region"] + f"_complete{sd['suffix']}.vrt")
            complete_vrt = os.path.join(project_dir, rel_path)
            create_vrt(vrt_lists[sd_idx], complete_vrt, [16], relative_to_vrt, sd["band_descriptions"],
                       target_resolution=target_resolution)
            fields[f"complete{suffix_label}_vrt"] = rel_path
            if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                fields[f"complete{suffix_label}_ovr"] = rel_path + ".ovr"
    else:
        # Single dataset (BlueTopo, Modeling, BAG, S102V21)
        band_descs = cfg["band_descriptions"]
        vrt_list = []
        for res in sorted_resolutions:
            tiles = resolution_tiles[res]
            print(f"Building {subregion['region']} band {res}...")
            rel_path = os.path.join(rel_dir, subregion["region"] + f"_{res}.vrt")
            res_vrt = os.path.join(project_dir, rel_path)
            tiffs = [os.path.join(project_dir, tile[disk_field]) for tile in tiles]
            if "2" in res:
                create_vrt(tiffs, res_vrt, [2, 4], relative_to_vrt, band_descs)
                vrt_list.append(res_vrt)
                fields["res_2_vrt"] = rel_path
                if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                    fields["res_2_ovr"] = rel_path + ".ovr"
            if "4" in res:
                create_vrt(tiffs, res_vrt, [4, 8], relative_to_vrt, band_descs)
                vrt_list.append(res_vrt)
                fields["res_4_vrt"] = rel_path
                if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                    fields["res_4_ovr"] = rel_path + ".ovr"
            if "8" in res:
                create_vrt(tiffs, res_vrt, [8], relative_to_vrt, band_descs)
                vrt_list.append(res_vrt)
                fields["res_8_vrt"] = rel_path
                if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                    fields["res_8_ovr"] = rel_path + ".ovr"
            if "16" in res:
                vrt_list.extend(tiffs)
        rel_path = os.path.join(rel_dir, subregion["region"] + "_complete.vrt")
        complete_vrt = os.path.join(project_dir, rel_path)
        create_vrt(vrt_list, complete_vrt, [16], relative_to_vrt, band_descs,
                   target_resolution=target_resolution)
        fields["complete_vrt"] = rel_path
        if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
            fields["complete_ovr"] = rel_path + ".ovr"
    return fields


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
    rat_zero_fields = cfg.get("rat_zero_fields", [])

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
            for mapped_col in col_map:
                entry_val = rat_n.GetValueAsString(row, mapped_col)
                if rat_n.GetNameOfCol(mapped_col).lower() in rat_zero_fields:
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


def select_tiles_by_subregion(project_dir: str, conn: sqlite3.Connection,
                              subregion: str, cfg: dict) -> list:
    """
    Return tiles in *subregion* whose files exist on disk.

    Tiles registered in the DB but whose files are missing are excluded and
    a warning is printed advising the user to re-run ``fetch_tiles``.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    conn : sqlite3.Connection
        Database connection.
    subregion : str
        Subregion name to filter by.
    cfg : dict
        Data source configuration.

    Returns
    -------
    list[dict]
        Tile records whose disk files were confirmed to exist.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tiles WHERE subregion = ?", (subregion,))
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
              f"registered tile(s) in subregion {subregion}. "
              "Run fetch_tiles to retrieve files "
              "or correct the directory path if incorrect.")
    return existing_tiles


def select_subregions_by_utm(project_dir: str, conn: sqlite3.Connection,
                             utm: str, cfg: dict) -> list:
    """
    Return fully-built subregion records in a UTM zone, validating VRT files.

    Only subregions with all ``built`` flags set to 1 are returned.  Each
    returned record is checked to ensure its complete VRT files exist on
    disk; a ``RuntimeError`` is raised if any are missing.

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
        Built subregion records in the given UTM zone.

    Raises
    ------
    RuntimeError
        If any expected VRT/OVR file is missing from disk.
    """
    built_flags = get_built_flags(cfg)
    where_built = " AND ".join(f"{f} = 1" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM vrt_subregion WHERE utm = ? AND {where_built}",
        (utm,),
    )
    subregions = [dict(row) for row in cursor.fetchall()]
    vrt_cols = get_vrt_file_columns(cfg)
    for s in subregions:
        for col in vrt_cols:
            if "complete" in col:
                if s[col] is None or not os.path.isfile(os.path.join(project_dir, s[col])):
                    raise RuntimeError(f"Subregion VRT files missing for {s['utm']}. Please rerun.")
            else:
                if s[col] and not os.path.isfile(os.path.join(project_dir, s[col])):
                    raise RuntimeError(f"Subregion VRT files missing for {s['utm']}. Please rerun.")
    return subregions


def select_unbuilt_subregions(conn: sqlite3.Connection, cfg: dict) -> list:
    """
    Retrieve all unbuilt subregion records.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    cfg : dict
        data source configuration.

    Returns
    -------
    subregions : list
        list of unbuilt subregion records.
    """
    built_flags = get_built_flags(cfg)
    where_clause = " or ".join(f"{f} = 0" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM vrt_subregion WHERE {where_clause}")
    return [dict(row) for row in cursor.fetchall()]


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


def update_subregion(conn: sqlite3.Connection, fields: dict, cfg: dict) -> None:
    """
    Update subregion records with given path values.

    Parameters
    ----------
    conn : sqlite3.Connection
        database connection object.
    fields : dict
        dictionary with the name of the subregion and paths for its associated
        VRT and OVR files.
    cfg : dict
        data source configuration.
    """
    vrt_cols = get_vrt_file_columns(cfg)
    built_flags = get_built_flags(cfg)
    set_parts = [f"{col} = ?" for col in vrt_cols]
    set_parts.extend(f"{f} = 1" for f in built_flags)
    set_clause = ", ".join(set_parts)
    values = [fields.get(col) for col in vrt_cols]
    values.append(fields["region"])
    cursor = conn.cursor()
    cursor.execute(
        f"UPDATE vrt_subregion SET {set_clause} WHERE region = ?",
        values,
    )
    conn.commit()


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


def missing_subregions(project_dir: str, conn: sqlite3.Connection, cfg: dict) -> int:
    """
    Reset subregions whose VRT/OVR files are missing from disk.

    Scans all subregions marked as built.  If any expected VRT or OVR file
    is missing, the subregion's paths are cleared, its built flags are set
    to 0, and the parent UTM record is also reset so both will be rebuilt
    on the next run.

    Complete VRTs are required to exist (None is treated as missing); other
    resolution VRTs are only checked when non-None (a subregion may not have
    data at every resolution).

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
        Number of subregions that were reset.
    """
    built_flags = get_built_flags(cfg)
    where_built = " or ".join(f"{f} = 1" for f in built_flags)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM vrt_subregion WHERE {where_built}")
    subregions = [dict(row) for row in cursor.fetchall()]
    missing_subregion_count = 0
    vrt_cols = get_vrt_file_columns(cfg)
    utm_cols = get_utm_file_columns(cfg)

    for s in subregions:
        missing = False
        for col in vrt_cols:
            if "complete" in col:
                if s[col] is None or not os.path.isfile(os.path.join(project_dir, s[col])):
                    missing = True
                    break
            else:
                if s[col] and not os.path.isfile(os.path.join(project_dir, s[col])):
                    missing = True
                    break
        if missing:
            missing_subregion_count += 1
            # Reset subregion
            set_parts = [f"{col} = ?" for col in vrt_cols]
            for f in built_flags:
                set_parts.append(f"{f} = 0")
            set_clause = ", ".join(set_parts)
            values = [None] * len(vrt_cols) + [s["region"]]
            cursor.execute(
                f"UPDATE vrt_subregion SET {set_clause} WHERE region = ?",
                values,
            )
            # Reset utm
            utm_set_parts = [f"{col} = ?" for col in utm_cols]
            if cfg["subdatasets"]:
                for f in built_flags:
                    utm_set_parts.append(f"{f} = 0")
                utm_set_parts.append("built_combined = 0")
            else:
                for f in built_flags:
                    utm_set_parts.append(f"{f} = 0")
            utm_set_clause = ", ".join(utm_set_parts)
            utm_values = [None] * len(utm_cols) + [s["utm"]]
            cursor.execute(
                f"UPDATE vrt_utm SET {utm_set_clause} WHERE utm = ?",
                utm_values,
            )
            conn.commit()
    return missing_subregion_count


def missing_utms(project_dir: str, conn: sqlite3.Connection, cfg: dict) -> int:
    """
    Reset UTM zones whose VRT/OVR files are missing from disk.

    Same logic as :func:`missing_subregions` but for the ``vrt_utm`` table.

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
    Build a gdal VRT for all available tiles.
    This VRT is a collection of smaller areas described as VRTs.
    Nominally 2 meter, 4 meter, and 8 meter data are collected with overviews.
    These data are then added to 16 meter data for the subregion.
    The subregions are then collected into a UTM zone VRT where higher level
    overviews are made.

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
        When set, forces the output pixel size (in meters) for complete and
        UTM VRTs instead of using the highest resolution from inputs.
        Per-resolution VRTs are unaffected.

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

    # Subregions missing files
    missing_subregion_count = missing_subregions(project_dir, conn, cfg)
    if missing_subregion_count:
        print(f"{missing_subregion_count} subregion vrts files missing. Added to build list.")

    # Build subregion VRTs
    unbuilt_subregions = select_unbuilt_subregions(conn, cfg)
    if len(unbuilt_subregions) > 0:
        print(f"Building {len(unbuilt_subregions)} subregion vrt(s). This may "
              "take minutes or hours depending on the amount of tiles.")
        for ub_sr in unbuilt_subregions:
            sr_tiles = select_tiles_by_subregion(project_dir, conn, ub_sr["region"], cfg)
            if len(sr_tiles) < 1:
                continue
            fields = build_sub_vrts(ub_sr, sr_tiles, project_dir, cfg, relative_to_vrt,
                                    target_resolution=target_resolution)
            update_subregion(conn, fields, cfg)
    else:
        print("Subregion vrt(s) appear up to date with the most recently fetched tiles.")

    # UTMs missing files
    missing_utm_count = missing_utms(project_dir, conn, cfg)
    if missing_utm_count:
        print(f"{missing_utm_count} utm vrts files missing. Added to build list.")

    # Build UTM VRTs
    unbuilt_utms = select_unbuilt_utms(conn, cfg)
    if len(unbuilt_utms) > 0:
        print(f"Building {len(unbuilt_utms)} utm vrt(s). This may take minutes "
              "or hours depending on the amount of tiles.")
        for ub_utm in unbuilt_utms:
            utm_start = datetime.datetime.now()
            subregions = select_subregions_by_utm(project_dir, conn, ub_utm["utm"], cfg)

            if cfg["subdatasets"]:
                # Build per-subdataset UTM VRTs
                sd_vrt_paths = []
                sd_rel_paths = []
                fields = {"utm": ub_utm["utm"]}
                for sd_idx, sd in enumerate(cfg["subdatasets"]):
                    suffix_label = f"_subdataset{sd_idx + 1}"
                    vrt_list = [os.path.join(project_dir, s[f"complete{suffix_label}_vrt"]) for s in subregions]
                    if len(vrt_list) < 1:
                        continue
                    rel_path = os.path.join(f"{data_source}_VRT",
                                            f"{data_source}_Fetched_UTM{ub_utm['utm']}{sd['suffix']}.vrt")
                    utm_sd_vrt = os.path.join(project_dir, rel_path)
                    print(f"Building utm{ub_utm['utm']}...")
                    create_vrt(vrt_list, utm_sd_vrt, [32, 64], relative_to_vrt, sd["band_descriptions"],
                              target_resolution=target_resolution)
                    sd_vrt_paths.append(utm_sd_vrt)
                    sd_rel_paths.append(rel_path)
                    fields[f"utm{suffix_label}_vrt"] = rel_path
                    fields[f"utm{suffix_label}_ovr"] = None
                    if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                        fields[f"utm{suffix_label}_ovr"] = rel_path + ".ovr"
                    else:
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
                # Single dataset UTM VRT
                vrt_list = [os.path.join(project_dir, s["complete_vrt"]) for s in subregions]
                if len(vrt_list) < 1:
                    continue
                rel_path = os.path.join(f"{data_source}_VRT",
                                        f"{data_source}_Fetched_UTM{ub_utm['utm']}.vrt")
                utm_vrt = os.path.join(project_dir, rel_path)
                print(f"Building utm{ub_utm['utm']}...")
                create_vrt(vrt_list, utm_vrt, [32, 64], relative_to_vrt, cfg["band_descriptions"],
                          target_resolution=target_resolution)

                if cfg["has_rat"]:
                    add_vrt_rat(conn, ub_utm["utm"], project_dir, utm_vrt, cfg)

                fields = {"utm_vrt": rel_path, "utm_ovr": None, "utm": ub_utm["utm"]}
                if os.path.isfile(os.path.join(project_dir, rel_path + ".ovr")):
                    fields["utm_ovr"] = rel_path + ".ovr"
                else:
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
