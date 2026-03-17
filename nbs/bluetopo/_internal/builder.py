"""
build_vrt.py - Orchestrate GDAL Virtual Raster creation from downloaded tiles.

Thin orchestrator that coordinates:
1. Data source resolution
2. UTM zone discovery and missing VRT detection
3. Per-UTM VRT creation with adaptive overviews
4. RAT aggregation for sources that support it
"""

import datetime
import os
import platform
from dataclasses import dataclass, field

from osgeo import gdal

from nbs.bluetopo._internal.config import (
    VALID_TARGET_RESOLUTIONS,
    _timestamp,
    resolve_data_source,
)
from nbs.bluetopo._internal.db import connect
from nbs.bluetopo._internal.vrt import (
    add_vrt_rat,
    build_tile_paths,
    compute_overview_factors,
    create_vrt,
    missing_utms,
    select_tiles_by_utm,
    select_unbuilt_utms,
    update_utm,
)


@dataclass
class BuildResult:
    """Result of a build_vrt operation.

    Attributes
    ----------
    built : list[dict]
        UTM zones that were built. Each dict has ``utm`` and ``vrt`` keys,
        plus ``ovr`` (str or None) for the overview file path.
    skipped : list[str]
        UTM zones that were already up to date.
    missing_reset : int
        Number of UTM zones that were reset due to missing VRT files on disk.
    """
    built: list = field(default_factory=list)
    skipped: list = field(default_factory=list)
    missing_reset: int = 0


def build_vrt(project_dir: str, data_source: str = None,
              relative_to_vrt: bool = True,
              target_resolution: float = None,
              debug: bool = False) -> BuildResult:
    """Build a flat GDAL VRT per UTM zone from all source tiles.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    data_source : str | None
        A known source name, a local directory path, or None (defaults to ``"bluetopo"``).
    relative_to_vrt : bool
        Store referenced file paths as relative to the VRT's directory.
    target_resolution : float | None
        Force output pixel size (in meters).  Must be in {2, 4, 8, 16, 32, 64}.
    debug : bool
        If True, writes a diagnostic report to the project directory.

    Returns
    -------
    BuildResult
        Structured result with built, skipped, and missing_reset counts.
    """
    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    cfg, _ = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]

    if int(gdal.VersionInfo()) < cfg["min_gdal_version"]:
        min_ver = cfg["min_gdal_version"]
        raise RuntimeError(
            f"Please update GDAL to >={min_ver // 1000000}.{(min_ver % 1000000) // 10000} "
            "to run build_vrt.\nSome users have encountered issues with "
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
                         "Note: fetch_tiles must be run at least once prior to build_vrt")

    if not os.path.isdir(os.path.join(project_dir, data_source)):
        raise ValueError(f"Tile downloads folder not found for {data_source}. "
                         "Confirm correct folder. "
                         "Note: fetch_tiles must be run at least once prior to build_vrt")

    if target_resolution is not None and target_resolution not in VALID_TARGET_RESOLUTIONS:
        raise ValueError(
            f"target_resolution must be one of {sorted(VALID_TARGET_RESOLUTIONS)}, "
            f"got {target_resolution}"
        )

    report = None
    if debug:
        from nbs.bluetopo._internal.diagnostics import DebugReport
        report = DebugReport(project_dir, data_source, cfg)

    result = BuildResult()
    try:
        result = _run_build(project_dir, cfg, data_source, relative_to_vrt,
                            target_resolution, result)
    except Exception:
        if report:
            report.capture_exception()
        raise
    finally:
        if report:
            report.add_result(result)
            report.write()
    return result


def _run_build(project_dir, cfg, data_source, relative_to_vrt,
               target_resolution, result):
    """Core build pipeline. Separated to allow debug wrapper."""
    start = datetime.datetime.now()
    print(f"[{_timestamp()}] {data_source}: Beginning work in project folder: {project_dir}\n")

    conn = connect(project_dir, cfg)
    try:
        result.missing_reset = missing_utms(project_dir, conn, cfg)
        if result.missing_reset:
            print(f"{result.missing_reset} utm vrts files missing. Added to build list.")

        vrt_dir = os.path.join(project_dir, f"{data_source}_VRT")
        os.makedirs(vrt_dir, exist_ok=True)

        unbuilt_utms = select_unbuilt_utms(conn, cfg)
        if unbuilt_utms:
            print(f"Building {len(unbuilt_utms)} utm vrt(s). This may take minutes "
                  "or hours depending on the amount of tiles.")
            for ub_utm in unbuilt_utms:
                utm_start = datetime.datetime.now()
                tiles = select_tiles_by_utm(project_dir, conn, ub_utm["utm"], cfg)
                if not tiles:
                    continue

                print(f"Building utm{ub_utm['utm']} from {len(tiles)} source tile(s)...")
                built_entry = {"utm": ub_utm["utm"]}

                if cfg["subdatasets"]:
                    sd_vrt_paths = []
                    fields = {"utm": ub_utm["utm"]}
                    for sd_idx, sd in enumerate(cfg["subdatasets"]):
                        suffix_label = f"_subdataset{sd_idx + 1}"
                        tile_paths = build_tile_paths(tiles, project_dir, cfg, sd)
                        if not tile_paths:
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
                                "Please try again. If error persists, please contact NBS.")

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
                    built_entry["vrt"] = os.path.join(project_dir, rel_combined)
                    built_entry["ovr"] = None
                else:
                    tile_paths = build_tile_paths(tiles, project_dir, cfg)
                    if not tile_paths:
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
                    built_entry["ovr"] = None
                    ovr_path = os.path.join(project_dir, rel_path + ".ovr")
                    if os.path.isfile(ovr_path):
                        fields["utm_ovr"] = rel_path + ".ovr"
                        built_entry["ovr"] = ovr_path
                    elif factors:
                        raise RuntimeError(
                            f"Overview failed to create for utm{ub_utm['utm']}. "
                            "Please try again. If error persists, please contact NBS.")
                    update_utm(conn, fields, cfg)
                    built_entry["vrt"] = utm_vrt

                result.built.append(built_entry)
                print(f"utm{ub_utm['utm']} complete after {datetime.datetime.now() - utm_start}")
            # Track skipped UTMs (already built, not in unbuilt list)
            all_utms_cursor = conn.cursor()
            all_utms_cursor.execute("SELECT utm FROM vrt_utm")
            all_utm_names = {row["utm"] for row in all_utms_cursor.fetchall()}
            built_utm_names = {e["utm"] for e in result.built}
            result.skipped = sorted(all_utm_names - built_utm_names)
        else:
            print("UTM vrt(s) appear up to date with the most recently "
                  f"fetched tiles.\nNote: deleting the {data_source}_VRT folder will "
                  "allow you to recreate from scratch if necessary")

        print(f"[{_timestamp()}] {data_source}: Operation complete after {datetime.datetime.now() - start}")
    finally:
        conn.close()
    return result
