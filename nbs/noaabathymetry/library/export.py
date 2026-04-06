"""Project export (zip) with integrity verification."""

import datetime
import json
import logging
import os
import zipfile
from dataclasses import dataclass

from nbs.noaabathymetry._internal.config import (
    get_utm_file_columns,
    get_verified_fields,
    resolve_data_source,
)
from nbs.noaabathymetry._internal.db import connect
from nbs.noaabathymetry._internal.download import all_db_tiles
from nbs.noaabathymetry.library.verify import generate_manifest, verify_tiles

logger = logging.getLogger("noaabathymetry")


@dataclass
class ExportResult:
    """Result of an export_project operation.

    Attributes
    ----------
    output_path : str
        Absolute path to the created zip file.
    tile_count : int
        Number of tiles in the project.
    file_count : int
        Total number of files in the zip.
    zip_size : int
        Size of the zip file in bytes.
    mosaics_included : bool
        Whether mosaic files were included.
    verification_passed : bool
        Whether all tile checksums matched.
    """
    output_path: str
    tile_count: int
    file_count: int
    zip_size: int
    mosaics_included: bool
    verification_passed: bool


def export_project(project_dir, output_path, data_source=None,
                   include_mosaics=True, flag_for_repair=False):
    """Export a project as a portable zip file.

    Verifies tile integrity (checksums), generates a manifest, and
    creates a zip containing all project files.  The zip is written
    to a temporary file and renamed on success — a partial zip is
    never visible to the user.

    Parameters
    ----------
    project_dir : str
        Absolute path to the project directory.
    output_path : str
        Absolute path for the output zip file.
    data_source : str | None
        Data source name.  Defaults to ``"bluetopo"``.
    include_mosaics : bool
        Include mosaic VRTs, OVRs, and hillshades.  Errors if the
        data source is S102V22 or S102V30 (non-portable VRTs).
    flag_for_repair : bool
        When True and verification finds checksum mismatches, reset
        their verified flags so the next ``fetch_tiles`` re-downloads
        them.  The export still does not proceed (files are corrupted),
        but the tiles are flagged for repair.

    Returns
    -------
    ExportResult
        Summary of the export.

    Raises
    ------
    ValueError
        If pre-flight checks fail (missing DB, unverified tiles,
        missing files, checksum mismatches, non-portable mosaics).
    """
    import platform

    project_dir = os.path.expanduser(project_dir)
    if not os.path.isabs(project_dir):
        msg = "Please use an absolute path for your project folder."
        if "windows" not in platform.system().lower():
            msg += "\nTypically for non windows systems this means starting with '/'"
        raise ValueError(msg)

    cfg, _ = resolve_data_source(data_source)
    data_source = cfg["canonical_name"]

    # --- Instant checks (no I/O or minimal I/O) ---

    if not os.path.isdir(project_dir):
        raise ValueError(f"Project directory not found: {project_dir}")

    db_name = f"{data_source.lower()}_registry.db"
    if not os.path.isfile(os.path.join(project_dir, db_name)):
        raise ValueError(
            f"Registry database not found ({db_name}). "
            "Note: fetch must be run at least once.")

    tmp_path = output_path + ".tmp"

    if include_mosaics and cfg.get("subdatasets"):
        for sd in cfg["subdatasets"]:
            if sd.get("s102_protocol"):
                raise ValueError(
                    f"Cannot export mosaics for {data_source}: "
                    "QualityOfBathymetryCoverage VRTs contain non-portable "
                    "absolute paths (GDAL limitation). Export without "
                    "mosaics (include_mosaics=False) and rebuild on the "
                    "destination machine.")

    export_start = datetime.datetime.now()
    logger.info("═══ Export ═══")
    logger.info("Project: %s", project_dir)
    logger.info("Data source: %s", data_source)
    logger.info("")

    # --- Step 1: Check file existence (scandir-based, fast) ---
    logger.info("Step 1/4: Checking project files...")

    # Scan all relevant directories once
    conn = connect(project_dir, cfg)
    try:
        tiles = all_db_tiles(conn)
        disk_fields = list({f"{s['name']}_disk" for s in cfg["file_slots"]})

        # Collect directories to scan
        dirs_to_scan = set()
        for tile in tiles:
            for df in disk_fields:
                path = tile.get(df)
                if path:
                    dirs_to_scan.add(os.path.dirname(path))

        if include_mosaics:
            utm_cols = get_utm_file_columns(cfg)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM mosaic_utm")
            mosaic_rows = [dict(row) for row in cursor.fetchall()]
            for utm_row in mosaic_rows:
                for col in utm_cols:
                    if utm_row.get(col):
                        dirs_to_scan.add(os.path.dirname(utm_row[col]))
                if utm_row.get("hillshade"):
                    dirs_to_scan.add(os.path.dirname(utm_row["hillshade"]))
    finally:
        conn.close()

    # Scan directories
    existing_files = set()
    for rel_dir in dirs_to_scan:
        abs_dir = os.path.join(project_dir, rel_dir) if rel_dir else project_dir
        try:
            with os.scandir(abs_dir) as entries:
                for e in entries:
                    if e.is_file(follow_symlinks=False):
                        existing_files.add(
                            os.path.join(rel_dir, e.name) if rel_dir else e.name)
        except FileNotFoundError:
            pass

    # Check tile files
    missing_tiles = []
    for tile in tiles:
        for df in disk_fields:
            path = tile.get(df)
            if path and path not in existing_files:
                missing_tiles.append(path)

    # Check mosaic files
    missing_mosaics = []
    if include_mosaics:
        for utm_row in mosaic_rows:
            for col in utm_cols:
                path = utm_row.get(col)
                if path and path not in existing_files:
                    missing_mosaics.append(path)
            hs_path = utm_row.get("hillshade")
            if hs_path and hs_path not in existing_files:
                missing_mosaics.append(hs_path)

    errors = []
    if missing_tiles:
        errors.append(
            f"{len(missing_tiles)} tile file(s) missing from disk: "
            f"{missing_tiles[:5]}")
    if missing_mosaics:
        errors.append(
            f"{len(missing_mosaics)} mosaic file(s) missing from disk: "
            f"{missing_mosaics[:5]}")
    if errors:
        raise ValueError(
            "Pre-flight check failed:\n" +
            "\n".join(f"  - {e}" for e in errors))

    logger.info("All files present.")
    logger.info("")

    # --- Step 2: Verify tile checksums (expensive — re-hashes all files) ---
    logger.info("Step 2/4: Verifying tile checksums...")
    verify_result = verify_tiles(project_dir, data_source)

    errors = []
    if verify_result.unverified:
        errors.append(
            f"{len(verify_result.unverified)} tile(s) not verified: "
            f"{verify_result.unverified[:5]}")
    if verify_result.missing_files:
        names = [e["tilename"] for e in verify_result.missing_files]
        errors.append(
            f"{len(verify_result.missing_files)} tile(s) missing files: "
            f"{names[:5]}")
    if verify_result.checksum_mismatch:
        names = [e["tilename"] for e in verify_result.checksum_mismatch]
        errors.append(
            f"{len(verify_result.checksum_mismatch)} tile(s) with "
            f"checksum mismatch: {names[:5]}")

    if errors:
        if flag_for_repair and verify_result.checksum_mismatch:
            conn = connect(project_dir, cfg)
            try:
                cursor = conn.cursor()
                verified_fields = get_verified_fields(cfg)
                set_clause = ", ".join(f"{vf} = 0" for vf in verified_fields)
                tilenames = [e["tilename"]
                             for e in verify_result.checksum_mismatch]
                ph = ", ".join(["?"] * len(tilenames))
                cursor.execute(
                    f"UPDATE tiles SET {set_clause} "
                    f"WHERE tilename IN ({ph})",
                    tilenames)
                conn.commit()
                logger.warning(
                    "%d tile(s) flagged for repair. Run fetch to "
                    "re-download them, then retry export.",
                    len(tilenames))
            finally:
                conn.close()

        raise ValueError(
            "Pre-flight verification failed:\n" +
            "\n".join(f"  - {e}" for e in errors))

    logger.info("Verification passed. %d tiles verified.",
                len(verify_result.verified))
    logger.info("")

    # --- Step 3: Generate manifest ---
    logger.info("Step 3/4: Generating manifest...")
    manifest = generate_manifest(
        project_dir, data_source, include_mosaics=include_mosaics)
    manifest["files"].sort(key=lambda e: e["path"])
    logger.info("Manifest ready: %d files", len(manifest["files"]))
    logger.info("")

    # --- Step 4: Create zip ---
    logger.info("Step 4/4: Creating zip (%d files)...", len(manifest["files"]))
    file_count = 0
    try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("manifest.json",
                        json.dumps(manifest, indent=2, ensure_ascii=False))
            file_count += 1

            total = len(manifest["files"])
            interval = max(1, min(100, total // 10))
            for i, entry in enumerate(manifest["files"]):
                abs_path = os.path.join(project_dir, entry["path"])
                zf.write(abs_path, entry["path"])
                file_count += 1

                if (i + 1) % interval == 0 or i + 1 == total:
                    logger.info("Added %d/%d files to zip", i + 1, total)

        if os.path.isfile(output_path):
            os.remove(output_path)
        os.rename(tmp_path, output_path)

    except Exception:
        if os.path.isfile(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        raise

    zip_size = os.path.getsize(output_path)
    elapsed = datetime.datetime.now() - export_start
    logger.info("")
    logger.info("Export complete")
    logger.info("  Output:   %s", output_path)
    logger.info("  Size:     %.1f MB", zip_size / 1_000_000)
    logger.info("  Files:    %d", file_count)
    logger.info("  Tiles:    %d", manifest["tile_count"])
    logger.info("  Mosaics:  %s", "included" if include_mosaics else "excluded")
    logger.info("  Duration: %s", elapsed)
    logger.info("══════════════")

    return ExportResult(
        output_path=output_path,
        tile_count=manifest["tile_count"],
        file_count=file_count,
        zip_size=zip_size,
        mosaics_included=include_mosaics,
        verification_passed=True,
    )
