"""
diagnostics.py - Diagnostic report generation for troubleshooting.

When debug=True is passed to fetch_tiles() or mosaic_tiles(), a report file
is written to the project directory capturing environment, config, DB state,
filesystem state, tile anomalies, and any errors encountered during the run.

The report contains only technical information (paths, tile names, config,
DB state). No credentials, environment variables, or personal data are
included beyond the project directory path.
"""

import datetime
import logging
import os
import platform
import sys
import traceback

logger = logging.getLogger("noaabathymetry")

from nbs.noaabathymetry._internal.config import (
    get_built_flags,
    get_disk_fields,
    get_utm_file_columns,
    get_verified_fields,
)


def _safe(fn):
    """Call *fn* and return its result, or an ``'ERROR: ...'`` string on failure."""
    try:
        return fn()
    except Exception as e:
        return f"ERROR: {e}"


class DebugReport:
    """Collects diagnostic information and writes a structured report file."""

    def __init__(self, project_dir, data_source, cfg):
        """Initialize a debug report.

        Parameters
        ----------
        project_dir : str
            Absolute path to the project directory.
        data_source : str
            Canonical data source name (e.g. ``"BlueTopo"``).
        cfg : dict
            Data source configuration dict.
        """
        self.project_dir = project_dir
        self.data_source = data_source
        self.cfg = cfg
        self.conn = None
        self.sections = []
        self.exception_text = None
        self.result_text = None

    def set_conn(self, conn):
        """Store the DB connection for schema/summary introspection during write()."""
        self.conn = conn

    def capture_exception(self):
        """Capture the current exception traceback."""
        self.exception_text = traceback.format_exc()

    def add_result(self, result):
        """Capture the ``FetchResult`` or ``MosaicResult`` for inclusion in the report."""
        if result is None:
            return
        lines = []
        for attr in sorted(vars(result)):
            val = getattr(result, attr)
            if isinstance(val, list):
                lines.append(f"  {attr}: {len(val)} item(s)")
                for item in val[:50]:
                    lines.append(f"    - {item}")
                if len(val) > 50:
                    lines.append(f"    ... and {len(val) - 50} more")
            else:
                lines.append(f"  {attr}: {val}")
        self.result_text = "\n".join(lines)

    def write(self):
        """Assemble all sections and write the report to a timestamped log file."""
        # Collect all sections in order
        self._collect_environment()
        self._collect_config()
        self._collect_filesystem()
        if self.conn:
            self._collect_db_schema()
            self._collect_db_summary()
            self._collect_tile_details()
            self._collect_utm_details()

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"noaabathymetry_debug_{timestamp}.log"
        filepath = os.path.join(self.project_dir, filename)

        lines = [
            "=" * 72,
            "  noaabathymetry Debug Report",
            "=" * 72,
            f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} "
            f"{_safe(lambda: datetime.datetime.now().astimezone().tzname())}",
            "",
        ]

        for section in self.sections:
            lines.append(section)
            lines.append("")

        if self.result_text:
            lines.append("-" * 72)
            lines.append("  PIPELINE RESULT")
            lines.append("-" * 72)
            lines.append(self.result_text)
            lines.append("")

        if self.exception_text:
            lines.append("-" * 72)
            lines.append("  EXCEPTION")
            lines.append("-" * 72)
            lines.append(self.exception_text)

        try:
            os.makedirs(self.project_dir, exist_ok=True)
            with open(filepath, "w") as f:
                f.write("\n".join(lines))
            logger.info("Debug report written to: %s", filepath)
        except Exception as e:
            logger.error("Failed to write debug report: %s", e)

    # ------------------------------------------------------------------
    # Section collectors (called in order during write())
    # ------------------------------------------------------------------

    def _collect_environment(self):
        """Section 1: Package version, Python version, GDAL version, platform."""
        from nbs.noaabathymetry import __version__
        gdal_version = _safe(lambda: __import__("osgeo").gdal.VersionInfo())
        self.sections.append("\n".join([
            "-" * 72,
            "  1. ENVIRONMENT",
            "-" * 72,
            f"  noaabathymetry   : {__version__}",
            f"  Python           : {sys.version.split()[0]}",
            f"  GDAL             : {gdal_version}",
            f"  Platform         : {platform.system()} {platform.release()} ({platform.machine()})",
        ]))

    def _collect_config(self):
        """Section 2: active data source settings, file slots, gpkg field mappings."""
        cfg = self.cfg
        slots = ", ".join(
            f"{s['name']} (gpkg: {s['gpkg_link']})"
            for s in cfg.get("file_slots", [])
        )
        gpkg = cfg.get("gpkg_fields", {})
        gpkg_str = ", ".join(f"{k}={v}" for k, v in gpkg.items())
        self.sections.append("\n".join([
            "-" * 72,
            "  2. CONFIGURATION",
            "-" * 72,
            f"  Data source      : {self.data_source}",
            f"  Canonical name   : {cfg.get('canonical_name')}",
            f"  Bucket           : {cfg.get('bucket')}",
            f"  Geom prefix      : {cfg.get('geom_prefix')}",
            f"  XML prefix       : {cfg.get('xml_prefix')}",
            f"  File slots       : {slots}",
            f"  Gpkg fields      : {gpkg_str}",
            f"  Has RAT          : {cfg.get('has_rat')}",
            f"  RAT method       : {cfg.get('rat_open_method')}",
            f"  Subdatasets      : {len(cfg['subdatasets']) if cfg.get('subdatasets') else 'None'}",
            f"  Min GDAL version : {cfg.get('min_gdal_version')}",
        ]))

    def _collect_filesystem(self):
        """Section 3: existence and size of registry DB, tile folder, mosaic folder."""
        pd = self.project_dir
        ds = self.data_source
        db_path = os.path.join(pd, f"{ds.lower()}_registry.db")
        tile_dir = os.path.join(pd, ds)
        mosaic_dir = os.path.join(pd, f"{ds}_Mosaic")

        def _dir_info(path):
            if not os.path.isdir(path):
                return "DOES NOT EXIST"
            count = len(os.listdir(path))
            return f"exists ({count} items)"

        def _file_info(path):
            if not os.path.isfile(path):
                return "DOES NOT EXIST"
            size = os.path.getsize(path)
            if size > 1024 * 1024:
                return f"exists ({size / 1024 / 1024:.1f} MB)"
            return f"exists ({size / 1024:.1f} KB)"

        self.sections.append("\n".join([
            "-" * 72,
            "  3. FILESYSTEM",
            "-" * 72,
            f"  Project dir  : {pd} ({'exists' if os.path.isdir(pd) else 'DOES NOT EXIST'})",
            f"  Registry DB  : {_file_info(db_path)}",
            f"  Tile folder  : {ds}/ — {_dir_info(tile_dir)}",
            f"  Mosaic folder: {ds}_Mosaic/ — {_dir_info(mosaic_dir)}",
        ]))

    def _collect_db_schema(self):
        """Section 4: column definitions for tiles, mosaic_utm, and catalog tables."""
        try:
            cursor = self.conn.cursor()
            lines = [
                "-" * 72,
                "  4. DATABASE SCHEMA",
                "-" * 72,
            ]
            for table in ["tiles", "mosaic_utm", self.cfg["catalog_table"]]:
                cursor.execute(f"SELECT name, type FROM pragma_table_info('{table}')")
                cols = cursor.fetchall()
                lines.append(f"  Table: {table} ({len(cols)} columns)")
                for col in cols:
                    lines.append(f"    {col['name']:40s} {col['type']}")
            self.sections.append("\n".join(lines))
        except Exception as e:
            self.sections.append(f"  4. DATABASE SCHEMA\n  ERROR: {e}")

    def _collect_db_summary(self):
        """Section 5: tile counts (verified, unverified, pending) and UTM build status."""
        try:
            cursor = self.conn.cursor()
            disk_fields = get_disk_fields(self.cfg)
            verified_fields = get_verified_fields(self.cfg)
            built_flags = get_built_flags(self.cfg)

            cursor.execute("SELECT COUNT(*) FROM tiles")
            total_tiles = cursor.fetchone()[0]

            disk_not_null = " AND ".join(f"{df} IS NOT NULL" for df in disk_fields)
            verified_all = " AND ".join(f"{vf} = 1" for vf in verified_fields)

            cursor.execute(f"SELECT COUNT(*) FROM tiles WHERE {disk_not_null} AND {verified_all}")
            verified = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM tiles WHERE {disk_not_null} AND NOT ({verified_all})")
            unverified = cursor.fetchone()[0]
            pending = total_tiles - verified - unverified

            cursor.execute("SELECT COUNT(*) FROM mosaic_utm WHERE params_key = ''")
            total_utms = cursor.fetchone()[0]
            built_check = " AND ".join(f"{f} = 1" for f in built_flags)
            cursor.execute(f"SELECT COUNT(*) FROM mosaic_utm WHERE params_key = '' AND {built_check}")
            built_utms = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(DISTINCT params_key) FROM mosaic_utm WHERE params_key != ''")
            param_partitions = cursor.fetchone()[0]

            lines = [
                "-" * 72,
                "  5. DATABASE SUMMARY",
                "-" * 72,
                f"  Tiles: {total_tiles} total",
                f"    Downloaded & verified : {verified}",
                f"    Downloaded, unverified: {unverified}",
                f"    Pending download      : {pending}",
                f"  UTM zones (default): {total_utms} total",
                f"    Built   : {built_utms}",
                f"    Unbuilt : {total_utms - built_utms}",
            ]
            if param_partitions:
                lines.append(f"  Parameterized partitions: {param_partitions}")
            self.sections.append("\n".join(lines))
        except Exception as e:
            self.sections.append(f"  5. DATABASE SUMMARY\n  ERROR: {e}")

    def _collect_tile_details(self):
        """Section 6: per-tile anomalies (missing links, missing files, unverified)."""
        try:
            cursor = self.conn.cursor()
            disk_fields = get_disk_fields(self.cfg)
            verified_fields = get_verified_fields(self.cfg)

            cursor.execute("SELECT * FROM tiles ORDER BY utm, tilename")
            tiles = [dict(row) for row in cursor.fetchall()]

            lines = [
                "-" * 72,
                "  6. TILE DETAILS",
                "-" * 72,
                f"  Total tiles: {len(tiles)}",
                "",
            ]

            # Detect anomalies
            anomalies = []
            for tile in tiles:
                issues = []
                # Missing links
                for slot in self.cfg["file_slots"]:
                    link_col = f"{slot['name']}_link"
                    link = tile.get(link_col)
                    if not link or str(link).lower() == "none":
                        issues.append(f"no {slot['name']} link")
                # Null delivered date
                if tile.get("delivered_date") is None:
                    issues.append("null delivered_date")
                # Disk path set but file missing
                for df in disk_fields:
                    if tile.get(df) and not os.path.isfile(
                            os.path.join(self.project_dir, tile[df])):
                        issues.append(f"{df} path set but file missing")
                # Downloaded but not verified
                for vf in verified_fields:
                    disk_col = vf.replace("_verified", "_disk")
                    if tile.get(disk_col) and tile.get(vf) != 1:
                        issues.append(f"{vf} not verified")
                if issues:
                    anomalies.append((tile["tilename"], issues))

            if anomalies:
                lines.append(f"  ANOMALIES ({len(anomalies)} tile(s)):")
                for name, issues in anomalies[:100]:
                    lines.append(f"    {name}: {'; '.join(issues)}")
                if len(anomalies) > 100:
                    lines.append(f"    ... and {len(anomalies) - 100} more")
                lines.append("")

            # Per-UTM tile listing
            utms = {}
            for tile in tiles:
                utm = tile.get("utm", "?")
                utms.setdefault(utm, []).append(tile["tilename"])

            lines.append(f"  Tiles by UTM zone ({len(utms)} zones):")
            for utm in sorted(utms.keys()):
                names = utms[utm]
                if len(names) <= 10:
                    lines.append(f"    UTM {utm} ({len(names)}): {', '.join(names)}")
                else:
                    preview = ", ".join(names[:5])
                    lines.append(f"    UTM {utm} ({len(names)}): {preview}, ... +{len(names) - 5} more")

            self.sections.append("\n".join(lines))
        except Exception as e:
            self.sections.append(f"  6. TILE DETAILS\n  ERROR: {e}")

    def _collect_utm_details(self):
        """Section 7: Mosaic/OVR paths and build status per UTM zone."""
        try:
            cursor = self.conn.cursor()
            utm_cols = get_utm_file_columns(self.cfg)
            built_flags = get_built_flags(self.cfg)

            cursor.execute("SELECT * FROM mosaic_utm ORDER BY params_key, utm")
            utms = [dict(row) for row in cursor.fetchall()]

            lines = [
                "-" * 72,
                "  7. UTM ZONE DETAILS",
                "-" * 72,
                f"  Total UTM zone rows: {len(utms)}",
                "",
            ]

            for utm in utms:
                pk = utm.get("params_key", "")
                pk_label = f" [params: {pk}]" if pk else ""
                built = all(utm.get(f) == 1 for f in built_flags)
                status = "BUILT" if built else "UNBUILT"
                lines.append(f"  UTM {utm['utm']}{pk_label} [{status}]")
                for col in utm_cols:
                    val = utm.get(col)
                    if val is None:
                        lines.append(f"    {col}: (none)")
                    else:
                        exists = os.path.isfile(os.path.join(self.project_dir, val))
                        flag = "" if exists else " [FILE MISSING]"
                        lines.append(f"    {col}: {val}{flag}")

            self.sections.append("\n".join(lines))
        except Exception as e:
            self.sections.append(f"  7. UTM ZONE DETAILS\n  ERROR: {e}")
