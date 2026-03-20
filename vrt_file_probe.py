#!/usr/bin/env python3
"""
VRT File Access Probe — uses GDAL CPL_DEBUG logging to empirically verify
which files GDAL actually opens and which overview levels it selects at
each target resolution.

Each resolution is probed in a fresh subprocess so there is no GDAL
dataset caching between probes.

Usage: python3 vrt_file_probe.py [path_to_vrt]
Defaults to ~/utm18/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt
"""

import json
import os
import re
import subprocess
import sys
import tempfile

from osgeo import gdal

gdal.UseExceptions()

VRT_DEFAULT = os.path.expanduser(
    "~/utm18/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt"
)

# Probe location: Dense 4m area (30+ tiles in window) for maximum contrast.
PROBE_CENTER = (90000, 70000)
GROUND_WINDOW_M = 32768
TARGET_RESOLUTIONS = [4, 8, 16, 32, 64, 128, 256, 512]

# Subprocess worker script template.  Runs a single read with CPL_DEBUG
# and writes structured results to stdout as JSON.
_WORKER = r'''
import json, os, re, sys, time
from osgeo import gdal
gdal.UseExceptions()

vrt_path = sys.argv[1]
xoff, yoff, xsize, ysize = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
buf_xsize, buf_ysize = int(sys.argv[6]), int(sys.argv[7])
log_path = sys.argv[8]

gdal.SetConfigOption("CPL_DEBUG", "ON")
gdal.SetConfigOption("CPL_LOG", log_path)

ds = gdal.Open(vrt_path)
band = ds.GetRasterBand(1)

t0 = time.perf_counter()
band.ReadRaster(xoff, yoff, xsize, ysize,
                buf_xsize=buf_xsize, buf_ysize=buf_ysize,
                buf_type=gdal.GDT_Float32)
elapsed = time.perf_counter() - t0
ds = None

gdal.SetConfigOption("CPL_DEBUG", None)
gdal.SetConfigOption("CPL_LOG", None)

# Parse the log
tiff_files = set()
ovr_accessed = False
vrt_open = False
overview_msgs = []
open_events = []
log_lines = 0

if os.path.exists(log_path):
    with open(log_path, "r", errors="replace") as f:
        lines = f.readlines()
    log_lines = len(lines)
    for line in lines:
        low = line.lower()
        m = re.search(r"GDALOpen\(([^)]+)\)", line)
        if m:
            fpath = m.group(1)
            open_events.append(fpath)
            if ".tiff" in fpath.lower() or ".tif" in fpath.lower():
                tiff_files.add(os.path.basename(fpath))
            if ".ovr" in fpath.lower():
                ovr_accessed = True
            if ".vrt" in fpath.lower():
                vrt_open = True
        if "overview" in low:
            overview_msgs.append(line.rstrip())

print(json.dumps({
    "elapsed": elapsed,
    "tiff_files": sorted(tiff_files),
    "ovr_accessed": ovr_accessed,
    "vrt_open": vrt_open,
    "overview_msgs": overview_msgs,
    "open_events": open_events,
    "log_lines": log_lines,
}))
'''


def parse_result(stdout):
    """Parse JSON result from worker subprocess."""
    for line in stdout.strip().split("\n"):
        line = line.strip()
        if line.startswith("{"):
            return json.loads(line)
    return None


def main():
    vrt_path = sys.argv[1] if len(sys.argv) > 1 else VRT_DEFAULT
    if not os.path.isfile(vrt_path):
        print(f"VRT not found: {vrt_path}")
        sys.exit(1)

    ds = gdal.Open(vrt_path)
    gt = ds.GetGeoTransform()
    native_res = abs(gt[1])
    vrt_xsize = ds.RasterXSize
    vrt_ysize = ds.RasterYSize
    ds = None

    cx, cy = PROBE_CENTER
    win_px = int(GROUND_WINDOW_M / native_res)
    xoff = max(0, cx - win_px // 2)
    yoff = max(0, cy - win_px // 2)
    xsize = min(win_px, vrt_xsize - xoff)
    ysize = min(win_px, vrt_ysize - yoff)

    print(f"VRT: {vrt_path}")
    print(f"Native resolution: {native_res}m")
    print(f"Probe location: pixel ({cx}, {cy})")
    print(f"Read window: offset=({xoff}, {yoff})  size={xsize}x{ysize}  "
          f"({xsize * native_res / 1000:.1f}km x "
          f"{ysize * native_res / 1000:.1f}km)")
    print()

    log_dir = tempfile.mkdtemp(prefix="vrt_probe_")
    worker_path = os.path.join(log_dir, "_worker.py")
    with open(worker_path, "w") as f:
        f.write(_WORKER)

    # =====================================================================
    # Summary table
    # =====================================================================
    print("=" * 120)
    print(f"{'Res(m)':>6}  {'Time(s)':>8}  {'GDALOpen .tiff':>15}  "
          f"{'GDALOpen .ovr':>14}  {'Log lines':>10}  {'Classification'}")
    print("-" * 120)

    all_results = {}

    for target_res in TARGET_RESOLUTIONS:
        buf_xsize = max(1, int(xsize * native_res / target_res))
        buf_ysize = max(1, int(ysize * native_res / target_res))

        log_path = os.path.join(log_dir, f"probe_{target_res}m.log")

        result = subprocess.run(
            [sys.executable, worker_path, vrt_path,
             str(xoff), str(yoff), str(xsize), str(ysize),
             str(buf_xsize), str(buf_ysize), log_path],
            capture_output=True, text=True, timeout=120,
        )

        info = parse_result(result.stdout)
        if info is None:
            print(f"{target_res:>6}  {'ERROR':>8}  stderr: {result.stderr[:80]}")
            continue

        all_results[target_res] = info
        n_tiff = len(info["tiff_files"])
        ovr_str = "YES" if info["ovr_accessed"] else "no"

        # Classify what GDAL used
        if info["ovr_accessed"] and n_tiff == 0:
            classification = "VRT .ovr sidecar (single file)"
        elif n_tiff > 0 and not info["ovr_accessed"]:
            classification = f"Individual source COGs ({n_tiff} files)"
        elif n_tiff > 0 and info["ovr_accessed"]:
            classification = f"Source COGs + .ovr ({n_tiff} files)"
        else:
            classification = "VRT .ovr sidecar (single file)"

        print(f"{target_res:>6}  {info['elapsed']:>8.3f}  {n_tiff:>15}  "
              f"{ovr_str:>14}  {info['log_lines']:>10}  {classification}")

    print()

    # =====================================================================
    # Detailed comparison: gap zone (8m, 16m) vs .ovr zone (32m)
    # =====================================================================
    for target_res in [4, 8, 16, 32]:
        info = all_results.get(target_res)
        if info is None:
            continue
        print(f"--- Detail for {target_res}m ---")
        n_tiff = len(info["tiff_files"])
        print(f"  Source .tiff files opened (GDALOpen): {n_tiff}")
        if info["tiff_files"]:
            for f in info["tiff_files"][:10]:
                print(f"    {f}")
            if n_tiff > 10:
                print(f"    ... and {n_tiff - 10} more")
        print(f"  .ovr sidecar opened: {info['ovr_accessed']}")
        print(f"  All GDALOpen calls ({len(info['open_events'])}):")
        for evt in info["open_events"][:10]:
            basename = os.path.basename(evt.rstrip(", "))
            print(f"    {basename}")
        if len(info["open_events"]) > 10:
            print(f"    ... and {len(info['open_events']) - 10} more")
        if info["overview_msgs"]:
            print(f"  Overview messages ({len(info['overview_msgs'])}):")
            for msg in info["overview_msgs"][:5]:
                print(f"    {msg[:140]}")
        print()

    print(f"Log files saved in: {log_dir}")
    print("Done.")


if __name__ == "__main__":
    main()
