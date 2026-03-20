#!/usr/bin/env python3
"""
VRT Performance Benchmark — measures read times and file intersection counts
across multiple zoom levels and geographic locations.

Usage: python3 vrt_perf_bench.py [path_to_vrt]
Defaults to ~/utm18/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt
"""

import math
import os
import struct
import sys
import time
import xml.etree.ElementTree as ET
from collections import namedtuple

from osgeo import gdal

gdal.UseExceptions()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VRT_DEFAULT = os.path.expanduser(
    "~/utm18/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt"
)

# Ground window size in meters — each sample reads a box of this size.
GROUND_WINDOW_M = 32768  # ~32 km x 32 km

# Target resolutions to test (metres per pixel).
TARGET_RESOLUTIONS = [4, 8, 16, 32, 64, 128, 256, 512]

# Number of cold-cache timing repeats per read.
REPEATS = 3

# Sample locations as (name, pixel_x_center, pixel_y_center) in the
# full-resolution VRT pixel space.
#
# Resolution class extents (pixels):
#   4m : x=[6714,139526], y=[0,228725]     — 1682 tiles
#   8m : x=[6235,140161], y=[26672,245530]  — 85 tiles
#  16m : x=[0,151148],    y=[45408,495436]  — 53 tiles
SAMPLE_LOCATIONS = [
    ("Dense_4m_NE",           90000,  70000),   # 31 tiles (30x4m, 1x8m)
    ("Dense_4m_south",        65000, 175000),   # 30 tiles (all 4m)
    ("Mixed_4m_8m_16m",      100000,  60000),   # 22 tiles (18x4m, 3x8m, 1x16m)
    ("Pure_16m_offshore",    120000, 395000),   # 4 tiles (all 16m)
    ("8m_heavy_mix",         100000,  75000),   # 6 tiles (1x4m, 4x8m, 1x16m)
]


# ---------------------------------------------------------------------------
# Spatial index of VRT sources (band 1 only)
# ---------------------------------------------------------------------------

SourceRect = namedtuple(
    "SourceRect",
    ["filename", "dst_xoff", "dst_yoff", "dst_xsize", "dst_ysize",
     "src_xsize", "src_ysize"],
)


def build_source_index(vrt_path):
    """Parse VRT XML and extract ComplexSource/SimpleSource entries for band 1."""
    tree = ET.parse(vrt_path)
    root = tree.getroot()

    band1 = None
    for band_elem in root.findall("VRTRasterBand"):
        if band_elem.get("band") == "1":
            band1 = band_elem
            break
    if band1 is None:
        band1 = root.findall("VRTRasterBand")[0]

    sources = []
    for tag in ("ComplexSource", "SimpleSource"):
        for src in band1.findall(tag):
            sf = src.find("SourceFilename")
            sr = src.find("SrcRect")
            dr = src.find("DstRect")
            if sf is None or sr is None or dr is None:
                continue
            sources.append(SourceRect(
                filename=sf.text,
                dst_xoff=float(dr.get("xOff")),
                dst_yoff=float(dr.get("yOff")),
                dst_xsize=float(dr.get("xSize")),
                dst_ysize=float(dr.get("ySize")),
                src_xsize=float(sr.get("xSize")),
                src_ysize=float(sr.get("ySize")),
            ))
    return sources


def query_intersecting(sources, win_xoff, win_yoff, win_xsize, win_ysize):
    """Return sources whose DstRect intersects the given pixel window."""
    wx2, wy2 = win_xoff + win_xsize, win_yoff + win_ysize
    return [
        s for s in sources
        if (s.dst_xoff < wx2 and s.dst_xoff + s.dst_xsize > win_xoff and
            s.dst_yoff < wy2 and s.dst_yoff + s.dst_ysize > win_yoff)
    ]


# ---------------------------------------------------------------------------
# Overview classification
# ---------------------------------------------------------------------------

def classify_overview(ds, target_res, native_res):
    """Predict which data source GDAL will use for a given target resolution.

    GDAL picks the overview whose resolution is coarser-or-equal to the
    requested resolution.  If no VRT overview qualifies, GDAL falls through
    to individual source COGs (which have their own internal overviews).
    """
    factor = target_res / native_res
    if factor < 2.0:
        return "source_fullres"

    band = ds.GetRasterBand(1)
    ovr_count = band.GetOverviewCount()

    # Build (level_index, overview_factor) for VRT .ovr levels
    ovr_levels = []
    for i in range(ovr_count):
        ovr = band.GetOverview(i)
        ovr_factor = ds.RasterXSize / ovr.XSize
        ovr_levels.append((i, ovr_factor))

    # GDAL selects the overview with smallest dimension >= buf_size,
    # i.e. the overview whose factor is the largest that is <= requested factor.
    best = None
    for i, of in ovr_levels:
        if of <= factor * 1.2:  # allow small tolerance
            if best is None or of > best[1]:
                best = (i, of)

    if best is not None:
        lvl, of = best
        return f"vrt_ovr_level_{lvl} ({native_res * of:.0f}m, {of:.0f}x)"

    # No suitable VRT overview → GDAL reads from individual source COGs.
    # Each COG has internal overviews that GDAL will use for downsampling.
    return f"source_cog_internal (~{target_res}m, {factor:.0f}x)"


# ---------------------------------------------------------------------------
# Tile resolution classification helpers
# ---------------------------------------------------------------------------

def classify_tile(src):
    """Return the effective ground resolution of a source tile."""
    ratio = src.dst_xsize / src.src_xsize if src.src_xsize else 0
    if abs(ratio - 1.0) < 0.15:
        return 4
    if abs(ratio - 2.0) < 0.3:
        return 8
    if abs(ratio - 4.0) < 0.6:
        return 16
    return round(ratio * 4)


# ---------------------------------------------------------------------------
# Read & measure
# ---------------------------------------------------------------------------

def flush_gdal_cache():
    """Flush GDAL block cache for cold reads."""
    old = gdal.GetCacheMax()
    gdal.SetCacheMax(0)
    gdal.SetCacheMax(old)


def measure_read(ds, xoff, yoff, xsize, ysize, buf_xsize, buf_ysize,
                 repeats=REPEATS):
    """Timed reads returning (min, median, max) in seconds plus last buffer."""
    band = ds.GetRasterBand(1)
    times = []
    data = None
    for _ in range(repeats):
        flush_gdal_cache()
        t0 = time.perf_counter()
        data = band.ReadRaster(
            xoff, yoff, xsize, ysize,
            buf_xsize=buf_xsize, buf_ysize=buf_ysize,
            buf_type=gdal.GDT_Float32,
        )
        t1 = time.perf_counter()
        times.append(t1 - t0)
    times.sort()
    median = times[len(times) // 2]
    return times[0], median, times[-1], data


def count_nan(data_bytes, pixel_count):
    """Count NaN pixels in a Float32 raster buffer."""
    vals = struct.unpack(f"{pixel_count}f", data_bytes)
    return sum(1 for v in vals if math.isnan(v))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    vrt_path = sys.argv[1] if len(sys.argv) > 1 else VRT_DEFAULT
    if not os.path.isfile(vrt_path):
        print(f"VRT not found: {vrt_path}")
        sys.exit(1)

    print(f"VRT: {vrt_path}")
    print(f"Ground window: {GROUND_WINDOW_M}m x {GROUND_WINDOW_M}m")
    print(f"Resolutions: {TARGET_RESOLUTIONS}")
    print(f"Repeats per read: {REPEATS}")
    print()

    # --- Parse VRT sources ---
    print("Parsing VRT XML for source index...")
    sources = build_source_index(vrt_path)
    print(f"  {len(sources)} sources in band 1")

    # --- Open VRT ---
    ds = gdal.Open(vrt_path)
    gt = ds.GetGeoTransform()
    native_res = abs(gt[1])
    vrt_xsize = ds.RasterXSize
    vrt_ysize = ds.RasterYSize
    print(f"  VRT: {vrt_xsize}x{vrt_ysize} @ {native_res}m")

    band = ds.GetRasterBand(1)
    ovr_count = band.GetOverviewCount()
    print(f"  Overviews in .ovr: {ovr_count}")
    for i in range(ovr_count):
        ovr = band.GetOverview(i)
        factor = vrt_xsize / ovr.XSize
        print(f"    Level {i}: {ovr.XSize}x{ovr.YSize} "
              f"(factor {factor:.1f}x = {native_res * factor:.0f}m)")
    print()

    # Window size in native pixels
    win_px = int(GROUND_WINDOW_M / native_res)

    # =====================================================================
    # LOCATION SUMMARY
    # =====================================================================
    print("=" * 110)
    print("LOCATION SUMMARY")
    print("=" * 110)
    fmt = "{:<28} {:>14} {:>12} {:>10} {:>8} {:>8} {:>8}"
    print(fmt.format("Location", "Center (px)", "Window (px)",
                     "Intersect", "4m", "8m", "16m"))
    print("-" * 110)

    for name, cx, cy in SAMPLE_LOCATIONS:
        xoff = max(0, cx - win_px // 2)
        yoff = max(0, cy - win_px // 2)
        xsize = min(win_px, vrt_xsize - xoff)
        ysize = min(win_px, vrt_ysize - yoff)
        hits = query_intersecting(sources, xoff, yoff, xsize, ysize)
        by_res = {}
        for h in hits:
            r = classify_tile(h)
            by_res[r] = by_res.get(r, 0) + 1
        print(fmt.format(
            name,
            f"({cx},{cy})",
            f"{xsize}x{ysize}",
            str(len(hits)),
            str(by_res.get(4, 0)),
            str(by_res.get(8, 0)),
            str(by_res.get(16, 0)),
        ))
    print()

    # =====================================================================
    # PER-READ DETAIL
    # =====================================================================
    print("=" * 150)
    print("PER-READ DETAIL")
    print("=" * 150)
    hdr = (f"{'Location':<28} {'Res':>4} {'t_min':>8} {'t_med':>8} "
           f"{'t_max':>8} {'Files':>6} {'SrcPx(M)':>9} {'BufPx(K)':>9} "
           f"{'Ratio':>6} {'NaN%':>6}  {'Overview Source'}")
    print(hdr)
    print("-" * 150)

    results = []

    for name, cx, cy in SAMPLE_LOCATIONS:
        xoff = max(0, cx - win_px // 2)
        yoff = max(0, cy - win_px // 2)
        xsize = min(win_px, vrt_xsize - xoff)
        ysize = min(win_px, vrt_ysize - yoff)

        for target_res in TARGET_RESOLUTIONS:
            buf_xsize = max(1, int(xsize * native_res / target_res))
            buf_ysize = max(1, int(ysize * native_res / target_res))

            hits = query_intersecting(sources, xoff, yoff, xsize, ysize)
            total_src_px = sum(h.src_xsize * h.src_ysize for h in hits)

            ovr_class = classify_overview(ds, target_res, native_res)

            min_t, med_t, max_t, data = measure_read(
                ds, xoff, yoff, xsize, ysize, buf_xsize, buf_ysize)

            pixel_count = buf_xsize * buf_ysize
            nan_count = count_nan(data, pixel_count)
            nan_pct = 100.0 * nan_count / pixel_count if pixel_count else 0

            ratio = (xsize * ysize) / (buf_xsize * buf_ysize) \
                if buf_xsize * buf_ysize else 0

            row = {
                "location": name, "target_res": target_res,
                "min_t": min_t, "med_t": med_t, "max_t": max_t,
                "files": len(hits), "src_px_m": total_src_px / 1e6,
                "buf_px_k": pixel_count / 1e3, "ratio": ratio,
                "nan_pct": nan_pct, "ovr_class": ovr_class,
            }
            results.append(row)

            print(
                f"{name:<28} {target_res:>4} {min_t:>8.3f} {med_t:>8.3f} "
                f"{max_t:>8.3f} {len(hits):>6} {total_src_px / 1e6:>9.1f} "
                f"{pixel_count / 1e3:>9.1f} {ratio:>6.1f} "
                f"{nan_pct:>5.1f}%  {ovr_class}"
            )

        print()  # blank line between locations

    # =====================================================================
    # RESOLUTION SUMMARY
    # =====================================================================
    print("=" * 120)
    print("RESOLUTION SUMMARY")
    print("=" * 120)
    rfmt = (f"{'Res':>4} {'Avg t':>8} {'Min t':>8} {'Max t':>8} "
            f"{'Avg files':>10} {'Avg NaN%':>9}  {'Overview Source'}")
    print(rfmt)
    print("-" * 120)

    for target_res in TARGET_RESOLUTIONS:
        rows = [r for r in results if r["target_res"] == target_res]
        if not rows:
            continue
        avg_t = sum(r["med_t"] for r in rows) / len(rows)
        min_t = min(r["med_t"] for r in rows)
        max_t = max(r["med_t"] for r in rows)
        avg_files = sum(r["files"] for r in rows) / len(rows)
        avg_nan = sum(r["nan_pct"] for r in rows) / len(rows)
        ovr = rows[0]["ovr_class"]
        print(
            f"{target_res:>4} {avg_t:>8.3f} {min_t:>8.3f} {max_t:>8.3f} "
            f"{avg_files:>10.1f} {avg_nan:>8.1f}%  {ovr}"
        )

    print()
    print("Done.")
    ds = None


if __name__ == "__main__":
    main()
