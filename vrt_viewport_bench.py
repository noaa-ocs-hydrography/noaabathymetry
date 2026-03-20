#!/usr/bin/env python3
"""
VRT Viewport Benchmark — simulates real QGIS/map viewer behavior where the
screen size is fixed (~1024x1024 pixels) and zoom level determines how much
ground area is visible.

Zoomed in tight (4m) = small ground area, few source COGs.
Zoomed out wide (512m) = huge ground area, many source COGs (but uses .ovr).

Usage: python3 vrt_viewport_bench.py [path_to_vrt]
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

VRT_DEFAULT = os.path.expanduser(
    "~/utm18/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt"
)

# Simulated screen/viewport size in pixels — what the map viewer requests.
VIEWPORT_PX = 1024

# Target resolutions (metres per pixel on screen).
TARGET_RESOLUTIONS = [4, 8, 16, 32, 64, 128, 256, 512]

REPEATS = 3

# Sample locations as (name, pixel_x_center, pixel_y_center).
# Same data-rich locations as vrt_perf_bench.py.
SAMPLE_LOCATIONS = [
    ("Dense_4m_NE",          90000,  70000),
    ("Dense_4m_south",       65000, 175000),
    ("Mixed_4m_8m_16m",     100000,  60000),
    ("Pure_16m_offshore",   120000, 395000),
    ("8m_heavy_mix",        100000,  75000),
]

SourceRect = namedtuple(
    "SourceRect",
    ["filename", "dst_xoff", "dst_yoff", "dst_xsize", "dst_ysize",
     "src_xsize", "src_ysize"],
)


def build_source_index(vrt_path):
    tree = ET.parse(vrt_path)
    root = tree.getroot()
    band1 = None
    for b in root.findall("VRTRasterBand"):
        if b.get("band") == "1":
            band1 = b
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


def query_intersecting(sources, xoff, yoff, xsize, ysize):
    wx2, wy2 = xoff + xsize, yoff + ysize
    return [
        s for s in sources
        if (s.dst_xoff < wx2 and s.dst_xoff + s.dst_xsize > xoff and
            s.dst_yoff < wy2 and s.dst_yoff + s.dst_ysize > yoff)
    ]


def classify_overview(ds, target_res, native_res):
    factor = target_res / native_res
    if factor < 2.0:
        return "source_fullres"
    band = ds.GetRasterBand(1)
    ovr_count = band.GetOverviewCount()
    ovr_levels = []
    for i in range(ovr_count):
        ovr = band.GetOverview(i)
        ovr_levels.append((i, ds.RasterXSize / ovr.XSize))
    best = None
    for i, of in ovr_levels:
        if of <= factor * 1.2:
            if best is None or of > best[1]:
                best = (i, of)
    if best is not None:
        lvl, of = best
        return f"vrt_ovr_{lvl} ({native_res * of:.0f}m)"
    return f"source_cog (~{target_res}m)"


def flush_gdal_cache():
    old = gdal.GetCacheMax()
    gdal.SetCacheMax(0)
    gdal.SetCacheMax(old)


def measure_read(ds, xoff, yoff, xsize, ysize, buf_xsize, buf_ysize):
    band = ds.GetRasterBand(1)
    times = []
    data = None
    for _ in range(REPEATS):
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
    return times[0], times[len(times) // 2], times[-1], data


def count_nan(data_bytes, pixel_count):
    vals = struct.unpack(f"{pixel_count}f", data_bytes)
    return sum(1 for v in vals if math.isnan(v))


def main():
    vrt_path = sys.argv[1] if len(sys.argv) > 1 else VRT_DEFAULT
    if not os.path.isfile(vrt_path):
        print(f"VRT not found: {vrt_path}")
        sys.exit(1)

    print(f"VRT: {vrt_path}")
    print(f"Viewport: {VIEWPORT_PX}x{VIEWPORT_PX} px (fixed screen size)")
    print(f"Resolutions: {TARGET_RESOLUTIONS}")
    print(f"Repeats: {REPEATS}")
    print()

    sources = build_source_index(vrt_path)
    ds = gdal.Open(vrt_path)
    gt = ds.GetGeoTransform()
    native_res = abs(gt[1])
    vrt_xsize = ds.RasterXSize
    vrt_ysize = ds.RasterYSize

    band = ds.GetRasterBand(1)
    ovr_count = band.GetOverviewCount()
    print(f"VRT: {vrt_xsize}x{vrt_ysize} @ {native_res}m, {ovr_count} overviews")
    for i in range(ovr_count):
        ovr = band.GetOverview(i)
        f = vrt_xsize / ovr.XSize
        print(f"  Level {i}: {ovr.XSize}x{ovr.YSize} ({f:.0f}x = {native_res*f:.0f}m)")
    print()

    # Explain the viewport model
    print("Viewport model: screen is always 1024x1024 px.")
    print("At each zoom level, ground window scales with resolution:")
    print()
    print(f"  {'Res':>4}  {'Ground window':>16}  {'VRT pixels read':>16}  {'Output buf':>12}")
    print(f"  {'':>4}  {'(m)':>16}  {'(src window)':>16}  {'(viewport)':>12}")
    print(f"  {'-'*4}  {'-'*16}  {'-'*16}  {'-'*12}")
    for target_res in TARGET_RESOLUTIONS:
        ground_m = target_res * VIEWPORT_PX
        src_px = int(ground_m / native_res)
        print(f"  {target_res:>4}  {ground_m:>10}m x {ground_m}m"
              f"  {src_px:>6} x {src_px:<6}"
              f"  {VIEWPORT_PX:>4} x {VIEWPORT_PX}")
    print()

    # Per-read detail
    print("=" * 140)
    print("PER-READ DETAIL")
    print("=" * 140)
    hdr = (f"{'Location':<24} {'Res':>4} {'Ground(km)':>10} {'t_min':>7} "
           f"{'t_med':>7} {'t_max':>7} {'Files':>6} {'SrcPx(K)':>9} "
           f"{'NaN%':>6}  {'Overview Source'}")
    print(hdr)
    print("-" * 140)

    results = []

    for name, cx, cy in SAMPLE_LOCATIONS:
        for target_res in TARGET_RESOLUTIONS:
            ground_m = target_res * VIEWPORT_PX
            src_px = int(ground_m / native_res)

            xoff = max(0, cx - src_px // 2)
            yoff = max(0, cy - src_px // 2)
            xsize = min(src_px, vrt_xsize - xoff)
            ysize = min(src_px, vrt_ysize - yoff)
            buf_xsize = VIEWPORT_PX
            buf_ysize = VIEWPORT_PX

            hits = query_intersecting(sources, xoff, yoff, xsize, ysize)
            ovr_class = classify_overview(ds, target_res, native_res)

            min_t, med_t, max_t, data = measure_read(
                ds, xoff, yoff, xsize, ysize, buf_xsize, buf_ysize)

            pixel_count = buf_xsize * buf_ysize
            nan_count = count_nan(data, pixel_count)
            nan_pct = 100.0 * nan_count / pixel_count if pixel_count else 0

            row = {
                "location": name, "target_res": target_res,
                "ground_km": ground_m / 1000,
                "min_t": min_t, "med_t": med_t, "max_t": max_t,
                "files": len(hits), "src_px_k": (xsize * ysize) / 1e3,
                "nan_pct": nan_pct, "ovr_class": ovr_class,
            }
            results.append(row)

            print(
                f"{name:<24} {target_res:>4} {ground_m/1000:>8.1f}km "
                f"{min_t:>7.3f} {med_t:>7.3f} {max_t:>7.3f} "
                f"{len(hits):>6} {(xsize*ysize)/1e3:>9.1f} "
                f"{nan_pct:>5.1f}%  {ovr_class}"
            )

        print()

    # Resolution summary
    print("=" * 120)
    print("RESOLUTION SUMMARY")
    print("=" * 120)
    print(f"{'Res':>4} {'Ground':>8} {'Avg t':>7} {'Min t':>7} {'Max t':>7} "
          f"{'Avg files':>10} {'Avg NaN%':>9}  {'Overview Source'}")
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
        ground_km = rows[0]["ground_km"]
        ovr = rows[0]["ovr_class"]
        print(
            f"{target_res:>4} {ground_km:>6.1f}km {avg_t:>7.3f} {min_t:>7.3f} "
            f"{max_t:>7.3f} {avg_files:>10.1f} {avg_nan:>8.1f}%  {ovr}"
        )

    print()
    print("Done.")
    ds = None


if __name__ == "__main__":
    main()
