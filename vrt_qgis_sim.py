#!/usr/bin/env python3
"""
VRT QGIS Simulation — models what QGIS actually does when rendering a VRT:

1. Opens the VRT fresh (simulating a zoom/pan event)
2. Reads ALL 3 bands (not just band 1)
3. Breaks the viewport into 256x256 render tiles (QGIS default)
4. Issues separate ReadRaster calls per tile per band
5. Measures total wall-clock time for the full viewport

Usage: python3 vrt_qgis_sim.py [path_to_vrt]
"""

import math
import os
import sys
import time

from osgeo import gdal

gdal.UseExceptions()

VRT_DEFAULT = os.path.expanduser(
    "~/utm18/BlueTopo_VRT/BlueTopo_Fetched_UTM18.vrt"
)

# QGIS default render tile size
RENDER_TILE = 256

# Actual QGIS Retina canvas (2260x817 logical * 2.0 device pixel ratio)
VIEWPORT_W = 4520
VIEWPORT_H = 1634

TARGET_RESOLUTIONS = [4, 8, 16, 32, 64, 128, 256, 512]

# One representative location per category
SAMPLE_LOCATIONS = [
    ("Dense_4m_NE",          90000,  70000),
    ("Mixed_4m_8m_16m",     100000,  60000),
    ("Pure_16m_offshore",   120000, 395000),
]


def run_qgis_sim(vrt_path, cx, cy, target_res, native_res,
                 vrt_xsize, vrt_ysize, num_bands):
    """Simulate a full QGIS viewport render at a given zoom level.

    Returns (total_time, open_time, read_time, n_tiles, n_reads).
    """
    # Ground area visible at this zoom: each screen pixel = target_res metres
    ground_w = target_res * VIEWPORT_W
    ground_h = target_res * VIEWPORT_H
    src_w = int(ground_w / native_res)
    src_h = int(ground_h / native_res)

    xoff = max(0, cx - src_w // 2)
    yoff = max(0, cy - src_h // 2)
    xsize = min(src_w, vrt_xsize - xoff)
    ysize = min(src_h, vrt_ysize - yoff)

    # Output buffer matches viewport
    buf_xsize = min(VIEWPORT_W, xsize)
    buf_ysize = min(VIEWPORT_H, ysize)
    tiles_x = math.ceil(buf_xsize / RENDER_TILE)
    tiles_y = math.ceil(buf_ysize / RENDER_TILE)

    # Flush cache to simulate cold start
    old_cache = gdal.GetCacheMax()
    gdal.SetCacheMax(0)
    gdal.SetCacheMax(old_cache)

    t_total_start = time.perf_counter()

    # Phase 1: Open the VRT
    t_open_start = time.perf_counter()
    ds = gdal.Open(vrt_path)
    t_open = time.perf_counter() - t_open_start

    # Phase 2: Read render tiles across all bands
    t_read_start = time.perf_counter()
    n_reads = 0
    for band_idx in range(1, num_bands + 1):
        band = ds.GetRasterBand(band_idx)
        for ty in range(tiles_y):
            for tx in range(tiles_x):
                tile_buf_x0 = tx * RENDER_TILE
                tile_buf_y0 = ty * RENDER_TILE
                tile_buf_w = min(RENDER_TILE, buf_xsize - tile_buf_x0)
                tile_buf_h = min(RENDER_TILE, buf_ysize - tile_buf_y0)

                scale_x = xsize / buf_xsize
                scale_y = ysize / buf_ysize
                tile_src_x = int(xoff + tile_buf_x0 * scale_x)
                tile_src_y = int(yoff + tile_buf_y0 * scale_y)
                tile_src_w = int(tile_buf_w * scale_x)
                tile_src_h = int(tile_buf_h * scale_y)

                if tile_src_w <= 0 or tile_src_h <= 0:
                    continue

                band.ReadRaster(
                    tile_src_x, tile_src_y,
                    tile_src_w, tile_src_h,
                    buf_xsize=tile_buf_w, buf_ysize=tile_buf_h,
                    buf_type=gdal.GDT_Float32,
                )
                n_reads += 1

    t_read = time.perf_counter() - t_read_start
    ds = None

    t_total = time.perf_counter() - t_total_start
    n_tiles = tiles_x * tiles_y

    return t_total, t_open, t_read, n_tiles, n_reads


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
    num_bands = ds.RasterCount
    ds = None

    tiles_x = math.ceil(VIEWPORT_W / RENDER_TILE)
    tiles_y = math.ceil(VIEWPORT_H / RENDER_TILE)
    tiles_per_band = tiles_x * tiles_y

    print(f"VRT: {vrt_path}")
    print(f"  Size: {vrt_xsize}x{vrt_ysize} @ {native_res}m, {num_bands} bands")
    print(f"  Viewport: {VIEWPORT_W}x{VIEWPORT_H} px (Retina effective)")
    print(f"  Render tiles: {RENDER_TILE}x{RENDER_TILE} px "
          f"({tiles_x}x{tiles_y} = {tiles_per_band} tiles/band)")
    print(f"  Bands: {num_bands}")
    print(f"  Total reads per viewport: {tiles_per_band * num_bands}")
    print()

    print("=" * 130)
    hdr = (f"{'Location':<24} {'Res':>4} {'Ground':>14} "
           f"{'TOTAL':>7} {'Open':>7} {'Read':>7} "
           f"{'Tiles':>6} {'Reads':>6}  {'Bottleneck'}")
    print(hdr)
    print("-" * 130)

    results = []

    for name, cx, cy in SAMPLE_LOCATIONS:
        for target_res in TARGET_RESOLUTIONS:
            t_total, t_open, t_read, n_tiles, n_reads = run_qgis_sim(
                vrt_path, cx, cy, target_res, native_res,
                vrt_xsize, vrt_ysize, num_bands,
            )

            ground_w_km = target_res * VIEWPORT_W / 1000
            ground_h_km = target_res * VIEWPORT_H / 1000

            if t_open > t_read:
                bottleneck = f"VRT open ({t_open:.3f}s)"
            else:
                bottleneck = f"pixel reads ({n_reads} calls)"

            row = {
                "location": name, "target_res": target_res,
                "ground_w_km": ground_w_km, "ground_h_km": ground_h_km,
                "t_total": t_total, "t_open": t_open, "t_read": t_read,
                "n_tiles": n_tiles, "n_reads": n_reads,
            }
            results.append(row)

            print(
                f"{name:<24} {target_res:>4} "
                f"{ground_w_km:>5.0f}x{ground_h_km:<5.0f}km "
                f"{t_total:>7.3f} {t_open:>7.3f} {t_read:>7.3f} "
                f"{n_tiles:>6} {n_reads:>6}  {bottleneck}"
            )
        print()

    # Summary
    print("=" * 100)
    print("RESOLUTION SUMMARY (averaged across locations)")
    print("=" * 100)
    print(f"{'Res':>4} {'Ground':>14} {'Avg TOTAL':>10} {'Avg Open':>10} "
          f"{'Avg Read':>10} {'Avg Reads':>10}")
    print("-" * 100)

    for target_res in TARGET_RESOLUTIONS:
        rows = [r for r in results if r["target_res"] == target_res]
        if not rows:
            continue
        print(
            f"{target_res:>4} "
            f"{rows[0]['ground_w_km']:>5.0f}x{rows[0]['ground_h_km']:<5.0f}km "
            f"{sum(r['t_total'] for r in rows)/len(rows):>10.3f} "
            f"{sum(r['t_open'] for r in rows)/len(rows):>10.3f} "
            f"{sum(r['t_read'] for r in rows)/len(rows):>10.3f} "
            f"{sum(r['n_reads'] for r in rows)/len(rows):>10.0f}"
        )

    print()
    print("Done.")


if __name__ == "__main__":
    main()
