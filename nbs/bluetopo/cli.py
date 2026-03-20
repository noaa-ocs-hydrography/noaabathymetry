"""CLI entry points for fetch_tiles and build_vrt commands."""

from argparse import ArgumentParser, ArgumentTypeError

from nbs.bluetopo import __version__
from nbs.bluetopo._internal.builder import build_vrt
from nbs.bluetopo._internal.fetcher import fetch_tiles


def str_to_bool(value):
    if isinstance(value, bool):
        return value
    if value.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif value.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise ArgumentTypeError("Boolean value expected.")


def build_vrt_command():
    """Console_scripts entry point for build_vrt."""
    parser = ArgumentParser(prog="build_vrt")
    parser.add_argument("-v", "--version", action="version",
                        version=f"%(prog)s {__version__}")
    parser.add_argument(
        "-d", "--dir", "--directory",
        help="The directory path to use. Required argument.",
        type=str, dest="dir", required=True,
    )
    parser.add_argument(
        "-s", "--source",
        help="Data source identifier. BlueTopo is the default.",
        default="bluetopo", dest="source", nargs="?",
    )
    parser.add_argument(
        "-r", "--rel", "--relative_to_vrt",
        help="Store VRT file paths as relative (default: true).",
        nargs="?", dest="relative_to_vrt", default=True,
        const=True, type=str_to_bool,
    )
    parser.add_argument(
        "-t", "--vrt-resolution-target",
        help="VRT output pixel size in meters (any positive number).",
        type=float,
        dest="vrt_resolution_target", default=None,
    )
    parser.add_argument(
        "--tile-resolution-filter",
        help="Only include tiles at these resolutions (meters). "
             "Outputs to a separate VRT directory. Example: --tile-resolution-filter 4 8",
        type=int, nargs="+", dest="tile_resolution_filter", default=None,
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Write a diagnostic report to the project directory.",
    )
    args = parser.parse_args()
    build_vrt(
        project_dir=args.dir,
        data_source=args.source,
        relative_to_vrt=args.relative_to_vrt,
        vrt_resolution_target=args.vrt_resolution_target,
        debug=args.debug,
        tile_resolution_filter=args.tile_resolution_filter,
    )


def fetch_tiles_command():
    """Console_scripts entry point for fetch_tiles."""
    parser = ArgumentParser(prog="fetch_tiles")
    parser.add_argument("-v", "--version", action="version",
                        version=f"%(prog)s {__version__}")
    parser.add_argument(
        "-d", "--dir", "--directory",
        help="The directory path to use. Required argument.",
        type=str, dest="dir", required=True,
    )
    parser.add_argument(
        "-g", "--geom", "--geometry",
        help=("Geometry input to find intersecting tiles. "
              "Accepts: bounding box, WKT, GeoJSON, or file path. "
              "String inputs assume EPSG:4326."),
        type=str, dest="geom", nargs="?",
    )
    parser.add_argument(
        "-s", "--source",
        help="Data source identifier. BlueTopo is the default.",
        default="bluetopo", dest="source", nargs="?",
    )
    parser.add_argument(
        "--tile-resolution-filter",
        help="Only fetch tiles at these resolutions (meters). Example: --tile-resolution-filter 4 8",
        type=int, nargs="+", dest="tile_resolution_filter", default=None,
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Write a diagnostic report to the project directory.",
    )
    args = parser.parse_args()
    fetch_tiles(
        project_dir=args.dir,
        geometry=args.geom,
        data_source=args.source,
        debug=args.debug,
        tile_resolution_filter=args.tile_resolution_filter,
    )
