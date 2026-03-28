"""CLI entry points for fetch_tiles and mosaic_tiles commands."""

from argparse import ArgumentParser, ArgumentTypeError

from nbs.noaabathymetry import __version__
from nbs.noaabathymetry._internal.builder import mosaic_tiles
from nbs.noaabathymetry._internal.fetcher import fetch_tiles


def str_to_bool(value):
    """Convert a string to a boolean for argparse type coercion.

    Accepts common truthy (``yes``, ``true``, ``t``, ``y``, ``1``) and
    falsy (``no``, ``false``, ``f``, ``n``, ``0``) strings, case-insensitive.

    Parameters
    ----------
    value : str | bool
        The value to convert.

    Returns
    -------
    bool

    Raises
    ------
    ArgumentTypeError
        If the string is not a recognized boolean representation.
    """
    if isinstance(value, bool):
        return value
    if value.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif value.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise ArgumentTypeError("Boolean value expected.")


def mosaic_tiles_command():
    """Console_scripts entry point for mosaic_tiles."""
    parser = ArgumentParser(prog="mosaic_tiles")
    parser.add_argument("-v", "--version", action="version",
                        version=f"%(prog)s {__version__}")
    parser.add_argument(
        "-d", "--dir", "--directory",
        help="The directory path to use. Required argument.",
        type=str, dest="dir", required=True,
    )
    parser.add_argument(
        "-s", "--source", "--data-source",
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
        "-t", "--mosaic-resolution-target",
        help="Output pixel size in meters (any positive number).",
        type=float,
        dest="mosaic_resolution_target", default=None,
    )
    parser.add_argument(
        "--tile-resolution-filter",
        help="Only include tiles at these resolutions (meters). "
             "Outputs to a separate mosaic directory. Example: --tile-resolution-filter 4 8",
        type=int, nargs="+", dest="tile_resolution_filter", default=None,
    )
    parser.add_argument(
        "--hillshade", action="store_true",
        help="Generate a hillshade GeoTIFF from the elevation band.",
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help="Number of parallel worker processes for building UTM zones.",
    )
    parser.add_argument(
        "--reproject", action="store_true",
        help="Reproject to EPSG:3857 (Web Mercator) GeoTIFFs.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        help="Custom output directory name within the project directory.",
        type=str, dest="output_dir", default=None,
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Write a diagnostic report to the project directory.",
    )
    args = parser.parse_args()
    mosaic_tiles(
        project_dir=args.dir,
        data_source=args.source,
        relative_to_vrt=args.relative_to_vrt,
        mosaic_resolution_target=args.mosaic_resolution_target,
        tile_resolution_filter=args.tile_resolution_filter,
        hillshade=args.hillshade,
        workers=args.workers,
        reproject=args.reproject,
        output_dir=args.output_dir,
        debug=args.debug,
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
        "-s", "--source", "--data-source",
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
