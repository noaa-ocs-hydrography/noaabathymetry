"""CLI entry point: ``nbs fetch``, ``nbs mosaic``, and ``nbs status`` subcommands."""

import sqlite3
from argparse import ArgumentTypeError

import botocore.exceptions

from nbs.noaabathymetry import __version__
from nbs.noaabathymetry.cli_formatter import PaneledArgumentParser
from nbs.noaabathymetry._internal.builder import mosaic_tiles
from nbs.noaabathymetry._internal.fetcher import fetch_tiles
from nbs.noaabathymetry._internal.status import status_tiles


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


def _add_fetch_parser(subparsers):
    """Add the ``nbs fetch`` subcommand."""
    parser = subparsers.add_parser("fetch", help="Download tiles from NOAA NBS.")
    parser.add_argument(
        "-d", "--dir", "--directory",
        help="Absolute path to the project directory.",
        type=str, dest="dir", required=True,
    )
    parser.add_argument(
        "-g", "--geom", "--geometry",
        help=("Geometry input to find intersecting tiles. "
              "Accepts: bounding box, WKT, GeoJSON, or file path. "
              "String inputs assume EPSG:4326."),
        type=str, dest="geom",
    )
    parser.add_argument(
        "-s", "--source", "--data-source",
        help="Data source identifier. BlueTopo is the default.",
        default="bluetopo", dest="source",
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
    parser.add_argument(
        "--json", action="store_true", dest="json_output",
        help="Print result as JSON to stdout.",
    )
    parser.set_defaults(func=_run_fetch, _subparser=parser)


def _add_mosaic_parser(subparsers):
    """Add the ``nbs mosaic`` subcommand."""
    parser = subparsers.add_parser("mosaic", help="Build mosaics from downloaded tiles.")
    parser.add_argument(
        "-d", "--dir", "--directory",
        help="Absolute path to the project directory.",
        type=str, dest="dir", required=True,
    )
    parser.add_argument(
        "-s", "--source", "--data-source",
        help="Data source identifier. BlueTopo is the default.",
        default="bluetopo", dest="source",
    )
    parser.add_argument(
        "-r", "--relative-to-vrt",
        help="Store VRT file paths as relative (default: true).",
        nargs="?", dest="relative_to_vrt", default=True,
        const=True, type=str_to_bool,
    )
    parser.add_argument(
        "-t", "--mosaic-resolution-target",
        help="Mosaic output pixel size in meters (any positive number).",
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
    parser.add_argument(
        "--json", action="store_true", dest="json_output",
        help="Print result as JSON to stdout.",
    )
    parser.set_defaults(func=_run_mosaic, _subparser=parser)


def _add_status_parser(subparsers):
    """Add the ``nbs status`` subcommand."""
    parser = subparsers.add_parser("status", help="Check for tile updates on S3.")
    parser.add_argument(
        "-d", "--dir", "--directory",
        help="Absolute path to the project directory.",
        type=str, dest="dir", required=True,
    )
    parser.add_argument(
        "-s", "--source", "--data-source",
        help="Data source identifier. BlueTopo is the default.",
        default="bluetopo", dest="source",
    )
    parser.add_argument(
        "--verbosity", choices=["quiet", "normal", "verbose"], default="normal",
        help="Logging verbosity (default: normal).",
    )
    parser.add_argument(
        "--json", action="store_true", dest="json_output",
        help="Print result as JSON to stdout.",
    )
    parser.set_defaults(func=_run_status, _subparser=parser)


def _print_json(result):
    """Serialize a dataclass result to JSON and print to stdout."""
    import dataclasses
    import json
    print(json.dumps(dataclasses.asdict(result), indent=2))


def _run_status(args):
    """Execute the status subcommand."""
    result = status_tiles(
        project_dir=args.dir,
        data_source=args.source,
        verbosity=args.verbosity,
    )
    if args.json_output:
        _print_json(result)


def _run_fetch(args):
    """Execute the fetch subcommand."""
    result = fetch_tiles(
        project_dir=args.dir,
        geometry=args.geom,
        data_source=args.source,
        debug=args.debug,
        tile_resolution_filter=args.tile_resolution_filter,
    )
    if args.json_output:
        _print_json(result)


def _run_mosaic(args):
    """Execute the mosaic subcommand."""
    result = mosaic_tiles(
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
    if args.json_output:
        _print_json(result)


def main():
    """Entry point for the ``nbs`` command."""
    parser = PaneledArgumentParser(prog="nbs", description="NOAA National Bathymetric Source tools.")
    parser.add_argument("-v", "--version", action="version",
                        version=f"%(prog)s {__version__}")
    subparsers = parser.add_subparsers(dest="command")
    _add_fetch_parser(subparsers)
    _add_mosaic_parser(subparsers)
    _add_status_parser(subparsers)

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        return
    subparser = getattr(args, "_subparser", parser)
    try:
        args.func(args)
    except KeyboardInterrupt:
        raise SystemExit(130)
    except (ValueError, RuntimeError, OSError, sqlite3.Error) as e:
        subparser.error(str(e))
    except botocore.exceptions.BotoCoreError as e:
        subparser.error(str(e))
    except botocore.exceptions.ClientError as e:
        subparser.error(e.response["Error"]["Message"])
