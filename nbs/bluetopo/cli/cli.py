import argparse
from argparse import ArgumentParser

from nbs.bluetopo.core.build_vrt import main as build_vrt
from nbs.bluetopo.core.fetch_tiles import main as fetch_tiles


def str_to_bool(relative_to_vrt):
    if isinstance(relative_to_vrt, bool):
        return relative_to_vrt
    if relative_to_vrt.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif relative_to_vrt.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def build_vrt_command():
    """
    console_scripts entry point for build_vrt cli command

    """
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--dir",
        "--directory",
        help="The directory path to use. " "Will create if it does not currently exist. Required argument.",
        type=str,
        nargs="?",
        dest="dir",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--source",
        help=("The NBS offers various products to different end-users. " "Some are available publicly. Use this argument to identify " "the data source. BlueTopo is the default."),
        default="bluetopo",
        dest="source",
        nargs="?",
    )
    parser.add_argument(
        "-r",
        "--rel",
        "--relative_to_vrt",
        help=("This bool argument will determine whether files referenced in the VRT " "are relative or absolute. The default value is true setting all paths " "inside the VRT to relative."),
        nargs="?",
        dest="relative_to_vrt",
        default="true",
        const=True,
        type=str_to_bool,
    )
    parser.add_argument(
        "-t",
        "--target-resolution",
        help="Target resolution in meters for UTM VRTs (e.g., 8). "
             "When set, forces the output pixel size instead of using the highest "
             "resolution from inputs.",
        type=float,
        dest="target_resolution",
        default=None,
    )
    args = parser.parse_args()
    build_vrt(
        project_dir=args.dir,
        data_source=args.source,
        relative_to_vrt=args.relative_to_vrt,
        target_resolution=args.target_resolution,
    )


def fetch_tiles_command():
    """
    console_scripts entry point for fetch_tiles cli command

    """
    parser = ArgumentParser()
    parser.add_argument(
        "-d",
        "--dir",
        "--directory",
        help="The directory path to use. " "Will create if it does not currently exist. Required argument.",
        type=str,
        nargs="?",
        dest="dir",
        required=True,
    )
    parser.add_argument(
        "-g",
        "--geom",
        "--geometry",
        help=("Geometry input to find intersecting available tiles. "
              "Accepts: bounding box (xmin,ymin,xmax,ymax), "
              "WKT string (e.g. POLYGON((...))) , "
              "GeoJSON string (e.g. {\"type\":\"Polygon\",...}), "
              "or a file path (shapefile, geopackage, geojson file, etc.). "
              "String inputs assume EPSG:4326. "
              "The returned tile ids at the time of intersection will be added to "
              "tracking. fetch_tiles will stay up to date with the latest data "
              "available from the NBS for all tracked tiles. This argument is "
              "not necessary if you do not want to add new tile ids to tracking."),
        type=str,
        dest="geom",
        nargs="?",
    )
    parser.add_argument(
        "-s",
        "--source",
        help=("The NBS offers various products to different end-users. " "Some are available publicly. Use this argument to identify " "the data source. BlueTopo is the default."),
        default="bluetopo",
        dest="source",
        nargs="?",
    )
    parser.add_argument(
        "-u",
        "--untrack",
        help=("This flag will untrack tiles that have missing files in your local " "download directory. fetch_tiles will no longer retrieve these tiles."),
        dest="untrack",
        action="store_true",
    )
    args = parser.parse_args()
    fetch_tiles(
        project_dir=args.dir,
        desired_area_filename=args.geom,
        untrack_missing=args.untrack,
        data_source=args.source,
    )
