"""BlueTopo — NOAA NBS bathymetric data download & VRT builder."""

import logging
import sys
from importlib.metadata import version, PackageNotFoundError

from ._internal.builder import build_vrt, BuildResult
from ._internal.fetcher import fetch_tiles, FetchResult

_logger = logging.getLogger("bluetopo")
_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(logging.Formatter("[%(asctime)s] %(message)s", datefmt="%H:%M:%S"))
_logger.addHandler(_handler)
_logger.setLevel(logging.INFO)
_logger.propagate = False

try:
    __version__ = version("BlueTopo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["fetch_tiles", "build_vrt", "FetchResult", "BuildResult", "__version__"]
