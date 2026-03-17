"""BlueTopo — NOAA NBS bathymetric data download & VRT builder."""

from importlib.metadata import version, PackageNotFoundError

from ._internal.builder import build_vrt, BuildResult
from ._internal.fetcher import fetch_tiles, FetchResult

try:
    __version__ = version("BlueTopo")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["fetch_tiles", "build_vrt", "FetchResult", "BuildResult", "__version__"]
