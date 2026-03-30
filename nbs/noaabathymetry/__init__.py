"""noaabathymetry — NOAA NBS bathymetric data download, mosaic, and status tools."""

import logging
import sys
from importlib.metadata import version, PackageNotFoundError

from ._internal.builder import mosaic_tiles, MosaicResult
from ._internal.fetcher import fetch_tiles, FetchResult
from ._internal.status import status_tiles, StatusResult

class _ColorFormatter(logging.Formatter):
    # -- Active: muted, per-operation colors --
    FETCH = "\033[38;5;110m"    # Kanagawa blue
    BUILD = "\033[38;5;180m"    # Kanagawa orange
    STATUS = "\033[38;5;183m"   # Kanagawa purple
    DONE = "\033[38;5;114m"     # Tokyo Night green
    RESET = "\033[0m"

    def __init__(self, fmt, datefmt, use_color):
        super().__init__(fmt, datefmt=datefmt)
        self.use_color = use_color

    def format(self, record):
        msg = super().format(record)
        if self.use_color and "═══" in record.getMessage():
            text = record.getMessage()
            if text.strip("═ ") == "":
                color = self.DONE
            elif "═══ Fetch ═══" in text:
                color = self.FETCH
            elif "═══ Mosaic ═══" in text:
                color = self.BUILD
            elif "═══ Status ═══" in text:
                color = self.STATUS
            else:
                color = self.RESET
            parts = msg.split("] ", 1)
            return f"{parts[0]}] {color}{parts[1]}{self.RESET}"
        return msg

_logger = logging.getLogger("noaabathymetry")
_handler = logging.StreamHandler(sys.stderr)
_handler.setFormatter(_ColorFormatter("[%(asctime)s] %(message)s", "%H:%M:%S",
                                      use_color=sys.stderr.isatty()))
_logger.addHandler(_handler)
_logger.setLevel(logging.INFO)
_logger.propagate = False

try:
    __version__ = version("noaabathymetry")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["fetch_tiles", "mosaic_tiles", "status_tiles",
           "FetchResult", "MosaicResult", "StatusResult", "__version__"]
