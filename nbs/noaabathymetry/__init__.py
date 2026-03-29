"""noaabathymetry — NOAA NBS bathymetric data download & mosaic builder."""

import logging
import sys
from importlib.metadata import version, PackageNotFoundError

from ._internal.builder import mosaic_tiles, MosaicResult
from ._internal.fetcher import fetch_tiles, FetchResult

class _ColorFormatter(logging.Formatter):
    # -- Active: muted, per-operation colors --
    FETCH = "\033[38;5;110m"   # Kanagawa blue
    BUILD = "\033[38;5;180m"   # Kanagawa orange
    DONE = "\033[38;5;114m"    # Tokyo Night green
    # -- Alt: vibrant, per-operation colors --
    # FETCH = "\033[38;5;111m"  # Tokyo Night blue
    # BUILD = "\033[38;5;209m"  # Catppuccin peach
    # DONE = "\033[38;5;84m"    # Dracula green
    RESET = "\033[0m"

    def __init__(self, fmt, datefmt, use_color):
        super().__init__(fmt, datefmt=datefmt)
        self.use_color = use_color

    def format(self, record):
        msg = super().format(record)
        if self.use_color and "═══" in record.getMessage():
            text = record.getMessage()
            if "═══ Complete" in text:
                color = self.DONE
            elif "Fetching" in text:
                color = self.FETCH
            else:
                color = self.BUILD
            return f"{color}{msg}{self.RESET}"
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

__all__ = ["fetch_tiles", "mosaic_tiles", "FetchResult", "MosaicResult", "__version__"]
