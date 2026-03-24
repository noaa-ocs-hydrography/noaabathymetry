"""Tests for CLI argument parsing (no network, no GDAL)."""

import sys
from unittest import mock

import pytest
import argparse

from nbs.bluetopo.cli import str_to_bool


# ---------------------------------------------------------------------------
# str_to_bool
# ---------------------------------------------------------------------------


class TestStrToBool:
    @pytest.mark.parametrize("val", ["true", "True", "TRUE", "yes", "YES", "y", "Y", "1", "t", "T"])
    def test_truthy_values(self, val):
        assert str_to_bool(val) is True

    @pytest.mark.parametrize("val", ["false", "False", "FALSE", "no", "NO", "n", "N", "0", "f", "F"])
    def test_falsy_values(self, val):
        assert str_to_bool(val) is False

    def test_bool_passthrough_true(self):
        assert str_to_bool(True) is True

    def test_bool_passthrough_false(self):
        assert str_to_bool(False) is False

    def test_invalid_raises(self):
        with pytest.raises(argparse.ArgumentTypeError, match="Boolean"):
            str_to_bool("maybe")

    def test_invalid_empty(self):
        with pytest.raises(argparse.ArgumentTypeError):
            str_to_bool("invalid_value")


# ---------------------------------------------------------------------------
# CLI argument parsing - build_vrt_command
# ---------------------------------------------------------------------------


class TestBuildVrtCommand:
    def test_parses_all_args(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "-d", "/tmp/test", "-s", "bluetopo", "-r", "true"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                mock_bv.assert_called_once_with(
                    project_dir="/tmp/test",
                    data_source="bluetopo",
                    relative_to_vrt=True,
                    vrt_resolution_target=None,
                    tile_resolution_filter=None,
                    hillshade=False,
                    workers=None,
                    reproject=False,
                    output_dir=None,
                    debug=False,
                )

    def test_default_source(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "-d", "/tmp/test"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                args = mock_bv.call_args
                assert args.kwargs["data_source"] == "bluetopo"

    def test_default_relative(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "-d", "/tmp/test"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                args = mock_bv.call_args
                assert args.kwargs["relative_to_vrt"] is True


# ---------------------------------------------------------------------------
# CLI argument parsing - fetch_tiles_command
# ---------------------------------------------------------------------------


class TestFetchTilesCommand:
    def test_parses_all_args(self):
        from nbs.bluetopo.cli import fetch_tiles_command
        with mock.patch("sys.argv", ["fetch_tiles", "-d", "/tmp/test", "-g", "polygon.shp", "-s", "bag"]):
            with mock.patch("nbs.bluetopo.cli.fetch_tiles") as mock_ft:
                fetch_tiles_command()
                mock_ft.assert_called_once_with(
                    project_dir="/tmp/test",
                    geometry="polygon.shp",
                    data_source="bag",
                    debug=False,
                    tile_resolution_filter=None,
                )

    def test_default_no_geom(self):
        from nbs.bluetopo.cli import fetch_tiles_command
        with mock.patch("sys.argv", ["fetch_tiles", "-d", "/tmp/test"]):
            with mock.patch("nbs.bluetopo.cli.fetch_tiles") as mock_ft:
                fetch_tiles_command()
                args = mock_ft.call_args
                assert args.kwargs["geometry"] is None


# ---------------------------------------------------------------------------
# Long-form argument aliases
# ---------------------------------------------------------------------------


class TestLongFormArgs:
    def test_build_vrt_directory_alias(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "--directory", "/tmp/test"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                mock_bv.assert_called_once()
                assert mock_bv.call_args.kwargs["project_dir"] == "/tmp/test"

    def test_build_vrt_relative_to_vrt_alias(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "--directory", "/tmp/test",
                                     "--relative_to_vrt", "false"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is False

    def test_fetch_tiles_directory_alias(self):
        from nbs.bluetopo.cli import fetch_tiles_command
        with mock.patch("sys.argv", ["fetch_tiles", "--directory", "/tmp/test"]):
            with mock.patch("nbs.bluetopo.cli.fetch_tiles") as mock_ft:
                fetch_tiles_command()
                assert mock_ft.call_args.kwargs["project_dir"] == "/tmp/test"

    def test_fetch_tiles_geometry_alias(self):
        from nbs.bluetopo.cli import fetch_tiles_command
        with mock.patch("sys.argv", ["fetch_tiles", "--directory", "/tmp/test",
                                     "--geometry", "area.shp"]):
            with mock.patch("nbs.bluetopo.cli.fetch_tiles") as mock_ft:
                fetch_tiles_command()
                assert mock_ft.call_args.kwargs["geometry"] == "area.shp"

    def test_fetch_tiles_source_alias(self):
        from nbs.bluetopo.cli import fetch_tiles_command
        with mock.patch("sys.argv", ["fetch_tiles", "--directory", "/tmp/test",
                                     "--source", "s102v22"]):
            with mock.patch("nbs.bluetopo.cli.fetch_tiles") as mock_ft:
                fetch_tiles_command()
                assert mock_ft.call_args.kwargs["data_source"] == "s102v22"


# ---------------------------------------------------------------------------
# -r flag with no value (uses const=True)
# ---------------------------------------------------------------------------


class TestRelativeFlag:
    def test_r_flag_no_value(self):
        """'-r' with no following value uses const=True."""
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "-d", "/tmp/test", "-r"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is True

    def test_r_flag_false(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "-d", "/tmp/test", "-r", "false"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is False

    def test_rel_flag_no_value(self):
        """'--rel' with no following value uses const=True."""
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt", "-d", "/tmp/test", "--rel"]):
            with mock.patch("nbs.bluetopo.cli.build_vrt") as mock_bv:
                build_vrt_command()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is True


# ---------------------------------------------------------------------------
# Missing required args
# ---------------------------------------------------------------------------


class TestMissingRequired:
    def test_build_vrt_no_dir_fails(self):
        from nbs.bluetopo.cli import build_vrt_command
        with mock.patch("sys.argv", ["build_vrt"]):
            with pytest.raises(SystemExit):
                build_vrt_command()

    def test_fetch_tiles_no_dir_fails(self):
        from nbs.bluetopo.cli import fetch_tiles_command
        with mock.patch("sys.argv", ["fetch_tiles"]):
            with pytest.raises(SystemExit):
                fetch_tiles_command()


