"""Tests for CLI argument parsing (no network, no GDAL)."""

import sys
from unittest import mock

import pytest
import argparse

from nbs.noaabathymetry.cli import str_to_bool


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
# nbs mosaic
# ---------------------------------------------------------------------------


class TestNbsMosaic:
    def test_parses_all_args(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "-d", "/tmp/test", "-s", "bluetopo", "-r", "true"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                mock_bv.assert_called_once_with(
                    project_dir="/tmp/test",
                    data_source="bluetopo",
                    relative_to_vrt=True,
                    mosaic_resolution_target=None,
                    tile_resolution_filter=None,
                    hillshade=False,
                    workers=None,
                    reproject=False,
                    output_dir=None,
                    debug=False,
                )

    def test_default_source(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "-d", "/tmp/test"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                args = mock_bv.call_args
                assert args.kwargs["data_source"] == "bluetopo"

    def test_default_relative(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "-d", "/tmp/test"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                args = mock_bv.call_args
                assert args.kwargs["relative_to_vrt"] is True


# ---------------------------------------------------------------------------
# nbs fetch
# ---------------------------------------------------------------------------


class TestNbsFetch:
    def test_parses_all_args(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "fetch", "-d", "/tmp/test", "-g", "polygon.shp", "-s", "bag"]):
            with mock.patch("nbs.noaabathymetry.cli.fetch_tiles") as mock_ft:
                main()
                mock_ft.assert_called_once_with(
                    project_dir="/tmp/test",
                    geometry="polygon.shp",
                    data_source="bag",
                    debug=False,
                    tile_resolution_filter=None,
                )

    def test_default_no_geom(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "fetch", "-d", "/tmp/test"]):
            with mock.patch("nbs.noaabathymetry.cli.fetch_tiles") as mock_ft:
                main()
                args = mock_ft.call_args
                assert args.kwargs["geometry"] is None


# ---------------------------------------------------------------------------
# Long-form argument aliases
# ---------------------------------------------------------------------------


class TestLongFormArgs:
    def test_mosaic_directory_alias(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "--directory", "/tmp/test"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                mock_bv.assert_called_once()
                assert mock_bv.call_args.kwargs["project_dir"] == "/tmp/test"

    def test_mosaic_relative_to_vrt_alias(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "--directory", "/tmp/test",
                                     "--relative-to-vrt", "false"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is False

    def test_fetch_directory_alias(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "fetch", "--directory", "/tmp/test"]):
            with mock.patch("nbs.noaabathymetry.cli.fetch_tiles") as mock_ft:
                main()
                assert mock_ft.call_args.kwargs["project_dir"] == "/tmp/test"

    def test_fetch_geometry_alias(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "fetch", "--directory", "/tmp/test",
                                     "--geometry", "area.shp"]):
            with mock.patch("nbs.noaabathymetry.cli.fetch_tiles") as mock_ft:
                main()
                assert mock_ft.call_args.kwargs["geometry"] == "area.shp"

    def test_fetch_source_alias(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "fetch", "--directory", "/tmp/test",
                                     "--source", "s102v22"]):
            with mock.patch("nbs.noaabathymetry.cli.fetch_tiles") as mock_ft:
                main()
                assert mock_ft.call_args.kwargs["data_source"] == "s102v22"


# ---------------------------------------------------------------------------
# -r flag with no value (uses const=True)
# ---------------------------------------------------------------------------


class TestRelativeFlag:
    def test_r_flag_no_value(self):
        """'-r' with no following value uses const=True."""
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "-d", "/tmp/test", "-r"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is True

    def test_r_flag_false(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "-d", "/tmp/test", "-r", "false"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is False

    def test_relative_to_vrt_flag_no_value(self):
        """'--relative-to-vrt' with no following value uses const=True."""
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic", "-d", "/tmp/test", "--relative-to-vrt"]):
            with mock.patch("nbs.noaabathymetry.cli.mosaic_tiles") as mock_bv:
                main()
                assert mock_bv.call_args.kwargs["relative_to_vrt"] is True


# ---------------------------------------------------------------------------
# Missing required args
# ---------------------------------------------------------------------------


class TestMissingRequired:
    def test_mosaic_no_dir_fails(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "mosaic"]):
            with pytest.raises(SystemExit):
                main()

    def test_fetch_no_dir_fails(self):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs", "fetch"]):
            with pytest.raises(SystemExit):
                main()

    def test_no_subcommand_prints_help(self, capsys):
        from nbs.noaabathymetry.cli import main
        with mock.patch("sys.argv", ["nbs"]):
            main()
        captured = capsys.readouterr()
        assert "usage:" in captured.out.lower() or "nbs" in captured.out
