"""Tests for local data source resolution in main() entry points.

When a user passes a directory path (instead of a named source like
"bluetopo"), both fetch_tiles.main() and build_vrt.main() scan for
a geopackage whose filename encodes the data source name:

    /some/dir/HSD_Tile_Scheme.gpkg  ->  get_local_config("HSD")
    /some/dir/BlueTopo_Tile_Scheme.gpkg  ->  get_local_config("BlueTopo")
    /some/dir/Unknown_Tile_Scheme.gpkg  ->  get_local_config("Unknown")

If the resolved name matches a known source, that source's config is
used as the base (preserving file_slots, RAT settings, etc.).
Otherwise, BlueTopo is used as the base with the full KNOWN_RAT_FIELDS
superset.  In both cases, S3 prefixes are cleared for local file access.

These tests exercise that resolution logic and the associated error paths
without hitting S3 or running the full download/build pipeline.
"""

import os
from unittest import mock

import pytest

from nbs.noaabathymetry._internal.config import get_config, get_local_config, KNOWN_RAT_FIELDS
import nbs.noaabathymetry._internal.fetcher as fetch_tiles_mod
import nbs.noaabathymetry._internal.builder as build_vrt_mod


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_local_dir(tmp_path, gpkg_names):
    """Create a local directory containing fake tile-scheme geopackages."""
    local_dir = str(tmp_path / "local_source")
    os.makedirs(local_dir, exist_ok=True)
    for name in gpkg_names:
        with open(os.path.join(local_dir, name), "w") as f:
            f.write("fake gpkg")
    return local_dir


# ---------------------------------------------------------------------------
# fetch_tiles.main() local source resolution
# ---------------------------------------------------------------------------


class TestFetchTilesLocalResolution:
    """Test the config resolution in fetch_tiles.main() for local dirs."""

    def test_hsd_gpkg_resolves_to_hsd_local_config(self, tmp_path):
        """HSD_Tile_Scheme.gpkg via directory -> local config based on HSD."""
        local_dir = _create_local_dir(tmp_path, ["HSD_Tile_Scheme.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        # Mock everything after config resolution to prevent real pipeline work
        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "HSD"
            assert len(call_cfg["file_slots"]) == 2

    def test_bluetopo_gpkg_resolves_to_local_bluetopo_config(self, tmp_path):
        """BlueTopo_Tile_Scheme.gpkg via directory -> local config based on BlueTopo."""
        local_dir = _create_local_dir(tmp_path, ["BlueTopo_Tile_Scheme.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "BlueTopo"
            assert len(call_cfg["file_slots"]) == 2

    def test_unknown_gpkg_uses_local_config(self, tmp_path):
        """UnknownSource_Tile_Scheme.gpkg -> get_local_config with resolved name."""
        local_dir = _create_local_dir(tmp_path, ["UnknownSource_Tile_Scheme.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "UnknownSource"
            assert call_cfg["geom_prefix"] is None
            assert len(call_cfg["rat_fields"]) == len(KNOWN_RAT_FIELDS)

    def test_bag_gpkg_resolves_to_bag_local_config(self, tmp_path):
        """BAG-named gpkg in local dir gets BAG config with local overrides."""
        local_dir = _create_local_dir(tmp_path, ["BAG_Tile_Scheme.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "BAG"
            assert len(call_cfg["file_slots"]) == 1

    def test_no_gpkg_raises_valueerror(self, tmp_path):
        """Directory with no tile-scheme gpkg raises ValueError."""
        local_dir = _create_local_dir(tmp_path, [])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with pytest.raises(ValueError, match="tile scheme file"):
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )

    def test_gpkg_without_tile_scheme_in_name_raises(self, tmp_path):
        """Geopackage that doesn't contain 'Tile_Scheme' is ignored."""
        local_dir = _create_local_dir(tmp_path, ["HSD_Data.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with pytest.raises(ValueError, match="tile scheme file"):
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )

    def test_hsd_by_name_raises_valueerror(self, tmp_path):
        """Passing 'hsd' as source name (not a dir path) raises ValueError."""
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with pytest.raises(ValueError, match="local-only"):
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source="hsd",
            )

    def test_multiple_gpkgs_picks_reverse_sorted_first(self, tmp_path):
        """Multiple Tile_Scheme gpkgs -> reverse sorted, first is used."""
        local_dir = _create_local_dir(tmp_path, [
            "AAA_Tile_Scheme.gpkg",
            "ZZZ_Tile_Scheme.gpkg",
        ])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            # "ZZZ" sorted first (reverse), unknown -> uses local config
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "ZZZ"

    def test_multiple_gpkgs_hsd_and_bluetopo(self, tmp_path):
        """HSD + BlueTopo gpkgs in same dir -> HSD wins (reverse sort)."""
        local_dir = _create_local_dir(tmp_path, [
            "BlueTopo_Tile_Scheme.gpkg",
            "HSD_Tile_Scheme.gpkg",
        ])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            # "HSD" > "BlueTopo" in reverse sort, and HSD is a known config
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "HSD"

    def test_multiple_gpkgs_bluetopo_and_unknown(self, tmp_path):
        """BlueTopo + Unknown gpkgs -> Unknown wins (reverse sort, U > B)."""
        local_dir = _create_local_dir(tmp_path, [
            "BlueTopo_Tile_Scheme.gpkg",
            "Unknown_Tile_Scheme.gpkg",
        ])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            # "Unknown" > "BlueTopo" in reverse sort -> local config
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "Unknown"

    def test_multiple_gpkgs_only_tile_scheme_considered(self, tmp_path):
        """Non-Tile_Scheme gpkgs are ignored even if they sort higher."""
        local_dir = _create_local_dir(tmp_path, [
            "ZZZ_Data.gpkg",
            "BlueTopo_Tile_Scheme.gpkg",
        ])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            # ZZZ_Data.gpkg filtered out (no "Tile_Scheme"), BlueTopo used
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "BlueTopo"

    def test_local_passes_local_dir_to_build_download_plan(self, tmp_path):
        """Local source passes local_dir to build_download_plan."""
        local_dir = _create_local_dir(tmp_path, ["BlueTopo_Tile_Scheme.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan",
                               return_value=({}, [], [])) as mock_plan, \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            # build_download_plan receives local_dir as keyword argument
            call_kwargs = mock_plan.call_args[1]
            assert call_kwargs["local_dir"] == local_dir

    def test_local_sets_geom_prefix_to_local_dir(self, tmp_path):
        """Local source sets geom_prefix to the local directory path."""
        local_dir = _create_local_dir(tmp_path, ["BlueTopo_Tile_Scheme.gpkg"])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation",
                               return_value=None) as mock_tess, \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=local_dir,
            )
            # get_tessellation receives the local dir as geom_prefix
            tess_args = mock_tess.call_args[0]
            assert tess_args[2] == local_dir

    def test_relative_path_raises(self, tmp_path):
        """Relative project_dir path raises ValueError."""
        with pytest.raises(ValueError, match="absolute path"):
            fetch_tiles_mod.fetch_tiles(
                project_dir="relative/path",
                data_source="bluetopo",
            )

    def test_none_defaults_to_bluetopo(self, tmp_path):
        """data_source=None defaults to 'bluetopo'."""
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with mock.patch.object(fetch_tiles_mod, "connect") as mock_conn, \
             mock.patch.object(fetch_tiles_mod, "get_tessellation", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "upsert_tiles"), \
             mock.patch.object(fetch_tiles_mod, "all_db_tiles", return_value=[]), \
             mock.patch.object(fetch_tiles_mod, "_get_s3_client", return_value=None), \
             mock.patch.object(fetch_tiles_mod, "build_download_plan", return_value=({}, [], [])), \
             mock.patch.object(fetch_tiles_mod, "execute_downloads", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            fetch_tiles_mod.fetch_tiles(
                project_dir=project_dir,
                data_source=None,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "BlueTopo"


# ---------------------------------------------------------------------------
# build_vrt.main() local source resolution
# ---------------------------------------------------------------------------


class TestBuildVrtLocalResolution:
    """Test the config resolution in build_vrt.main() for local dirs."""

    def _setup_project(self, tmp_path, source_name, cfg):
        """Create a project dir with registry DB and tile folder."""
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)
        # Create the registry DB file
        from nbs.noaabathymetry._internal.db import connect as connect_to_survey_registry
        conn = connect_to_survey_registry(project_dir, cfg)
        conn.close()
        # Create the tile folder that main() expects
        os.makedirs(os.path.join(project_dir, source_name), exist_ok=True)
        return project_dir

    def test_hsd_gpkg_resolves_to_hsd_local_config(self, tmp_path):
        """HSD_Tile_Scheme.gpkg via directory -> local config based on HSD."""
        cfg = get_local_config("HSD")
        local_dir = _create_local_dir(tmp_path, ["HSD_Tile_Scheme.gpkg"])
        project_dir = self._setup_project(tmp_path, "HSD", cfg)

        with mock.patch.object(build_vrt_mod, "connect") as mock_conn, \
             mock.patch.object(build_vrt_mod, "missing_utms", return_value=[]), \
             mock.patch.object(build_vrt_mod, "select_unbuilt_utms", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            build_vrt_mod.build_vrt(
                project_dir=project_dir,
                data_source=local_dir,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "HSD"
            assert len(call_cfg["file_slots"]) == 2

    def test_no_gpkg_raises_valueerror(self, tmp_path):
        """Directory with no tile-scheme gpkg raises ValueError."""
        local_dir = _create_local_dir(tmp_path, [])
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with pytest.raises(ValueError, match="tile scheme file"):
            build_vrt_mod.build_vrt(
                project_dir=project_dir,
                data_source=local_dir,
            )

    def test_hsd_by_name_raises_valueerror(self, tmp_path):
        """Passing 'hsd' as source name raises ValueError (local-only)."""
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with pytest.raises(ValueError, match="local-only"):
            build_vrt_mod.build_vrt(
                project_dir=project_dir,
                data_source="hsd",
            )

    def test_relative_path_raises(self, tmp_path):
        """Relative project_dir raises ValueError."""
        with pytest.raises(ValueError, match="absolute path"):
            build_vrt_mod.build_vrt(
                project_dir="relative/path",
                data_source="bluetopo",
            )

    def test_missing_project_dir_raises(self, tmp_path):
        """Non-existent project_dir raises ValueError."""
        with pytest.raises(ValueError, match="Folder path not found"):
            build_vrt_mod.build_vrt(
                project_dir=str(tmp_path / "nonexistent"),
                data_source="bluetopo",
            )

    def test_missing_registry_db_raises(self, tmp_path):
        """Project dir without registry DB raises ValueError."""
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)

        with pytest.raises(ValueError, match="database not found"):
            build_vrt_mod.build_vrt(
                project_dir=project_dir,
                data_source="bluetopo",
            )

    def test_missing_tile_folder_raises(self, tmp_path):
        """Project dir without tile download folder raises ValueError."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir)
        # Create DB but not the tile folder
        from nbs.noaabathymetry._internal.db import connect as connect_to_survey_registry
        conn = connect_to_survey_registry(project_dir, cfg)
        conn.close()

        with pytest.raises(ValueError, match="Tile downloads folder not found"):
            build_vrt_mod.build_vrt(
                project_dir=project_dir,
                data_source="bluetopo",
            )

    def test_unknown_gpkg_uses_local_config(self, tmp_path):
        """Unknown source name in gpkg uses get_local_config with resolved name."""
        local_dir = _create_local_dir(tmp_path, ["Weird_Tile_Scheme.gpkg"])
        # Resolved name is "Weird", so main() checks for weird_registry.db
        # and Weird/ tile folder. We must create those.
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)
        # Create DB with filename matching resolved name
        db_path = os.path.join(project_dir, "weird_registry.db")
        with open(db_path, "w") as f:
            f.write("")
        os.makedirs(os.path.join(project_dir, "Weird"), exist_ok=True)

        with mock.patch.object(build_vrt_mod, "connect") as mock_conn, \
             mock.patch.object(build_vrt_mod, "missing_utms", return_value=[]), \
             mock.patch.object(build_vrt_mod, "select_unbuilt_utms", return_value=[]):
            mock_conn.return_value = mock.MagicMock()
            build_vrt_mod.build_vrt(
                project_dir=project_dir,
                data_source=local_dir,
            )
            call_cfg = mock_conn.call_args[0][1]
            assert call_cfg["canonical_name"] == "Weird"
            assert call_cfg["geom_prefix"] is None
            assert len(call_cfg["rat_fields"]) == len(KNOWN_RAT_FIELDS)
