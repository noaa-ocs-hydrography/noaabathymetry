"""Tests for download functions in fetch_tiles.py (uses moto for S3 mocking)."""

import datetime
import hashlib
import os
import time
from unittest import mock

import boto3
import pytest
from moto import mock_aws
from osgeo import gdal

from nbs.bluetopo.core.datasource import get_config, DATA_SOURCES
from nbs.bluetopo.core.build_vrt import connect_to_survey_registry
from nbs.bluetopo.core.fetch_tiles import (
    get_tessellation,
    get_xml,
    _get_s3_client,
    all_db_tiles,
)
import nbs.bluetopo.core.fetch_tiles as fetch_tiles_module

BUCKET = "noaa-ocs-nationalbathymetry-pds"


def _mock_s3_client():
    """Return a plain boto3 S3 client compatible with moto."""
    return boto3.client("s3", region_name="us-east-1")


# ---------------------------------------------------------------------------
# _get_s3_client
# ---------------------------------------------------------------------------


class TestGetS3Client:
    @mock_aws
    def test_returns_client(self):
        client = _get_s3_client()
        assert client is not None


# ---------------------------------------------------------------------------
# get_tessellation
# ---------------------------------------------------------------------------


class TestGetTessellation:
    @mock_aws
    def test_s3_download(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        key = "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme.gpkg"
        client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=b"fake gpkg content",
        )

        result = get_tessellation(
            conn, project_dir,
            "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme",
            "BlueTopo", cfg,
        )
        assert result is not None
        assert os.path.isfile(result)

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM tileset WHERE tilescheme = 'Tessellation'")
        row = cursor.fetchone()
        assert row is not None

    def test_local_source(self, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)
        conn = connect_to_survey_registry(project_dir, cfg)

        local_dir = str(tmp_path / "local")
        os.makedirs(local_dir)
        gpkg_path = os.path.join(local_dir, "Test_Tile_Scheme.gpkg")
        with open(gpkg_path, "w") as f:
            f.write("fake content")

        result = get_tessellation(
            conn, project_dir, local_dir,
            "CustomSource", cfg,
            local_dir=local_dir,
        )
        assert result is not None
        assert os.path.isfile(result)

    @mock_aws
    def test_not_found_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        result = get_tessellation(
            conn, project_dir,
            "NonExistent/Prefix",
            "BlueTopo", cfg,
        )
        assert result is None

    @mock_aws
    def test_replaces_old_tessellation(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        key = "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme.gpkg"
        client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=b"first version",
        )

        result1 = get_tessellation(
            conn, project_dir,
            "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme",
            "BlueTopo", cfg,
        )
        assert os.path.isfile(result1)

        client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=b"second version",
        )
        result2 = get_tessellation(
            conn, project_dir,
            "BlueTopo/_BlueTopo_Tile_Scheme/BlueTopo_Tile_Scheme",
            "BlueTopo", cfg,
        )
        assert os.path.isfile(result2)


# ---------------------------------------------------------------------------
# get_xml
# ---------------------------------------------------------------------------


class TestGetXml:
    @mock_aws
    def test_downloads_and_renames(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("s102v21")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        key = "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V21/_CATALOG/exchange_catalogue.xml"
        client.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=b"<xml>catalog</xml>",
        )

        result = get_xml(
            conn, project_dir,
            "Test-and-Evaluation/Navigation_Test_and_Evaluation/S102V21/_CATALOG",
            "S102V21", cfg,
        )
        assert result is not None
        assert result.endswith("CATALOG.XML")
        assert os.path.isfile(result)

    @mock_aws
    def test_not_found_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("s102v21")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        result = get_xml(
            conn, project_dir,
            "NonExistent/Prefix",
            "S102V21", cfg,
        )
        assert result is None


# ---------------------------------------------------------------------------
# get_tessellation local edge cases
# ---------------------------------------------------------------------------


class TestGetTessellationLocal:
    def test_local_source_no_gpkg_returns_none(self, tmp_path):
        """Local dir with no gpkg files returns None."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)
        conn = connect_to_survey_registry(project_dir, cfg)

        local_dir = str(tmp_path / "empty_local")
        os.makedirs(local_dir)

        result = get_tessellation(
            conn, project_dir, local_dir,
            "CustomUnknown", cfg,
            local_dir=local_dir,
        )
        assert result is None

    def test_local_source_multiple_gpkgs(self, tmp_path):
        """Local dir with multiple gpkgs picks most recent (sorted reverse)."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)
        conn = connect_to_survey_registry(project_dir, cfg)

        local_dir = str(tmp_path / "local")
        os.makedirs(local_dir)
        for name in ["AAA_Tile_Scheme.gpkg", "ZZZ_Tile_Scheme.gpkg"]:
            with open(os.path.join(local_dir, name), "w") as f:
                f.write("fake")

        result = get_tessellation(
            conn, project_dir, local_dir,
            "CustomUnknown", cfg,
            local_dir=local_dir,
        )
        assert result is not None
        # Sorted reverse alphabetically: ZZZ is first
        assert "ZZZ_Tile_Scheme.gpkg" in result

    def test_local_removes_old_tessellation(self, tmp_path):
        """Second local tessellation download removes the first."""
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path / "project")
        os.makedirs(project_dir, exist_ok=True)
        conn = connect_to_survey_registry(project_dir, cfg)

        local_dir = str(tmp_path / "local")
        os.makedirs(local_dir)
        gpkg_path = os.path.join(local_dir, "Test_Tile_Scheme.gpkg")
        with open(gpkg_path, "w") as f:
            f.write("v1")

        result1 = get_tessellation(
            conn, project_dir, local_dir,
            "CustomSource", cfg,
            local_dir=local_dir,
        )
        assert os.path.isfile(result1)

        with open(gpkg_path, "w") as f:
            f.write("v2")

        result2 = get_tessellation(
            conn, project_dir, local_dir,
            "CustomSource", cfg,
            local_dir=local_dir,
        )
        assert os.path.isfile(result2)


# ---------------------------------------------------------------------------
# get_tessellation S3 – multiple objects & timestamp ordering
# ---------------------------------------------------------------------------


class TestGetTessellationS3MultipleObjects:
    """Test how get_tessellation handles multiple gpkg objects on S3."""

    @mock_aws
    def test_s3_picks_most_recently_modified(self, tmp_path, monkeypatch):
        """With multiple S3 objects under the same prefix, the most recently
        modified one (by LastModified) is downloaded."""
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        prefix = cfg["geom_prefix"]

        # Upload "old" version first
        client.put_object(Bucket=BUCKET, Key=f"{prefix}_old.gpkg", Body=b"old")
        time.sleep(1.1)
        # Upload "new" version second (higher LastModified)
        client.put_object(Bucket=BUCKET, Key=f"{prefix}_new.gpkg", Body=b"new")

        result = get_tessellation(conn, project_dir, prefix, "BlueTopo", cfg)
        assert result is not None
        # The newest file should be picked
        assert "_new.gpkg" in os.path.basename(result)

    @mock_aws
    def test_s3_multiple_versions_downloads_latest_content(self, tmp_path, monkeypatch):
        """Uploading a newer version of the same key replaces content."""
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        key = f"{cfg['geom_prefix']}.gpkg"

        client.put_object(Bucket=BUCKET, Key=key, Body=b"version1")
        result1 = get_tessellation(conn, project_dir, cfg["geom_prefix"], "BlueTopo", cfg)
        assert os.path.isfile(result1)
        with open(result1, "rb") as f:
            assert f.read() == b"version1"

        # Re-upload with new content (simulates S3 update)
        client.put_object(Bucket=BUCKET, Key=key, Body=b"version2")
        result2 = get_tessellation(conn, project_dir, cfg["geom_prefix"], "BlueTopo", cfg)
        assert os.path.isfile(result2)
        with open(result2, "rb") as f:
            assert f.read() == b"version2"

    @mock_aws
    def test_s3_single_object_works(self, tmp_path, monkeypatch):
        """Single object under prefix works normally."""
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        key = f"{cfg['geom_prefix']}.gpkg"
        client.put_object(Bucket=BUCKET, Key=key, Body=b"only one")

        result = get_tessellation(conn, project_dir, cfg["geom_prefix"], "BlueTopo", cfg)
        assert result is not None
        assert os.path.isfile(result)


class TestGetTessellationS3AllSources:
    """Test get_tessellation S3 download for each data source with an S3 prefix."""

    @staticmethod
    def _s3_sources():
        """Return (name, cfg) for all data sources that have S3 geom_prefix."""
        return [
            (name, cfg) for name, cfg in DATA_SOURCES.items()
            if cfg["geom_prefix"] is not None
        ]

    @mock_aws
    def test_each_s3_source_downloads_single(self, tmp_path, monkeypatch):
        """Each S3-backed data source can download a single tile scheme gpkg."""
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        for name, cfg in self._s3_sources():
            project_dir = str(tmp_path / name)
            os.makedirs(project_dir, exist_ok=True)
            conn = connect_to_survey_registry(project_dir, cfg)

            key = f"{cfg['geom_prefix']}.gpkg"
            client.put_object(Bucket=BUCKET, Key=key, Body=b"fake gpkg")

            result = get_tessellation(
                conn, project_dir, cfg["geom_prefix"],
                cfg["canonical_name"], cfg,
            )
            assert result is not None, f"{name}: expected a file path"
            assert os.path.isfile(result), f"{name}: file not on disk"
            conn.close()

    def test_each_s3_source_picks_latest_of_multiple(self, tmp_path, monkeypatch):
        """Each S3-backed source picks the most recently modified object."""
        for name, cfg in self._s3_sources():
            with mock_aws():
                monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
                client = boto3.client("s3", region_name="us-east-1")
                client.create_bucket(Bucket=BUCKET)

                project_dir = str(tmp_path / name)
                os.makedirs(project_dir, exist_ok=True)
                conn = connect_to_survey_registry(project_dir, cfg)

                prefix = cfg["geom_prefix"]
                # Older object
                client.put_object(Bucket=BUCKET, Key=f"{prefix}_v1.gpkg", Body=b"old")
                time.sleep(1.1)
                # Newer object
                client.put_object(Bucket=BUCKET, Key=f"{prefix}_v2.gpkg", Body=b"new")

                result = get_tessellation(
                    conn, project_dir, prefix,
                    cfg["canonical_name"], cfg,
                )
                assert result is not None, f"{name}: expected a file path"
                assert "_v2.gpkg" in os.path.basename(result), (
                    f"{name}: expected newest object, got {os.path.basename(result)}"
                )
                conn.close()

    @mock_aws
    def test_each_s3_source_empty_prefix_returns_none(self, tmp_path, monkeypatch):
        """Each S3-backed source returns None when prefix has no objects."""
        monkeypatch.setattr(fetch_tiles_module, "_get_s3_client", _mock_s3_client)
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)

        for name, cfg in self._s3_sources():
            project_dir = str(tmp_path / name)
            os.makedirs(project_dir, exist_ok=True)
            conn = connect_to_survey_registry(project_dir, cfg)

            result = get_tessellation(
                conn, project_dir, cfg["geom_prefix"],
                cfg["canonical_name"], cfg,
            )
            assert result is None, f"{name}: expected None for empty prefix"
            conn.close()


class TestGetTessellationLocalAllSources:
    """Test get_tessellation local path for each data source config."""

    @staticmethod
    def _all_configs():
        """Return all data source configs (including local-only ones)."""
        return list(DATA_SOURCES.items())

    def test_each_source_config_local_single_gpkg(self, tmp_path):
        """Each config can be used with a local directory containing one gpkg."""
        for name, cfg in self._all_configs():
            project_dir = str(tmp_path / f"{name}_proj")
            os.makedirs(project_dir, exist_ok=True)
            conn = connect_to_survey_registry(project_dir, cfg)

            local_dir = str(tmp_path / f"{name}_local")
            os.makedirs(local_dir, exist_ok=True)
            gpkg = os.path.join(local_dir, f"{name}_Tile_Scheme.gpkg")
            with open(gpkg, "w") as f:
                f.write("fake")

            # Use "LocalTest" as data_source and pass local_dir to take local code path
            result = get_tessellation(
                conn, project_dir, local_dir,
                "LocalTest", cfg,
                local_dir=local_dir,
            )
            assert result is not None, f"{name}: expected a file path"
            assert os.path.isfile(result), f"{name}: file not on disk"
            conn.close()

    def test_each_source_config_local_multiple_gpkgs_picks_reverse_sorted(self, tmp_path):
        """Each config with multiple local gpkgs picks reverse-sorted first."""
        for name, cfg in self._all_configs():
            project_dir = str(tmp_path / f"{name}_proj")
            os.makedirs(project_dir, exist_ok=True)
            conn = connect_to_survey_registry(project_dir, cfg)

            local_dir = str(tmp_path / f"{name}_local")
            os.makedirs(local_dir, exist_ok=True)
            for gpkg_name in ["AAA_Tile_Scheme.gpkg", "ZZZ_Tile_Scheme.gpkg"]:
                with open(os.path.join(local_dir, gpkg_name), "w") as f:
                    f.write("fake")

            result = get_tessellation(
                conn, project_dir, local_dir,
                "LocalTest", cfg,
                local_dir=local_dir,
            )
            assert result is not None, f"{name}: expected a file path"
            assert "ZZZ_Tile_Scheme.gpkg" in result, (
                f"{name}: expected ZZZ (reverse sort), got {os.path.basename(result)}"
            )
            conn.close()


# ---------------------------------------------------------------------------
# all_db_tiles
# ---------------------------------------------------------------------------


class TestAllDbTiles:
    def test_returns_dicts(self, tmp_path):
        cfg = get_config("bluetopo")
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO tiles(tilename, resolution) VALUES(?, ?)",
            ("T1", "2m"),
        )
        conn.commit()
        result = all_db_tiles(conn)
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert result[0]["tilename"] == "T1"
        assert result[0]["resolution"] == "2m"
