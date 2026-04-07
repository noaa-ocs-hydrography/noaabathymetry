"""Tests for nbs.noaabathymetry.library.verify."""

import hashlib
import json
import os

import pytest

from nbs.noaabathymetry._internal.config import get_config
from nbs.noaabathymetry.library.verify import (
    VerifyResult,
    verify_tiles,
    generate_manifest,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _bt_tile(name, tmp_path, utm="18", content=b"tile data"):
    """Create a BlueTopo tile with files on disk and matching checksums."""
    tiff_rel = f"BlueTopo_Data/{name}.tiff"
    rat_rel = f"BlueTopo_Data/{name}.tiff.aux.xml"
    tiff_abs = os.path.join(str(tmp_path), tiff_rel)
    rat_abs = os.path.join(str(tmp_path), rat_rel)
    os.makedirs(os.path.dirname(tiff_abs), exist_ok=True)
    with open(tiff_abs, "wb") as f:
        f.write(content)
    with open(rat_abs, "wb") as f:
        f.write(content + b"_rat")
    return {
        "tilename": name,
        "delivered_date": "2024-06-01",
        "resolution": "4m",
        "utm": utm,
        "geotiff_disk": tiff_rel,
        "geotiff_sha256_checksum": _sha256(tiff_abs),
        "geotiff_verified": 1,
        "geotiff_disk_file_size": os.path.getsize(tiff_abs),
        "rat_disk": rat_rel,
        "rat_sha256_checksum": _sha256(rat_abs),
        "rat_verified": 1,
        "rat_disk_file_size": os.path.getsize(rat_abs),
    }


# ---------------------------------------------------------------------------
# verify_tiles
# ---------------------------------------------------------------------------


class TestVerifyTiles:
    def test_all_verified(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        t1 = _bt_tile("T1", tmp_path)
        t2 = _bt_tile("T2", tmp_path)
        conn, project_dir = registry_db(cfg, tiles=[t1, t2])
        conn.close()

        result = verify_tiles(project_dir, "bluetopo")
        assert len(result.verified) == 2
        assert result.unverified == []
        assert result.missing_files == []
        assert result.checksum_mismatch == []

    def test_unverified_flag(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        tile["geotiff_verified"] = 0
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        result = verify_tiles(project_dir, "bluetopo")
        assert result.unverified == ["T1"]
        assert result.verified == []

    def test_missing_file(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        # Delete the tiff
        os.remove(os.path.join(str(tmp_path), tile["geotiff_disk"]))
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        result = verify_tiles(project_dir, "bluetopo")
        assert len(result.missing_files) == 1
        assert result.missing_files[0]["tilename"] == "T1"
        assert "geotiff_disk" in result.missing_files[0]["missing"]

    def test_checksum_mismatch(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        # Corrupt the file
        tiff_abs = os.path.join(str(tmp_path), tile["geotiff_disk"])
        with open(tiff_abs, "wb") as f:
            f.write(b"corrupted data")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        result = verify_tiles(project_dir, "bluetopo")
        assert len(result.checksum_mismatch) == 1
        assert result.checksum_mismatch[0]["tilename"] == "T1"
        assert result.checksum_mismatch[0]["expected"] == tile["geotiff_sha256_checksum"]

    def test_no_db_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Registry database not found"):
            verify_tiles(str(tmp_path), "bluetopo")

    def test_empty_project(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        conn.close()

        result = verify_tiles(project_dir, "bluetopo")
        assert result == VerifyResult()


# ---------------------------------------------------------------------------
# generate_manifest
# ---------------------------------------------------------------------------


class TestGenerateManifest:
    def test_basic_manifest(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        manifest = generate_manifest(project_dir, "bluetopo",
                                     include_mosaics=False)

        assert manifest["data_source"] == "BlueTopo"
        assert manifest["tile_count"] == 1
        assert manifest["mosaics_included"] is False
        assert isinstance(manifest["files"], list)
        assert len(manifest["files"]) >= 1  # at least the DB

        # Check DB is in manifest
        db_files = [f for f in manifest["files"]
                    if f["path"].endswith("_registry.db")]
        assert len(db_files) == 1

        # Check tile files are in manifest
        tile_files = [f for f in manifest["files"]
                      if f["path"].endswith(".tiff")]
        assert len(tile_files) == 1
        assert "sha256" in tile_files[0]
        assert "size" in tile_files[0]

    def test_manifest_has_version(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        conn.close()

        manifest = generate_manifest(project_dir, "bluetopo")
        assert "package_version" in manifest
        assert "exported_at" in manifest

    def test_no_db_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Registry database not found"):
            generate_manifest(str(tmp_path), "bluetopo")
