"""Tests for nbs.noaabathymetry.library.export."""

import hashlib
import json
import os
import zipfile

import pytest

from nbs.noaabathymetry._internal.config import get_config
from nbs.noaabathymetry.library.export import (
    ExportResult,
    export_project,
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
    tiff_rel = f"BlueTopo/UTM{utm}/{name}.tiff"
    rat_rel = f"BlueTopo/UTM{utm}/{name}.tiff.aux.xml"
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
# export_project
# ---------------------------------------------------------------------------


class TestExportProject:
    def test_basic_export(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        t1 = _bt_tile("T1", tmp_path)
        t2 = _bt_tile("T2", tmp_path)
        conn, project_dir = registry_db(cfg, tiles=[t1, t2])
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        result = export_project(project_dir, output, "bluetopo",
                                include_mosaics=False)

        assert result.tile_count == 2
        assert result.verification_passed is True
        assert result.mosaics_included is False
        assert os.path.isfile(output)
        assert result.zip_size > 0

        # Verify zip contents
        with zipfile.ZipFile(output) as zf:
            names = zf.namelist()
            assert "manifest.json" in names
            assert t1["geotiff_disk"] in names
            assert t1["rat_disk"] in names
            assert t2["geotiff_disk"] in names

            # Check manifest
            manifest = json.loads(zf.read("manifest.json"))
            assert manifest["tile_count"] == 2
            assert manifest["data_source"] == "BlueTopo"

    def test_no_partial_zip_on_failure(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        tile["geotiff_verified"] = 0  # will fail pre-flight
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="not verified"):
            export_project(project_dir, output, "bluetopo",
                           include_mosaics=False)

        # No zip file should exist
        assert not os.path.isfile(output)
        assert not os.path.isfile(output + ".tmp")

    def test_checksum_mismatch_errors(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        # Corrupt the file after creating tile dict
        tiff_abs = os.path.join(str(tmp_path), tile["geotiff_disk"])
        with open(tiff_abs, "wb") as f:
            f.write(b"corrupted")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="checksum mismatch"):
            export_project(project_dir, output, "bluetopo",
                           include_mosaics=False)

    def test_missing_file_errors(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        # Delete file after creating tile dict
        os.remove(os.path.join(str(tmp_path), tile["geotiff_disk"]))
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="missing from disk"):
            export_project(project_dir, output, "bluetopo",
                           include_mosaics=False)

    def test_s102v30_mosaics_error(self, tmp_path, registry_db):
        cfg = get_config("s102v30")
        conn, project_dir = registry_db(cfg)
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="non-portable"):
            export_project(project_dir, output, "s102v30",
                           include_mosaics=True)

    def test_s102v22_mosaics_error(self, tmp_path, registry_db):
        cfg = get_config("s102v22")
        conn, project_dir = registry_db(cfg)
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="non-portable"):
            export_project(project_dir, output, "s102v22",
                           include_mosaics=True)

    def test_s102v30_without_mosaics_ok(self, tmp_path, registry_db):
        cfg = get_config("s102v30")
        conn, project_dir = registry_db(cfg)
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        result = export_project(project_dir, output, "s102v30",
                                include_mosaics=False)
        assert result.mosaics_included is False
        assert os.path.isfile(output)

    def test_no_project_dir_errors(self, tmp_path):
        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="not found"):
            export_project("/nonexistent/path", output, "bluetopo")

    def test_no_db_errors(self, tmp_path):
        output = os.path.join(str(tmp_path), "export.zip")
        with pytest.raises(ValueError, match="Registry database not found"):
            export_project(str(tmp_path), output, "bluetopo")

    def test_manifest_in_zip(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", tmp_path)
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        output = os.path.join(str(tmp_path), "export.zip")
        export_project(project_dir, output, "bluetopo",
                       include_mosaics=False)

        with zipfile.ZipFile(output) as zf:
            manifest = json.loads(zf.read("manifest.json"))
            assert "package_version" in manifest
            assert "exported_at" in manifest
            assert "files" in manifest

            # Verify SHA-256 in manifest matches actual file in zip
            tile_files = [f for f in manifest["files"]
                          if "sha256" in f]
            assert len(tile_files) > 0
            for entry in tile_files:
                data = zf.read(entry["path"])
                actual = hashlib.sha256(data).hexdigest()
                assert actual == entry["sha256"]
