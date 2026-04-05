"""Tests for nbs.noaabathymetry.library.cleanup (no network, no S3)."""

import json
import os
import sqlite3
from unittest import mock

import pytest

from nbs.noaabathymetry._internal.config import get_config
from nbs.noaabathymetry._internal.download import all_db_tiles
from nbs.noaabathymetry.library.cleanup import (
    CleanupResult,
    clean_removed_from_scheme,
    _try_delete_garbage_files,
    _is_file_referenced,
    _reset_utms,
    _ensure_garbage_table,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tile_files(tmp_path, tile):
    """Create empty files on disk for a tile's disk fields."""
    for key in ("geotiff_disk", "rat_disk", "file_disk"):
        path = tile.get(key)
        if path:
            abs_path = os.path.join(str(tmp_path), path)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            open(abs_path, "w").close()


def _bt_tile(name, utm="18", res="4m"):
    """Build a BlueTopo tile dict for DB insertion."""
    return {
        "tilename": name,
        "delivered_date": "2024-06-01",
        "resolution": res,
        "utm": utm,
        "geotiff_disk": f"BlueTopo/UTM{utm}/{name}.tiff",
        "geotiff_verified": 1,
        "rat_disk": f"BlueTopo/UTM{utm}/{name}.tiff.aux.xml",
        "rat_verified": 1,
    }


def _open_db(project_dir):
    """Open a read connection to the registry DB."""
    conn = sqlite3.connect(
        os.path.join(project_dir, "bluetopo_registry.db"))
    conn.row_factory = sqlite3.Row
    return conn


def _tilenames(entries):
    """Extract tilenames from a list of result dicts."""
    return [e["tilename"] for e in entries]


# ---------------------------------------------------------------------------
# _is_file_referenced
# ---------------------------------------------------------------------------


class TestIsFileReferenced:
    def test_referenced(self, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1")
        conn, _ = registry_db(cfg, tiles=[tile])
        disk_fields = ["geotiff_disk", "rat_disk"]
        assert _is_file_referenced(conn, tile["geotiff_disk"], disk_fields) is True
        conn.close()

    def test_not_referenced(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, tiles=[_bt_tile("T1")])
        disk_fields = ["geotiff_disk", "rat_disk"]
        assert _is_file_referenced(conn, "nonexistent/path.tiff", disk_fields) is False
        conn.close()


# ---------------------------------------------------------------------------
# _try_delete_garbage_files
# ---------------------------------------------------------------------------


class TestTryDeleteGarbageFiles:
    def test_deletes_all_files(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        disk_fields = ["geotiff_disk", "rat_disk"]

        files = ["BlueTopo/UTM18/T1.tiff", "BlueTopo/UTM18/T1.tiff.aux.xml"]
        for f in files:
            abs_path = os.path.join(project_dir, f)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            open(abs_path, "w").close()

        assert _try_delete_garbage_files(files, project_dir, conn, disk_fields) is True
        for f in files:
            assert not os.path.isfile(os.path.join(project_dir, f))
        conn.close()

    def test_skips_referenced_files(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        _make_tile_files(tmp_path, tile)
        disk_fields = ["geotiff_disk", "rat_disk"]

        files = [tile["geotiff_disk"], tile["rat_disk"]]
        assert _try_delete_garbage_files(files, project_dir, conn, disk_fields) is True
        assert os.path.isfile(os.path.join(project_dir, tile["geotiff_disk"]))
        conn.close()

    def test_files_already_gone(self, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        disk_fields = ["geotiff_disk", "rat_disk"]
        files = ["BlueTopo/UTM18/gone.tiff"]
        assert _try_delete_garbage_files(files, project_dir, conn, disk_fields) is True
        conn.close()

    def test_fails_when_locked(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        disk_fields = ["geotiff_disk", "rat_disk"]

        files = ["BlueTopo/UTM18/locked.tiff"]
        abs_path = os.path.join(project_dir, files[0])
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        open(abs_path, "w").close()

        original_open = open
        def mock_open_fn(path, mode="r", *a, **kw):
            if path == abs_path and mode == "a":
                raise PermissionError("locked")
            return original_open(path, mode, *a, **kw)

        with mock.patch("builtins.open", side_effect=mock_open_fn):
            assert _try_delete_garbage_files(files, project_dir, conn, disk_fields) is False
        assert os.path.isfile(abs_path)
        conn.close()


# ---------------------------------------------------------------------------
# _reset_utms
# ---------------------------------------------------------------------------


class TestResetUtms:
    def test_resets_built_flags(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[
            {"utm": "18", "built": 1, "built_hillshade": 1},
            {"utm": "19", "built": 1, "built_hillshade": 0},
        ])
        _reset_utms(conn, {"18"}, cfg)
        conn.commit()

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mosaic_utm WHERE utm = '18'")
        assert dict(cursor.fetchone())["built"] == 0

        cursor.execute("SELECT * FROM mosaic_utm WHERE utm = '19'")
        assert dict(cursor.fetchone())["built"] == 1
        conn.close()

    def test_noop_when_empty(self, registry_db):
        cfg = get_config("bluetopo")
        conn, _ = registry_db(cfg, utms=[{"utm": "18", "built": 1}])
        _reset_utms(conn, set(), cfg)
        conn.commit()

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mosaic_utm WHERE utm = '18'")
        assert dict(cursor.fetchone())["built"] == 1
        conn.close()


# ---------------------------------------------------------------------------
# clean_removed_from_scheme — Phase 2
# ---------------------------------------------------------------------------


class TestCleanRemovedFromScheme:
    def test_deletes_tile_not_in_remote(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()
        _make_tile_files(tmp_path, tile)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.removed_from_scheme) == ["T1"]
        assert result.removed_from_scheme[0]["files"] == [
            tile["geotiff_disk"], tile["rat_disk"]]
        assert result.marked_for_deletion == []
        assert not os.path.isfile(os.path.join(project_dir, tile["geotiff_disk"]))
        assert not os.path.isfile(os.path.join(project_dir, tile["rat_disk"]))

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 0
        conn.close()

    def test_keeps_tile_in_remote(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()
        _make_tile_files(tmp_path, tile)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T1": {"tile": "T1"}})

        assert result.removed_from_scheme == []
        assert os.path.isfile(os.path.join(project_dir, tile["geotiff_disk"]))

    def test_marks_when_file_locked(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()
        _make_tile_files(tmp_path, tile)

        tiff_path = os.path.join(project_dir, tile["geotiff_disk"])
        original_open = open
        def mock_open_fn(path, mode="r", *a, **kw):
            if path == tiff_path and mode == "a":
                raise PermissionError("locked")
            return original_open(path, mode, *a, **kw)

        with mock.patch("builtins.open", side_effect=mock_open_fn):
            result = clean_removed_from_scheme(
                project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.marked_for_deletion) == ["T1"]
        assert os.path.isfile(tiff_path)

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 1
        conn.close()

    def test_resets_utm_built_flags(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg,
            tiles=[tile],
            utms=[{"utm": "18", "built": 1, "built_hillshade": 1}])
        conn.close()
        _make_tile_files(tmp_path, tile)

        clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        conn = _open_db(project_dir)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM mosaic_utm WHERE utm = '18'")
        row = dict(cursor.fetchone())
        assert row["built"] == 0
        assert row["built_hillshade"] == 0
        conn.close()

    def test_mixed_keep_and_remove(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        t1 = _bt_tile("T1", utm="18")
        t2 = _bt_tile("T2", utm="18")
        t3 = _bt_tile("T3", utm="19")
        conn, project_dir = registry_db(cfg, tiles=[t1, t2, t3])
        conn.close()
        for t in (t1, t2, t3):
            _make_tile_files(tmp_path, t)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T1": {"tile": "T1"}})

        assert sorted(_tilenames(result.removed_from_scheme)) == ["T2", "T3"]

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0] == 1
        conn.close()

    def test_no_tiles_to_remove(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T1": {"tile": "T1"}})

        assert result == CleanupResult()

    def test_accepts_local_tiles(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        local_tiles = all_db_tiles(conn)
        conn.close()
        _make_tile_files(tmp_path, tile)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={}, local_tiles=local_tiles)

        assert _tilenames(result.removed_from_scheme) == ["T1"]

    def test_tile_with_no_files(self, tmp_path, registry_db):
        """Tile that was never downloaded (null disk paths)."""
        cfg = get_config("bluetopo")
        tile = {"tilename": "T1", "utm": "18", "delivered_date": "2024-01-01"}
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.removed_from_scheme) == ["T1"]
        assert result.removed_from_scheme[0]["files"] == []

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 0
        conn.close()


# ---------------------------------------------------------------------------
# clean_removed_from_scheme — Phase 1: garbage collection
# ---------------------------------------------------------------------------


class TestGarbageCollection:
    def test_collects_previously_stored(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        _ensure_garbage_table(conn)

        files = ["BlueTopo/UTM18/OLD.tiff", "BlueTopo/UTM18/OLD.tiff.aux.xml"]
        for f in files:
            abs_path = os.path.join(project_dir, f)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            open(abs_path, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("OLD", json.dumps(files)))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.garbage_collected) == ["OLD"]
        assert result.garbage_collected[0]["files"] == files
        for f in files:
            assert not os.path.isfile(os.path.join(project_dir, f))

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 0
        conn.close()

    def test_still_locked(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        _ensure_garbage_table(conn)

        files = ["BlueTopo/UTM18/LOCKED.tiff"]
        abs_path = os.path.join(project_dir, files[0])
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        open(abs_path, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("LOCKED", json.dumps(files)))
        conn.commit()
        conn.close()

        original_open = open
        def mock_open_fn(path, mode="r", *a, **kw):
            if path == abs_path and mode == "a":
                raise PermissionError("locked")
            return original_open(path, mode, *a, **kw)

        with mock.patch("builtins.open", side_effect=mock_open_fn):
            result = clean_removed_from_scheme(
                project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.garbage_remaining) == ["LOCKED"]
        assert result.garbage_remaining[0]["files"] == files

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 1
        conn.close()

    def test_files_already_deleted_externally(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        _ensure_garbage_table(conn)

        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("GONE", json.dumps(["BlueTopo/UTM18/gone.tiff"])))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.garbage_collected) == ["GONE"]

    def test_skips_files_re_added_to_tiles(self, tmp_path, registry_db):
        """NBS re-added tile with same files — don't delete them."""
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        _ensure_garbage_table(conn)
        _make_tile_files(tmp_path, tile)

        files = [tile["geotiff_disk"], tile["rat_disk"]]
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("T1", json.dumps(files)))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T1": {"tile": "T1"}})

        assert _tilenames(result.garbage_collected) == ["T1"]
        assert os.path.isfile(os.path.join(project_dir, tile["geotiff_disk"]))

    def test_deletes_old_files_when_new_version_fetched(self, tmp_path, registry_db):
        """NBS re-added tile with new version — old files are stale."""
        cfg = get_config("bluetopo")
        new_tile = _bt_tile("T1", utm="18")
        new_tile["geotiff_disk"] = "BlueTopo/UTM18/T1_NEW.tiff"
        new_tile["rat_disk"] = "BlueTopo/UTM18/T1_NEW.tiff.aux.xml"
        conn, project_dir = registry_db(cfg, tiles=[new_tile])
        _ensure_garbage_table(conn)

        old_files = ["BlueTopo/UTM18/T1_OLD.tiff", "BlueTopo/UTM18/T1_OLD.tiff.aux.xml"]
        for f in old_files:
            abs_path = os.path.join(project_dir, f)
            os.makedirs(os.path.dirname(abs_path), exist_ok=True)
            open(abs_path, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("T1", json.dumps(old_files)))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T1": {"tile": "T1"}})

        assert _tilenames(result.garbage_collected) == ["T1"]
        for f in old_files:
            assert not os.path.isfile(os.path.join(project_dir, f))

    def test_phase1_then_phase2(self, tmp_path, registry_db):
        """Phase 1 cleans old garbage, Phase 2 finds new removal."""
        cfg = get_config("bluetopo")
        new_tile = _bt_tile("NEW", utm="19")
        conn, project_dir = registry_db(cfg, tiles=[new_tile])
        _ensure_garbage_table(conn)
        _make_tile_files(tmp_path, new_tile)

        old_files = ["BlueTopo/UTM18/OLD.tiff"]
        abs_path = os.path.join(project_dir, old_files[0])
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        open(abs_path, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("OLD", json.dumps(old_files)))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.garbage_collected) == ["OLD"]
        assert _tilenames(result.removed_from_scheme) == ["NEW"]

    def test_double_removal(self, tmp_path, registry_db):
        """Same tile removed twice creates two garbage rows."""
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        _ensure_garbage_table(conn)

        old_files = ["BlueTopo/UTM18/T1_V1.tiff"]
        abs_old = os.path.join(project_dir, old_files[0])
        os.makedirs(os.path.dirname(abs_old), exist_ok=True)
        open(abs_old, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("T1", json.dumps(old_files)))

        new_files = ["BlueTopo/UTM18/T1_V2.tiff"]
        abs_new = os.path.join(project_dir, new_files[0])
        open(abs_new, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("T1", json.dumps(new_files)))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.garbage_collected) == ["T1", "T1"]
        assert not os.path.isfile(abs_old)
        assert not os.path.isfile(abs_new)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestCleanupEdgeCases:
    def test_no_registry_db_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Registry database not found"):
            clean_removed_from_scheme(str(tmp_path), data_source="bluetopo",
                                     remote_tiles={})

    def test_empty_project(self, tmp_path, registry_db):
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert result == CleanupResult()

    def test_bag_source(self, tmp_path, registry_db):
        """Works with single-file-slot sources (BAG)."""
        cfg = get_config("bag")
        tile = {
            "tilename": "N1",
            "delivered_date": "2024-01-01",
            "resolution": "4m",
            "utm": "18",
            "file_disk": "BAG/UTM18/N1.bag",
            "file_verified": 1,
        }
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()

        abs_path = os.path.join(project_dir, tile["file_disk"])
        os.makedirs(os.path.dirname(abs_path), exist_ok=True)
        open(abs_path, "w").close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bag", remote_tiles={})

        assert _tilenames(result.removed_from_scheme) == ["N1"]
        assert result.removed_from_scheme[0]["files"] == [tile["file_disk"]]
        assert not os.path.isfile(abs_path)

    def test_partial_reference_in_garbage(self, tmp_path, registry_db):
        """One file referenced by active tile, other not — deletes unreferenced only."""
        cfg = get_config("bluetopo")
        # Active tile references only the tiff path (same path as garbage)
        active = _bt_tile("T1", utm="18")
        active["rat_disk"] = "BlueTopo/UTM18/T1_NEW.tiff.aux.xml"  # different aux
        conn, project_dir = registry_db(cfg, tiles=[active])
        _ensure_garbage_table(conn)
        _make_tile_files(tmp_path, active)

        # Garbage has old aux (unreferenced) + same tiff (referenced)
        old_aux = "BlueTopo/UTM18/T1_OLD.tiff.aux.xml"
        abs_old_aux = os.path.join(project_dir, old_aux)
        os.makedirs(os.path.dirname(abs_old_aux), exist_ok=True)
        open(abs_old_aux, "w").close()

        garbage_files = [active["geotiff_disk"], old_aux]
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("T1", json.dumps(garbage_files)))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T1": {"tile": "T1"}})

        # Entry collected: tiff skipped (referenced), old aux deleted
        assert _tilenames(result.garbage_collected) == ["T1"]
        assert os.path.isfile(os.path.join(project_dir, active["geotiff_disk"]))
        assert not os.path.isfile(abs_old_aux)

    def test_multiple_utm_zones_reset(self, tmp_path, registry_db):
        """Removing tiles from multiple UTMs resets all affected zones."""
        cfg = get_config("bluetopo")
        t1 = _bt_tile("T1", utm="17")
        t2 = _bt_tile("T2", utm="18")
        t3 = _bt_tile("T3", utm="19")
        conn, project_dir = registry_db(cfg,
            tiles=[t1, t2, t3],
            utms=[
                {"utm": "17", "built": 1},
                {"utm": "18", "built": 1},
                {"utm": "19", "built": 1},
            ])
        conn.close()
        for t in (t1, t2, t3):
            _make_tile_files(tmp_path, t)

        # Remove T1 (utm17) and T3 (utm19), keep T2 (utm18)
        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo",
            remote_tiles={"T2": {"tile": "T2"}})

        assert sorted(_tilenames(result.removed_from_scheme)) == ["T1", "T3"]

        conn = _open_db(project_dir)
        cursor = conn.cursor()
        # UTM 17 and 19 should be reset
        cursor.execute("SELECT built FROM mosaic_utm WHERE utm = '17'")
        assert cursor.fetchone()["built"] == 0
        cursor.execute("SELECT built FROM mosaic_utm WHERE utm = '19'")
        assert cursor.fetchone()["built"] == 0
        # UTM 18 should be untouched
        cursor.execute("SELECT built FROM mosaic_utm WHERE utm = '18'")
        assert cursor.fetchone()["built"] == 1
        conn.close()

    def test_multiple_tiles_same_utm(self, tmp_path, registry_db):
        """Multiple tiles in same UTM — UTM reset happens once."""
        cfg = get_config("bluetopo")
        t1 = _bt_tile("T1", utm="18")
        t2 = _bt_tile("T2", utm="18")
        conn, project_dir = registry_db(cfg,
            tiles=[t1, t2],
            utms=[{"utm": "18", "built": 1}])
        conn.close()
        _make_tile_files(tmp_path, t1)
        _make_tile_files(tmp_path, t2)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert sorted(_tilenames(result.removed_from_scheme)) == ["T1", "T2"]

        conn = _open_db(project_dir)
        cursor = conn.cursor()
        cursor.execute("SELECT built FROM mosaic_utm WHERE utm = '18'")
        assert cursor.fetchone()["built"] == 0
        # Only one UTM zone was affected
        assert conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0] == 0
        conn.close()

    def test_garbage_entry_empty_files_list(self, tmp_path, registry_db):
        """Garbage entry with empty files list is collected immediately."""
        cfg = get_config("bluetopo")
        conn, project_dir = registry_db(cfg)
        _ensure_garbage_table(conn)

        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("EMPTY", json.dumps([])))
        conn.commit()
        conn.close()

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert _tilenames(result.garbage_collected) == ["EMPTY"]
        assert result.garbage_collected[0]["files"] == []

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 0
        conn.close()

    def test_phase1_locked_phase2_same_tilename(self, tmp_path, registry_db):
        """Phase 1 has locked entry for T1, Phase 2 removes a new T1.
        The ID range filter ensures Phase 2 only processes its own entries."""
        cfg = get_config("bluetopo")
        # Active tile T1 (will be removed in Phase 2)
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        _ensure_garbage_table(conn)
        _make_tile_files(tmp_path, tile)

        # Old garbage entry for T1 with locked files
        old_files = ["BlueTopo/UTM18/T1_OLD.tiff"]
        abs_old = os.path.join(project_dir, old_files[0])
        os.makedirs(os.path.dirname(abs_old), exist_ok=True)
        open(abs_old, "w").close()
        conn.execute(
            "INSERT INTO garbage_tiles(tilename, files) VALUES(?, ?)",
            ("T1", json.dumps(old_files)))
        conn.commit()
        conn.close()

        # Lock old file so Phase 1 can't delete it
        original_open = open
        def mock_open_fn(path, mode="r", *a, **kw):
            if path == abs_old and mode == "a":
                raise PermissionError("locked")
            return original_open(path, mode, *a, **kw)

        with mock.patch("builtins.open", side_effect=mock_open_fn):
            result = clean_removed_from_scheme(
                project_dir, data_source="bluetopo", remote_tiles={})

        # Phase 1: old entry still locked
        assert _tilenames(result.garbage_remaining) == ["T1"]
        # Phase 2: new T1 removed from scheme, files deleted
        assert _tilenames(result.removed_from_scheme) == ["T1"]
        # Old locked file stays, new files deleted
        assert os.path.isfile(abs_old)
        assert not os.path.isfile(os.path.join(project_dir, tile["geotiff_disk"]))

    def test_remote_tiles_from_s3(self, tmp_path, registry_db):
        """When remote_tiles is None, downloads from S3 (mocked)."""
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()
        _make_tile_files(tmp_path, tile)

        # Mock _read_remote_geopackage to return empty (T1 not in scheme)
        with mock.patch(
            "nbs.noaabathymetry.library.cleanup._read_remote_geopackage",
            return_value={},
        ) as mock_remote:
            result = clean_removed_from_scheme(
                project_dir, data_source="bluetopo")
            mock_remote.assert_called_once()

        assert _tilenames(result.removed_from_scheme) == ["T1"]

    def test_result_dict_structure(self, tmp_path, registry_db):
        """Verify each result entry has tilename and files keys."""
        cfg = get_config("bluetopo")
        tile = _bt_tile("T1", utm="18")
        conn, project_dir = registry_db(cfg, tiles=[tile])
        conn.close()
        _make_tile_files(tmp_path, tile)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert len(result.removed_from_scheme) == 1
        entry = result.removed_from_scheme[0]
        assert "tilename" in entry
        assert "files" in entry
        assert entry["tilename"] == "T1"
        assert isinstance(entry["files"], list)
        assert all(isinstance(f, str) for f in entry["files"])

    def test_large_batch_removal(self, tmp_path, registry_db):
        """Remove 50 tiles across multiple UTMs."""
        cfg = get_config("bluetopo")
        tiles = []
        for i in range(50):
            utm = str(17 + (i % 3))
            tiles.append(_bt_tile(f"T{i}", utm=utm))

        conn, project_dir = registry_db(cfg, tiles=tiles)
        conn.close()
        for t in tiles:
            _make_tile_files(tmp_path, t)

        result = clean_removed_from_scheme(
            project_dir, data_source="bluetopo", remote_tiles={})

        assert len(result.removed_from_scheme) == 50
        assert result.marked_for_deletion == []

        conn = _open_db(project_dir)
        assert conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM garbage_tiles").fetchone()[0] == 0
        conn.close()
