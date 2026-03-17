"""Tests for config.py configuration helpers."""

import re

import pytest
from osgeo import gdal

from nbs.bluetopo._internal.config import (
    DATA_SOURCES,
    KNOWN_RAT_FIELDS,
    VALID_TARGET_RESOLUTIONS,
    _timestamp,
    get_config,
    get_local_config,
    get_catalog_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_built_flags,
    get_utm_file_columns,
    get_disk_field,
    get_disk_fields,
    get_verified_fields,
    get_link_fields,
    get_checksum_fields,
    validate_config,
)

# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------


class TestGetConfig:
    def test_lowercase_lookup(self):
        cfg = get_config("bluetopo")
        assert cfg["canonical_name"] == "BlueTopo"

    def test_uppercase_lookup(self):
        cfg = get_config("BLUETOPO")
        assert cfg["canonical_name"] == "BlueTopo"

    def test_mixed_case_lookup(self):
        cfg = get_config("BlueTopo")
        assert cfg["canonical_name"] == "BlueTopo"

    def test_unknown_source_raises(self):
        with pytest.raises(ValueError, match="Unknown data source"):
            get_config("nonexistent")

    def test_returns_deep_copy(self):
        cfg1 = get_config("bluetopo")
        cfg1["canonical_name"] = "MUTATED"
        cfg2 = get_config("bluetopo")
        assert cfg2["canonical_name"] == "BlueTopo"

    def test_all_sources_accessible(self):
        for key in DATA_SOURCES:
            cfg = get_config(key)
            assert cfg["canonical_name"] is not None


# ---------------------------------------------------------------------------
# _timestamp
# ---------------------------------------------------------------------------


class TestTimestamp:
    def test_format(self):
        ts = _timestamp()
        # Expect pattern like "2024-01-15 10:30:45 EST"
        assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} \S+", ts)

    def test_returns_string(self):
        assert isinstance(_timestamp(), str)


# ---------------------------------------------------------------------------
# get_catalog_fields
# ---------------------------------------------------------------------------


class TestGetCatalogFields:
    def test_tileset_pk(self):
        cfg = get_config("bluetopo")
        fields = get_catalog_fields(cfg)
        assert "tilescheme" in fields
        assert fields["tilescheme"] == "text"
        assert "file" not in fields

    def test_catalog_pk(self):
        cfg = get_config("bag")
        fields = get_catalog_fields(cfg)
        assert "file" in fields
        assert fields["file"] == "text"
        assert "tilescheme" not in fields

    def test_common_fields(self):
        for src in ["bluetopo", "bag"]:
            cfg = get_config(src)
            fields = get_catalog_fields(cfg)
            assert "location" in fields
            assert "downloaded" in fields


# ---------------------------------------------------------------------------
# get_vrt_utm_fields
# ---------------------------------------------------------------------------


class TestGetVrtUtmFields:
    def test_single_dataset(self):
        cfg = get_config("bluetopo")
        fields = get_vrt_utm_fields(cfg)
        assert "utm_vrt" in fields
        assert "utm_ovr" in fields
        assert "built" in fields
        assert "utm" in fields

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset(self, source):
        cfg = get_config(source)
        fields = get_vrt_utm_fields(cfg)
        assert "utm_subdataset1_vrt" in fields
        assert "utm_subdataset2_vrt" in fields
        assert "utm_combined_vrt" in fields
        assert "built_subdataset1" in fields
        assert "built_subdataset2" in fields
        assert "built_combined" in fields
        assert "built" not in fields


# ---------------------------------------------------------------------------
# get_tiles_fields
# ---------------------------------------------------------------------------


class TestGetTilesFields:
    def test_dual_file_slots(self):
        cfg = get_config("bluetopo")
        fields = get_tiles_fields(cfg)
        assert "geotiff_disk" in fields
        assert "rat_disk" in fields
        assert "geotiff_link" in fields
        assert "rat_link" in fields
        assert "geotiff_verified" in fields
        assert "rat_verified" in fields

    def test_single_file_slot(self):
        cfg = get_config("bag")
        fields = get_tiles_fields(cfg)
        assert "file_disk" in fields
        assert "file_link" in fields
        assert "file_verified" in fields
        assert "geotiff_disk" not in fields

    def test_common_fields(self):
        for src in ["bluetopo", "bag"]:
            cfg = get_config(src)
            fields = get_tiles_fields(cfg)
            assert "tilename" in fields
            assert "delivered_date" in fields
            assert "resolution" in fields
            assert "utm" in fields

    def test_verified_fields_are_integer(self):
        """Verified flags use integer (0/1), not text."""
        for src in ["bluetopo", "bag", "s102v22"]:
            cfg = get_config(src)
            fields = get_tiles_fields(cfg)
            for slot in cfg["file_slots"]:
                col = f"{slot['name']}_verified"
                assert fields[col] == "integer", f"{src}: {col} should be integer"

    def test_sha256_checksum_columns(self):
        """Each file slot generates a sha256_checksum column."""
        cfg = get_config("bluetopo")
        fields = get_tiles_fields(cfg)
        assert "geotiff_sha256_checksum" in fields
        assert "rat_sha256_checksum" in fields


# ---------------------------------------------------------------------------
# get_built_flags
# ---------------------------------------------------------------------------


class TestGetBuiltFlags:
    def test_single_dataset(self):
        cfg = get_config("bluetopo")
        assert get_built_flags(cfg) == ["built"]

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset(self, source):
        cfg = get_config(source)
        flags = get_built_flags(cfg)
        assert flags == ["built_subdataset1", "built_subdataset2"]


# ---------------------------------------------------------------------------
# get_utm_file_columns
# ---------------------------------------------------------------------------


class TestUtmFileColumns:
    def test_utm_excludes_utm_built(self):
        cfg = get_config("bluetopo")
        cols = get_utm_file_columns(cfg)
        assert "utm" not in cols
        assert "built" not in cols
        assert "utm_vrt" in cols

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_utm_multi_subdataset(self, source):
        cfg = get_config(source)
        cols = get_utm_file_columns(cfg)
        assert "utm_subdataset1_vrt" in cols
        assert "utm_combined_vrt" in cols
        assert "built_subdataset1" not in cols
        assert "built_combined" not in cols


# ---------------------------------------------------------------------------
# get_disk_field / get_disk_fields / get_verified_fields
# ---------------------------------------------------------------------------


class TestDiskAndVerifiedFields:
    def test_disk_field_dual(self):
        cfg = get_config("bluetopo")
        assert get_disk_field(cfg) == "geotiff_disk"

    def test_disk_field_single(self):
        cfg = get_config("bag")
        assert get_disk_field(cfg) == "file_disk"

    def test_disk_fields_dual(self):
        cfg = get_config("bluetopo")
        assert get_disk_fields(cfg) == ["geotiff_disk", "rat_disk"]

    def test_disk_fields_single(self):
        cfg = get_config("bag")
        assert get_disk_fields(cfg) == ["file_disk"]

    def test_verified_dual(self):
        cfg = get_config("bluetopo")
        assert get_verified_fields(cfg) == ["geotiff_verified", "rat_verified"]

    def test_verified_single(self):
        cfg = get_config("bag")
        assert get_verified_fields(cfg) == ["file_verified"]


# ---------------------------------------------------------------------------
# get_link_fields / get_checksum_fields
# ---------------------------------------------------------------------------


class TestLinkAndChecksumFields:
    def test_link_fields_dual(self):
        cfg = get_config("bluetopo")
        assert get_link_fields(cfg) == ["geotiff_link", "rat_link"]

    def test_link_fields_single(self):
        cfg = get_config("bag")
        assert get_link_fields(cfg) == ["file_link"]

    def test_checksum_fields_dual(self):
        cfg = get_config("bluetopo")
        assert get_checksum_fields(cfg) == ["geotiff_sha256_checksum", "rat_sha256_checksum"]

    def test_checksum_fields_single(self):
        cfg = get_config("bag")
        assert get_checksum_fields(cfg) == ["file_sha256_checksum"]


# ---------------------------------------------------------------------------
# Config completeness
# ---------------------------------------------------------------------------


REQUIRED_KEYS = [
    "canonical_name", "min_gdal_version", "required_gdal_drivers",
    "geom_prefix", "xml_prefix", "bucket",
    "catalog_table", "catalog_pk",
    "gpkg_fields", "file_slots",
    "subdatasets", "band_descriptions",
    "has_rat", "rat_open_method", "rat_band", "rat_fields",
    "rat_zero_fields",
]


class TestConfigCompleteness:
    @pytest.mark.parametrize("source", list(DATA_SOURCES.keys()))
    def test_all_required_keys(self, source):
        cfg = get_config(source)
        for key in REQUIRED_KEYS:
            assert key in cfg, f"Missing key '{key}' in {source}"

    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "bag", "s102v21", "s102v22", "s102v30"])
    def test_remote_sources_have_geom_prefix(self, source):
        cfg = get_config(source)
        assert cfg["geom_prefix"] is not None

    def test_hsd_local_only(self):
        cfg = get_config("hsd")
        assert cfg["geom_prefix"] is None

    def test_hsd_extra_rat_fields(self):
        cfg = get_config("hsd")
        assert "catzoc" in cfg["rat_fields"]
        assert "supercession_score" in cfg["rat_fields"]
        assert "decay_score" in cfg["rat_fields"]
        assert "unqualified" in cfg["rat_fields"]
        assert "sensitive" in cfg["rat_fields"]


# ---------------------------------------------------------------------------
# File slots
# ---------------------------------------------------------------------------


class TestFileSlots:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_dual_file_slot_sources(self, source):
        cfg = get_config(source)
        assert len(cfg["file_slots"]) == 2
        names = [s["name"] for s in cfg["file_slots"]]
        assert names == ["geotiff", "rat"]

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_single_file_slot_sources(self, source):
        cfg = get_config(source)
        assert len(cfg["file_slots"]) == 1
        assert cfg["file_slots"][0]["name"] == "file"

    @pytest.mark.parametrize("source", list(DATA_SOURCES.keys()))
    def test_every_slot_has_name_and_gpkg_link(self, source):
        cfg = get_config(source)
        for slot in cfg["file_slots"]:
            assert "name" in slot
            assert "gpkg_link" in slot
            assert "gpkg_checksum" in slot

    def test_bluetopo_gpkg_link_values(self):
        cfg = get_config("bluetopo")
        assert cfg["file_slots"][0]["gpkg_link"] == "GeoTIFF_Link"
        assert cfg["file_slots"][0]["gpkg_checksum"] == "GeoTIFF_SHA256_Checksum"
        assert cfg["file_slots"][1]["gpkg_link"] == "RAT_Link"
        assert cfg["file_slots"][1]["gpkg_checksum"] == "RAT_SHA256_Checksum"

    def test_bag_gpkg_link_values(self):
        cfg = get_config("bag")
        assert cfg["file_slots"][0]["gpkg_link"] == "BAG"
        assert cfg["file_slots"][0]["gpkg_checksum"] == "BAG_SHA256"

    def test_s102v22_gpkg_link_values(self):
        cfg = get_config("s102v22")
        assert cfg["file_slots"][0]["gpkg_link"] == "S102V22"
        assert cfg["file_slots"][0]["gpkg_checksum"] == "S102V22_SHA256"

    def test_s102v30_gpkg_link_values(self):
        cfg = get_config("s102v30")
        assert cfg["file_slots"][0]["gpkg_link"] == "S102V30"
        assert cfg["file_slots"][0]["gpkg_checksum"] == "S102V30_SHA256"


# ---------------------------------------------------------------------------
# gpkg_fields
# ---------------------------------------------------------------------------


class TestGpkgFields:
    @pytest.mark.parametrize("source", list(DATA_SOURCES.keys()))
    def test_all_sources_have_required_gpkg_field_keys(self, source):
        cfg = get_config(source)
        for key in ("tile", "delivered_date", "utm", "resolution"):
            assert key in cfg["gpkg_fields"]

    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_bluetopo_style_gpkg_fields(self, source):
        cfg = get_config(source)
        assert cfg["gpkg_fields"]["tile"] == "tile"
        assert cfg["gpkg_fields"]["delivered_date"] == "Delivered_Date"
        assert cfg["gpkg_fields"]["utm"] == "UTM"
        assert cfg["gpkg_fields"]["resolution"] == "Resolution"

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_navigation_gpkg_fields(self, source):
        cfg = get_config(source)
        assert cfg["gpkg_fields"]["tile"] == "TILE_ID"
        assert cfg["gpkg_fields"]["delivered_date"] == "ISSUANCE"
        assert cfg["gpkg_fields"]["utm"] == "UTM"
        assert cfg["gpkg_fields"]["resolution"] == "Resolution"

    @pytest.mark.parametrize("source", list(DATA_SOURCES.keys()))
    def test_gpkg_field_values_are_strings(self, source):
        cfg = get_config(source)
        for val in cfg["gpkg_fields"].values():
            assert isinstance(val, str)


# ---------------------------------------------------------------------------
# validate_config
# ---------------------------------------------------------------------------


class TestValidateConfig:
    def test_valid_config_passes(self):
        cfg = get_config("bluetopo")
        # Should not raise
        validate_config(cfg)

    def test_empty_file_slots_raises(self):
        cfg = get_config("bluetopo")
        cfg["file_slots"] = []
        with pytest.raises(ValueError, match="file_slots must be non-empty"):
            validate_config(cfg)

    def test_missing_slot_name_raises(self):
        cfg = get_config("bluetopo")
        cfg["file_slots"] = [{"gpkg_link": "X", "gpkg_checksum": "Y"}]
        with pytest.raises(ValueError, match="each file_slot must have 'name'"):
            validate_config(cfg)

    def test_missing_gpkg_fields_raises(self):
        cfg = get_config("bluetopo")
        cfg["gpkg_fields"] = {}
        with pytest.raises(ValueError, match="gpkg_fields must be defined"):
            validate_config(cfg)

    def test_missing_gpkg_field_key_raises(self):
        cfg = get_config("bluetopo")
        del cfg["gpkg_fields"]["tile"]
        with pytest.raises(ValueError, match="gpkg_fields missing required key 'tile'"):
            validate_config(cfg)

    def test_has_rat_without_rat_fields_raises(self):
        cfg = get_config("bluetopo")
        cfg["rat_fields"] = None
        with pytest.raises(ValueError, match="has_rat=True requires 'rat_fields'"):
            validate_config(cfg)

    def test_invalid_rat_open_method_raises(self):
        cfg = get_config("bluetopo")
        cfg["rat_open_method"] = "invalid"
        with pytest.raises(ValueError, match="unknown rat_open_method"):
            validate_config(cfg)

    def test_both_subdatasets_and_band_descriptions_raises(self):
        cfg = get_config("s102v22")
        cfg["band_descriptions"] = ["Elevation"]
        with pytest.raises(ValueError, match="cannot have both subdatasets and band_descriptions"):
            validate_config(cfg)

    def test_neither_subdatasets_nor_band_descriptions_raises(self):
        cfg = get_config("bluetopo")
        cfg["subdatasets"] = None
        cfg["band_descriptions"] = None
        with pytest.raises(ValueError, match="must have either subdatasets or band_descriptions"):
            validate_config(cfg)


# ---------------------------------------------------------------------------
# VALID_TARGET_RESOLUTIONS
# ---------------------------------------------------------------------------


class TestValidTargetResolutions:
    def test_contains_expected_values(self):
        assert VALID_TARGET_RESOLUTIONS == {2, 4, 8, 16, 32, 64}

    def test_is_a_set(self):
        assert isinstance(VALID_TARGET_RESOLUTIONS, set)


# ---------------------------------------------------------------------------
# RAT zero fields
# ---------------------------------------------------------------------------


class TestRatZeroFields:
    def test_s102v22_has_zero_fields(self):
        cfg = get_config("s102v22")
        assert "rat_zero_fields" in cfg
        assert "feature_size_var" in cfg["rat_zero_fields"]
        assert "bathymetric_uncertainty_type" in cfg["rat_zero_fields"]

    def test_s102v30_has_zero_fields(self):
        cfg = get_config("s102v30")
        assert "rat_zero_fields" in cfg
        assert "feature_size_var" in cfg["rat_zero_fields"]
        assert "type_of_bathymetric_estimation_uncertainty" in cfg["rat_zero_fields"]

    def test_non_s102v22_no_zero_fields(self):
        for src in ["bluetopo", "modeling", "bag", "s102v21", "hsd"]:
            cfg = get_config(src)
            # .get returns [] when key absent -- tested in add_vrt_rat
            assert cfg.get("rat_zero_fields", []) == [] or "rat_zero_fields" not in cfg


# ---------------------------------------------------------------------------
# RAT field type edge cases
# ---------------------------------------------------------------------------


class TestRatFieldTypes:
    def test_s102v22_feature_size_is_str(self):
        """S102V22 feature_size is str, unlike BlueTopo/Modeling where it's float."""
        cfg = get_config("s102v22")
        assert cfg["rat_fields"]["feature_size"][0] == str

    def test_bluetopo_feature_size_is_float(self):
        cfg = get_config("bluetopo")
        assert cfg["rat_fields"]["feature_size"][0] == float

    def test_hsd_has_same_base_fields_as_bluetopo(self):
        bt = get_config("bluetopo")
        hsd = get_config("hsd")
        for field in bt["rat_fields"]:
            assert field in hsd["rat_fields"]

    def test_hsd_has_extra_fields_not_in_bluetopo(self):
        bt = get_config("bluetopo")
        hsd = get_config("hsd")
        extra = set(hsd["rat_fields"].keys()) - set(bt["rat_fields"].keys())
        assert "catzoc" in extra
        assert "supercession_score" in extra

    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_rat_has_count_field(self, source):
        cfg = get_config(source)
        assert "count" in cfg["rat_fields"]
        assert cfg["rat_fields"]["count"][1] == gdal.GFU_PixelCount

    def test_s102v22_no_count_field(self):
        cfg = get_config("s102v22")
        assert "count" not in cfg["rat_fields"]

    def test_s102v30_no_count_field(self):
        cfg = get_config("s102v30")
        assert "count" not in cfg["rat_fields"]

    def test_s102v30_has_renamed_uncertainty_field(self):
        """S102V30 renames bathymetricUncertaintyType to type_of_bathymetric_estimation_uncertainty."""
        cfg = get_config("s102v30")
        assert "type_of_bathymetric_estimation_uncertainty" in cfg["rat_fields"]
        assert "bathymetric_uncertainty_type" not in cfg["rat_fields"]

    def test_s102v22_has_original_uncertainty_field(self):
        """S102V22 uses bathymetric_uncertainty_type (not the v3.0 renamed version)."""
        cfg = get_config("s102v22")
        assert "bathymetric_uncertainty_type" in cfg["rat_fields"]
        assert "type_of_bathymetric_estimation_uncertainty" not in cfg["rat_fields"]

    def test_s102v30_feature_size_is_str(self):
        """S102V30 feature_size is str, same as S102V22."""
        cfg = get_config("s102v30")
        assert cfg["rat_fields"]["feature_size"][0] == str


# ---------------------------------------------------------------------------
# Catalog table / PK consistency
# ---------------------------------------------------------------------------


class TestCatalogTableConsistency:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_dual_slot_uses_tileset_table(self, source):
        cfg = get_config(source)
        assert cfg["catalog_table"] == "tileset"
        assert cfg["catalog_pk"] == "tilescheme"

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_single_slot_uses_catalog_table(self, source):
        cfg = get_config(source)
        assert cfg["catalog_table"] == "catalog"
        assert cfg["catalog_pk"] == "file"


# ---------------------------------------------------------------------------
# Band descriptions
# ---------------------------------------------------------------------------


class TestBandDescriptions:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_dual_file_3_bands(self, source):
        cfg = get_config(source)
        assert len(cfg["band_descriptions"]) == 3
        assert cfg["band_descriptions"] == ["Elevation", "Uncertainty", "Contributor"]

    @pytest.mark.parametrize("source", ["bag", "s102v21"])
    def test_single_file_2_bands(self, source):
        cfg = get_config(source)
        assert len(cfg["band_descriptions"]) == 2
        assert cfg["band_descriptions"] == ["Elevation", "Uncertainty"]

    def test_s102v22_no_direct_band_descriptions(self):
        cfg = get_config("s102v22")
        assert cfg["band_descriptions"] is None

    def test_s102v22_subdataset_band_descriptions(self):
        cfg = get_config("s102v22")
        assert cfg["subdatasets"][0]["band_descriptions"] == ["Elevation", "Uncertainty"]
        assert cfg["subdatasets"][1]["band_descriptions"] == ["QualityOfSurvey"]

    def test_s102v30_no_direct_band_descriptions(self):
        cfg = get_config("s102v30")
        assert cfg["band_descriptions"] is None

    def test_s102v30_subdataset_band_descriptions(self):
        cfg = get_config("s102v30")
        assert cfg["subdatasets"][0]["band_descriptions"] == ["Elevation", "Uncertainty"]
        assert cfg["subdatasets"][1]["band_descriptions"] == ["QualityOfBathymetryCoverage"]


# ---------------------------------------------------------------------------
# XML prefix
# ---------------------------------------------------------------------------


class TestXmlPrefix:
    @pytest.mark.parametrize("source", ["s102v21", "s102v22", "s102v30"])
    def test_s102_has_xml_prefix(self, source):
        cfg = get_config(source)
        assert cfg["xml_prefix"] is not None
        assert "_CATALOG" in cfg["xml_prefix"]

    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "bag", "hsd"])
    def test_non_s102_no_xml_prefix(self, source):
        cfg = get_config(source)
        assert cfg["xml_prefix"] is None


# ---------------------------------------------------------------------------
# Min GDAL version
# ---------------------------------------------------------------------------


class TestMinGdalVersion:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "bag", "hsd"])
    def test_standard_sources_gdal_34(self, source):
        cfg = get_config(source)
        assert cfg["min_gdal_version"] == 3040000

    @pytest.mark.parametrize("source", ["s102v21", "s102v22", "s102v30"])
    def test_s102_sources_gdal_39(self, source):
        cfg = get_config(source)
        assert cfg["min_gdal_version"] == 3090000


# ---------------------------------------------------------------------------
# S102V22 subdataset s102_protocol flag
# ---------------------------------------------------------------------------


class TestS102Protocol:
    def test_bathymetry_no_s102_protocol(self):
        cfg = get_config("s102v22")
        assert cfg["subdatasets"][0]["s102_protocol"] is False

    def test_quality_has_s102_protocol(self):
        cfg = get_config("s102v22")
        assert cfg["subdatasets"][1]["s102_protocol"] is True

    def test_subdataset_suffixes_match_names(self):
        cfg = get_config("s102v22")
        assert cfg["subdatasets"][0]["suffix"] == "_BathymetryCoverage"
        assert cfg["subdatasets"][1]["suffix"] == "_QualityOfSurvey"

    def test_s102v30_subdataset_suffixes_match_names(self):
        cfg = get_config("s102v30")
        assert cfg["subdatasets"][0]["suffix"] == "_BathymetryCoverage"
        assert cfg["subdatasets"][1]["suffix"] == "_QualityOfBathymetryCoverage"

    def test_s102v30_bathymetry_no_s102_protocol(self):
        cfg = get_config("s102v30")
        assert cfg["subdatasets"][0]["s102_protocol"] is False

    def test_s102v30_quality_has_s102_protocol(self):
        cfg = get_config("s102v30")
        assert cfg["subdatasets"][1]["s102_protocol"] is True


# ---------------------------------------------------------------------------
# Deep copy isolation
# ---------------------------------------------------------------------------


class TestDeepCopyIsolation:
    def test_mutating_rat_fields_doesnt_affect_global(self):
        cfg = get_config("bluetopo")
        cfg["rat_fields"]["new_field"] = [str, gdal.GFU_Generic]
        cfg2 = get_config("bluetopo")
        assert "new_field" not in cfg2["rat_fields"]

    def test_mutating_subdatasets_doesnt_affect_global(self):
        cfg = get_config("s102v22")
        cfg["subdatasets"].append({"name": "Fake"})
        cfg2 = get_config("s102v22")
        assert len(cfg2["subdatasets"]) == 2

    def test_mutating_band_descriptions_doesnt_affect_global(self):
        cfg = get_config("bluetopo")
        cfg["band_descriptions"].append("Extra")
        cfg2 = get_config("bluetopo")
        assert len(cfg2["band_descriptions"]) == 3

    def test_mutating_file_slots_doesnt_affect_global(self):
        cfg = get_config("bluetopo")
        cfg["file_slots"].append({"name": "extra", "gpkg_link": "X", "gpkg_checksum": "Y"})
        cfg2 = get_config("bluetopo")
        assert len(cfg2["file_slots"]) == 2

    def test_mutating_gpkg_fields_doesnt_affect_global(self):
        cfg = get_config("bluetopo")
        cfg["gpkg_fields"]["extra"] = "EXTRA"
        cfg2 = get_config("bluetopo")
        assert "extra" not in cfg2["gpkg_fields"]


# ---------------------------------------------------------------------------
# KNOWN_RAT_FIELDS
# ---------------------------------------------------------------------------


class TestKnownRatFields:
    def test_has_23_entries(self):
        assert len(KNOWN_RAT_FIELDS) == 23

    def test_includes_all_bluetopo_fields(self):
        bt = get_config("bluetopo")
        for field in bt["rat_fields"]:
            assert field in KNOWN_RAT_FIELDS

    def test_includes_all_hsd_fields(self):
        hsd = get_config("hsd")
        for field in hsd["rat_fields"]:
            assert field in KNOWN_RAT_FIELDS

    def test_first_two_fields(self):
        keys = list(KNOWN_RAT_FIELDS.keys())
        assert keys[0] == "value"
        assert KNOWN_RAT_FIELDS["value"][1] == gdal.GFU_MinMax
        assert keys[1] == "count"
        assert KNOWN_RAT_FIELDS["count"][1] == gdal.GFU_PixelCount


# ---------------------------------------------------------------------------
# get_local_config
# ---------------------------------------------------------------------------


class TestGetLocalConfig:
    def test_canonical_name_is_resolved_name(self):
        cfg = get_local_config("Weird")
        assert cfg["canonical_name"] == "Weird"

    def test_prefixes_are_none(self):
        cfg = get_local_config("Weird")
        assert cfg["geom_prefix"] is None
        assert cfg["xml_prefix"] is None

    def test_rat_fields_is_full_known_set(self):
        cfg = get_local_config("Weird")
        assert len(cfg["rat_fields"]) == 23
        assert list(cfg["rat_fields"].keys()) == list(KNOWN_RAT_FIELDS.keys())

    def test_returns_deep_copy(self):
        cfg1 = get_local_config("Weird")
        cfg1["rat_fields"]["extra"] = [str, gdal.GFU_Generic]
        cfg1["canonical_name"] = "MUTATED"
        cfg2 = get_local_config("Weird")
        assert "extra" not in cfg2["rat_fields"]
        assert cfg2["canonical_name"] == "Weird"

    def test_inherits_bluetopo_file_slots(self):
        cfg = get_local_config("Weird")
        assert len(cfg["file_slots"]) == 2
        assert cfg["file_slots"][0]["name"] == "geotiff"
        assert cfg["file_slots"][1]["name"] == "rat"

    def test_inherits_bluetopo_gpkg_fields(self):
        cfg = get_local_config("Weird")
        assert cfg["gpkg_fields"]["tile"] == "tile"
        assert cfg["gpkg_fields"]["delivered_date"] == "Delivered_Date"

    def test_inherits_bluetopo_rat_settings(self):
        cfg = get_local_config("Weird")
        assert cfg["has_rat"] is True
        assert cfg["rat_open_method"] == "direct"
        assert cfg["rat_band"] == 3

    def test_known_name_inherits_source_config(self):
        """Known source name preserves file_slots and structure from that source."""
        cfg = get_local_config("BAG")
        assert len(cfg["file_slots"]) == 1
        assert cfg["file_slots"][0]["name"] == "file"
        assert cfg["geom_prefix"] is None
        assert cfg["xml_prefix"] is None
        assert cfg["canonical_name"] == "BAG"

    def test_known_dual_slot_name_inherits_dual_slots(self):
        """Known dual-slot source name preserves dual file_slots."""
        cfg = get_local_config("HSD")
        assert len(cfg["file_slots"]) == 2
        assert cfg["geom_prefix"] is None
        assert cfg["xml_prefix"] is None

    def test_s102v30_local_config(self):
        """S102V30 local config preserves subdatasets and single file_slot."""
        cfg = get_local_config("S102V30")
        assert len(cfg["file_slots"]) == 1
        assert cfg["file_slots"][0]["name"] == "file"
        assert cfg["subdatasets"] is not None
        assert len(cfg["subdatasets"]) == 2
        assert cfg["subdatasets"][1]["name"] == "QualityOfBathymetryCoverage"
        assert cfg["rat_open_method"] == "s102_quality"

    def test_known_name_uses_canonical_case(self):
        """Known source name uses canonical case regardless of input case."""
        cfg = get_local_config("bag")
        assert cfg["canonical_name"] == "BAG"
        cfg = get_local_config("s102v21")
        assert cfg["canonical_name"] == "S102V21"
        cfg = get_local_config("bluetopo")
        assert cfg["canonical_name"] == "BlueTopo"
