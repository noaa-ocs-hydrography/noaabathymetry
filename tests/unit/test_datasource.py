"""Tests for datasource.py configuration helpers."""

import re

import pytest
from osgeo import gdal

from nbs.bluetopo.core.datasource import (
    DATA_SOURCES,
    KNOWN_RAT_FIELDS,
    _timestamp,
    get_config,
    get_local_config,
    get_catalog_fields,
    get_vrt_subregion_fields,
    get_vrt_utm_fields,
    get_tiles_fields,
    get_built_flags,
    get_vrt_file_columns,
    get_utm_file_columns,
    get_disk_field,
    get_disk_fields,
    get_verified_fields,
    _subdataset_suffixes,
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
    def test_dual_file(self):
        cfg = get_config("bluetopo")
        fields = get_catalog_fields(cfg)
        assert "tilescheme" in fields
        assert fields["tilescheme"] == "text"
        assert "file" not in fields

    def test_single_file(self):
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
# get_vrt_subregion_fields
# ---------------------------------------------------------------------------


class TestGetVrtSubregionFields:
    def test_single_dataset_has_built(self):
        cfg = get_config("bluetopo")
        fields = get_vrt_subregion_fields(cfg)
        assert "built" in fields
        assert "res_2_vrt" in fields
        assert "complete_vrt" in fields

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_multi_subdataset_has_suffixed_built(self, source):
        cfg = get_config(source)
        fields = get_vrt_subregion_fields(cfg)
        assert "built_subdataset1" in fields
        assert "built_subdataset2" in fields
        assert "built" not in fields
        assert "res_2_subdataset1_vrt" in fields
        assert "complete_subdataset2_vrt" in fields

    def test_always_has_region_utm(self):
        for src in ["bluetopo", "s102v22", "s102v30"]:
            cfg = get_config(src)
            fields = get_vrt_subregion_fields(cfg)
            assert "region" in fields
            assert "utm" in fields

    def test_resolution_levels(self):
        cfg = get_config("bluetopo")
        fields = get_vrt_subregion_fields(cfg)
        for res in ["res_2", "res_4", "res_8", "complete"]:
            assert f"{res}_vrt" in fields
            assert f"{res}_ovr" in fields


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
    def test_dual_file(self):
        cfg = get_config("bluetopo")
        fields = get_tiles_fields(cfg)
        assert "geotiff_disk" in fields
        assert "rat_disk" in fields
        assert "geotiff_link" in fields
        assert "rat_link" in fields
        assert "geotiff_verified" in fields
        assert "rat_verified" in fields

    def test_single_file(self):
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
            assert "subregion" in fields


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
# get_vrt_file_columns / get_utm_file_columns
# ---------------------------------------------------------------------------


class TestVrtFileColumns:
    def test_excludes_region_utm_built(self):
        cfg = get_config("bluetopo")
        cols = get_vrt_file_columns(cfg)
        assert "region" not in cols
        assert "utm" not in cols
        assert "built" not in cols

    def test_contains_vrt_ovr(self):
        cfg = get_config("bluetopo")
        cols = get_vrt_file_columns(cfg)
        assert "res_2_vrt" in cols
        assert "res_2_ovr" in cols
        assert "complete_vrt" in cols

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
# Config completeness
# ---------------------------------------------------------------------------


REQUIRED_KEYS = [
    "canonical_name", "min_gdal_version", "geom_prefix", "tile_prefix",
    "xml_prefix", "bucket", "download_strategy", "file_layout",
    "catalog_table", "catalog_pk",
    "subdatasets", "band_descriptions", "has_rat", "rat_open_method",
    "rat_band", "rat_fields",
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

    @pytest.mark.parametrize("source", ["bluetopo", "modeling"])
    def test_prefix_listing_sources_have_tile_prefix(self, source):
        cfg = get_config(source)
        assert cfg["tile_prefix"] is not None

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_direct_link_remote_sources_have_no_tile_prefix(self, source):
        cfg = get_config(source)
        assert cfg["tile_prefix"] is None

    def test_hsd_local_only(self):
        cfg = get_config("hsd")
        assert cfg["geom_prefix"] is None
        assert cfg["tile_prefix"] is None

    def test_hsd_extra_rat_fields(self):
        cfg = get_config("hsd")
        assert "catzoc" in cfg["rat_fields"]
        assert "supercession_score" in cfg["rat_fields"]
        assert "decay_score" in cfg["rat_fields"]
        assert "unqualified" in cfg["rat_fields"]
        assert "sensitive" in cfg["rat_fields"]


# ---------------------------------------------------------------------------
# _subdataset_suffixes
# ---------------------------------------------------------------------------


class TestSubdatasetSuffixes:
    def test_none_for_single_dataset(self):
        cfg = get_config("bluetopo")
        assert _subdataset_suffixes(cfg) == [None]

    @pytest.mark.parametrize("source", ["s102v22", "s102v30"])
    def test_indexed_for_multi(self, source):
        cfg = get_config(source)
        assert _subdataset_suffixes(cfg) == ["_subdataset1", "_subdataset2"]

    def test_s102v22_subdataset_names(self):
        cfg = get_config("s102v22")
        assert cfg["subdatasets"][0]["name"] == "BathymetryCoverage"
        assert cfg["subdatasets"][1]["name"] == "QualityOfSurvey"

    def test_s102v30_subdataset_names(self):
        cfg = get_config("s102v30")
        assert cfg["subdatasets"][0]["name"] == "BathymetryCoverage"
        assert cfg["subdatasets"][1]["name"] == "QualityOfBathymetryCoverage"


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
# Tilescheme field map
# ---------------------------------------------------------------------------


class TestTileschemeFieldMap:
    def test_bluetopo_no_field_map(self):
        cfg = get_config("bluetopo")
        assert cfg["tilescheme_field_map"] is None

    def test_modeling_no_field_map(self):
        cfg = get_config("modeling")
        assert cfg["tilescheme_field_map"] is None

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_pmn_field_map_has_required_keys(self, source):
        cfg = get_config(source)
        fm = cfg["tilescheme_field_map"]
        assert fm is not None
        for key in ["tile", "file_link", "file_sha256_checksum",
                     "delivered_date", "utm", "resolution"]:
            assert key in fm

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_pmn_field_map_values_are_lowercase(self, source):
        cfg = get_config(source)
        fm = cfg["tilescheme_field_map"]
        for val in fm.values():
            assert val == val.lower()

    def test_bag_maps_to_correct_gpkg_fields(self):
        cfg = get_config("bag")
        fm = cfg["tilescheme_field_map"]
        assert fm["tile"] == "tile_id"
        assert fm["file_link"] == "bag"
        assert fm["file_sha256_checksum"] == "bag_sha256"

    def test_s102v22_maps_to_correct_gpkg_fields(self):
        cfg = get_config("s102v22")
        fm = cfg["tilescheme_field_map"]
        assert fm["file_link"] == "s102v22"
        assert fm["file_sha256_checksum"] == "s102v22_sha256"

    def test_s102v30_maps_to_correct_gpkg_fields(self):
        cfg = get_config("s102v30")
        fm = cfg["tilescheme_field_map"]
        assert fm["tile"] == "tile_id"
        assert fm["file_link"] == "s102v30"
        assert fm["file_sha256_checksum"] == "s102v30_sha256"
        assert fm["delivered_date"] == "issuance"


# ---------------------------------------------------------------------------
# Catalog table / PK consistency
# ---------------------------------------------------------------------------


class TestCatalogTableConsistency:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_dual_file_uses_tileset_table(self, source):
        cfg = get_config(source)
        assert cfg["catalog_table"] == "tileset"
        assert cfg["catalog_pk"] == "tilescheme"

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_single_file_uses_catalog_table(self, source):
        cfg = get_config(source)
        assert cfg["catalog_table"] == "catalog"
        assert cfg["catalog_pk"] == "file"


# ---------------------------------------------------------------------------
# File layout consistency
# ---------------------------------------------------------------------------


class TestFileLayoutConsistency:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling", "hsd"])
    def test_dual_file_sources(self, source):
        cfg = get_config(source)
        assert cfg["file_layout"] == "dual_file"

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30"])
    def test_single_file_sources(self, source):
        cfg = get_config(source)
        assert cfg["file_layout"] == "single_file"


# ---------------------------------------------------------------------------
# Download strategy consistency
# ---------------------------------------------------------------------------


class TestDownloadStrategyConsistency:
    @pytest.mark.parametrize("source", ["bluetopo", "modeling"])
    def test_prefix_listing_sources(self, source):
        cfg = get_config(source)
        assert cfg["download_strategy"] == "prefix_listing"

    @pytest.mark.parametrize("source", ["bag", "s102v21", "s102v22", "s102v30", "hsd"])
    def test_direct_link_sources(self, source):
        cfg = get_config(source)
        assert cfg["download_strategy"] == "direct_link"

    @pytest.mark.parametrize("source", list(DATA_SOURCES.keys()))
    def test_all_configs_have_valid_strategy(self, source):
        cfg = get_config(source)
        assert cfg["download_strategy"] in ("prefix_listing", "direct_link")

    @pytest.mark.parametrize("source", list(DATA_SOURCES.keys()))
    def test_prefix_listing_requires_tile_prefix(self, source):
        cfg = get_config(source)
        if cfg["download_strategy"] == "prefix_listing":
            assert cfg["tile_prefix"] is not None


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
        assert cfg["tile_prefix"] is None

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

    def test_inherits_bluetopo_file_layout(self):
        cfg = get_local_config("Weird")
        assert cfg["file_layout"] == "dual_file"

    def test_local_config_has_direct_link_strategy(self):
        cfg = get_local_config("Weird")
        assert cfg["download_strategy"] == "direct_link"

    def test_inherits_bluetopo_rat_settings(self):
        cfg = get_local_config("Weird")
        assert cfg["has_rat"] is True
        assert cfg["rat_open_method"] == "direct"
        assert cfg["rat_band"] == 3

    def test_known_name_inherits_source_config(self):
        """Known source name preserves file_layout and structure from that source."""
        cfg = get_local_config("BAG")
        assert cfg["file_layout"] == "single_file"
        assert cfg["download_strategy"] == "direct_link"
        assert cfg["tile_prefix"] is None
        assert cfg["canonical_name"] == "BAG"

    def test_known_dual_file_name_inherits_dual_file(self):
        """Known dual_file source name preserves dual_file layout."""
        cfg = get_local_config("HSD")
        assert cfg["file_layout"] == "dual_file"
        assert cfg["download_strategy"] == "direct_link"
        assert cfg["tile_prefix"] is None

    def test_s102v30_local_config(self):
        """S102V30 local config preserves subdatasets and single_file layout."""
        cfg = get_local_config("S102V30")
        assert cfg["file_layout"] == "single_file"
        assert cfg["download_strategy"] == "direct_link"
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
