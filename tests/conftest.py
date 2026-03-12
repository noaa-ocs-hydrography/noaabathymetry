"""Shared fixtures for the BlueTopo test suite."""

import os
import sqlite3

import h5py
import numpy as np
import pytest
from osgeo import gdal, ogr, osr

from nbs.bluetopo.core.datasource import get_config
from nbs.bluetopo.core.build_vrt import connect_to_survey_registry


# ---------------------------------------------------------------------------
# Session banner
# ---------------------------------------------------------------------------

def pytest_report_header():
    """Print a note about on-disk temp files created during the test run."""
    return "NOTE: Tests create temporary data on disk (GeoTIFFs, BAGs, HDF5, GeoPackages, SQLite DBs) under pytest_temporary_data/."


# ---------------------------------------------------------------------------
# Config fixtures
# ---------------------------------------------------------------------------

ALL_REMOTE_SOURCES = ["bluetopo", "modeling", "bag", "s102v21", "s102v22", "s102v30"]
ALL_SOURCES = ALL_REMOTE_SOURCES + ["hsd"]


@pytest.fixture(params=ALL_SOURCES)
def source_cfg(request):
    """Yield (cfg, name) for each data source."""
    cfg = get_config(request.param)
    return cfg, request.param


@pytest.fixture(params=ALL_REMOTE_SOURCES)
def remote_source_cfg(request):
    """Yield (cfg, name) for each remote data source."""
    cfg = get_config(request.param)
    return cfg, request.param


# ---------------------------------------------------------------------------
# Synthetic GeoTIFF generator
# ---------------------------------------------------------------------------

@pytest.fixture
def make_geotiff(tmp_path):
    """Create a minimal GeoTIFF with configurable bands, size, and optional RAT."""

    def _make(name="tile.tif", bands=3, width=4, height=4, utm_zone=19,
              rat_entries=None, rat_fields=None, rat_band=None,
              pixel_size=2):
        """
        Parameters
        ----------
        name : str
            Filename (created inside tmp_path).
        bands : int
            Number of bands.
        width, height : int
            Pixel dimensions.
        utm_zone : int
            UTM zone for the projection.
        rat_entries : list[list] | None
            Row data for the RAT. Each inner list is one row of values.
        rat_fields : dict | None
            {field_name: [python_type, gdal_usage]} for RAT columns.
        rat_band : int | None
            1-based band index on which to write the RAT.
        pixel_size : int | float
            Pixel size in meters for the GeoTransform.

        Returns
        -------
        str
            Absolute path to the created GeoTIFF.
        """
        path = str(tmp_path / name)
        driver = gdal.GetDriverByName("GTiff")
        ds = driver.Create(path, width, height, bands, gdal.GDT_Float32)

        # Set UTM projection
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(32600 + utm_zone)  # UTM North
        ds.SetProjection(srs.ExportToWkt())
        ds.SetGeoTransform([500000, pixel_size, 0, 4400000, 0, -pixel_size])

        # Write data to each band
        for b in range(1, bands + 1):
            band = ds.GetRasterBand(b)
            data = np.random.rand(height, width).astype(np.float32) * 100
            if b == bands:
                # Last band: integer contributor IDs
                data = np.arange(width * height, dtype=np.float32).reshape(height, width) % 5
            band.WriteArray(data)
            band.SetNoDataValue(-9999.0)

        # Optional RAT
        if rat_entries and rat_fields and rat_band:
            rat = gdal.RasterAttributeTable()
            for field_name, (field_type, usage) in rat_fields.items():
                if field_type == str:
                    col_type = gdal.GFT_String
                elif field_type == int:
                    col_type = gdal.GFT_Integer
                elif field_type == float:
                    col_type = gdal.GFT_Real
                else:
                    raise TypeError(f"Unknown type {field_type}")
                rat.CreateColumn(field_name, col_type, usage)
            rat.SetRowCount(len(rat_entries))
            field_names = list(rat_fields.keys())
            for row_idx, row_data in enumerate(rat_entries):
                for col_idx, field_name in enumerate(field_names):
                    field_type = rat_fields[field_name][0]
                    if field_type == str:
                        rat.SetValueAsString(row_idx, col_idx, str(row_data[col_idx]))
                    elif field_type == int:
                        rat.SetValueAsInt(row_idx, col_idx, int(row_data[col_idx]))
                    elif field_type == float:
                        rat.SetValueAsDouble(row_idx, col_idx, float(row_data[col_idx]))
            band = ds.GetRasterBand(rat_band)
            band.SetDefaultRAT(rat)

        ds.FlushCache()
        ds = None
        return path

    return _make


# ---------------------------------------------------------------------------
# Synthetic BAG generator
# ---------------------------------------------------------------------------

@pytest.fixture
def make_bag(tmp_path):
    """Create a minimal BAG file using GDAL's BAG driver.

    Produces a 2-band file (elevation + uncertainty) in the BAG HDF5 format,
    including the ISO 19139 metadata that GDAL requires for georeferencing.
    """

    def _make(name="tile.bag", width=4, height=4, utm_zone=19):
        """
        Parameters
        ----------
        name : str
            Filename (created inside tmp_path).
        width, height : int
            Pixel dimensions.
        utm_zone : int
            UTM zone for the projection.

        Returns
        -------
        str
            Absolute path to the created BAG file.
        """
        path = str(tmp_path / name)
        drv = gdal.GetDriverByName("BAG")
        ds = drv.Create(path, width, height, 2, gdal.GDT_Float32)

        srs = osr.SpatialReference()
        srs.ImportFromEPSG(32600 + utm_zone)
        ds.SetProjection(srs.ExportToWkt())
        ds.SetGeoTransform([500000, 2, 0, 4100000 + height * 2, 0, -2])

        elevation = np.random.rand(height, width).astype(np.float32) * -20 - 5
        uncertainty = np.random.rand(height, width).astype(np.float32) * 0.9 + 0.1
        ds.GetRasterBand(1).WriteArray(elevation)
        ds.GetRasterBand(1).SetNoDataValue(1000000.0)
        ds.GetRasterBand(2).WriteArray(uncertainty)
        ds.GetRasterBand(2).SetNoDataValue(1000000.0)

        ds.FlushCache()
        ds = None
        return path

    return _make


# ---------------------------------------------------------------------------
# Synthetic S102 v2.1 HDF5 generator
# ---------------------------------------------------------------------------

@pytest.fixture
def make_s102v21(tmp_path):
    """Create a minimal S102 v2.1 HDF5 file openable by GDAL's S102 driver.

    V2.1 uses ``dataCodingFormat=2`` and has only BathymetryCoverage (2 bands:
    depth + uncertainty).  No QualityOfSurvey subdataset.  The extent is stored
    as nested groups (high/low/coordValues) rather than a flat dataset.
    """
    str_type = h5py.special_dtype(vlen=str)

    def _make(name="tile.h5", width=4, height=4, utm_zone=19):
        """
        Parameters
        ----------
        name : str
            Filename (created inside tmp_path).
        width, height : int
            Pixel dimensions.
        utm_zone : int
            UTM zone for the projection.

        Returns
        -------
        str
            Absolute path to the created HDF5 file.
        """
        path = str(tmp_path / name)
        epsg = 32600 + utm_zone
        origin_x, origin_y = 500000.0, 4100000.0
        spacing = 2.0
        extent_x = origin_x + width * spacing
        extent_y = origin_y + height * spacing

        with h5py.File(path, "w") as f:
            f.attrs["productSpecification"] = "INT.IHO.S-102.2.1"
            f.attrs["horizontalDatumReference"] = "EPSG"
            f.attrs["horizontalDatumValue"] = np.int32(epsg)
            f.attrs["eastBoundLongitude"] = -68.0
            f.attrs["westBoundLongitude"] = -69.0
            f.attrs["northBoundLatitude"] = 38.0
            f.attrs["southBoundLatitude"] = 37.0
            f.attrs["epoch"] = ""
            f.attrs["extentTypeCode"] = np.int32(0)
            f.attrs["geographicIdentifier"] = "Test"
            f.attrs["issueDate"] = "20240101"
            f.attrs["issueTime"] = "000000+0000"
            f.attrs["metaFeatures"] = ""
            f.attrs["metadata"] = ""
            f.attrs["verticalDatum"] = np.int32(12)

            bc = f.create_group("BathymetryCoverage")
            bc.attrs["dataCodingFormat"] = np.int32(2)
            bc.attrs["dimension"] = np.int32(2)
            bc.attrs["commonPointRule"] = np.int32(1)
            bc.attrs["interpolationType"] = np.int32(1)
            bc.attrs["numInstances"] = np.int32(1)
            bc.attrs["sequencingRule.scanDirection"] = "Easting, Northing"
            bc.attrs["sequencingRule.type"] = np.int32(1)
            bc.attrs["horizontalPositionUncertainty"] = np.float32(0.0)
            bc.attrs["verticalUncertainty"] = np.float32(0.0)

            bc01 = bc.create_group("BathymetryCoverage.01")
            bc01.attrs["numGRP"] = np.int32(1)
            bc01.attrs["numPointsLatitudinal"] = np.int32(height)
            bc01.attrs["numPointsLongitudinal"] = np.int32(width)
            bc01.attrs["gridOriginLatitude"] = origin_y
            bc01.attrs["gridOriginLongitude"] = origin_x
            bc01.attrs["gridSpacingLatitudinal"] = spacing
            bc01.attrs["gridSpacingLongitudinal"] = spacing
            bc01.attrs["startSequence"] = "0,0"
            bc01.attrs["eastBoundLongitude"] = extent_x
            bc01.attrs["westBoundLongitude"] = origin_x
            bc01.attrs["northBoundLatitude"] = extent_y
            bc01.attrs["southBoundLatitude"] = origin_y
            bc01.attrs["extentTypeCode"] = np.int32(0)
            bc01.attrs["instanceChunking"] = f"{height},{width}"

            g001 = bc01.create_group("Group_001")
            g001.attrs["dimension"] = np.int32(2)
            g001.attrs["minimumDepth"] = np.float32(5.0)
            g001.attrs["maximumDepth"] = np.float32(20.0)
            g001.attrs["minimumUncertainty"] = np.float32(0.1)
            g001.attrs["maximumUncertainty"] = np.float32(1.0)

            # V2.1 extent uses nested groups
            extent = g001.create_group("extent")
            high = extent.create_group("high")
            high.create_dataset("coordValues",
                                data=np.array([width - 1, height - 1], dtype=np.int64))
            low = extent.create_group("low")
            low.create_dataset("coordValues",
                               data=np.array([0, 0], dtype=np.int64))

            dt = np.dtype([("depth", "<f4"), ("uncertainty", "<f4")])
            values = np.zeros((height, width), dtype=dt)
            values["depth"] = (np.random.rand(height, width) * 15 + 5).astype(np.float32)
            values["uncertainty"] = (np.random.rand(height, width) * 0.9 + 0.1).astype(np.float32)
            g001.create_dataset("values", data=values)

            bc.create_dataset("axisNames",
                              data=np.array(["Easting", "Northing"], dtype=object))

            gf = f.create_group("Group_F")
            gf_dt = np.dtype([
                ("code", str_type), ("name", str_type),
                ("uom.name", str_type), ("fillValue", str_type),
                ("datatype", str_type), ("lower", str_type),
                ("upper", str_type), ("closure", str_type),
            ])
            bc_desc = np.zeros(2, dtype=gf_dt)
            bc_desc[0] = ("depth", "depth", "metres", "1000000",
                          "H5T_NATIVE_FLOAT", "-12000", "12000", "closedInterval")
            bc_desc[1] = ("uncertainty", "uncertainty", "metres", "1000000",
                          "H5T_NATIVE_FLOAT", "0", "12000", "gtLeInterval")
            gf.create_dataset("BathymetryCoverage", data=bc_desc)
            gf.create_dataset("featureCode",
                              data=np.array(["BathymetryCoverage"], dtype=object))

        return path

    return _make


# ---------------------------------------------------------------------------
# Synthetic S102 v2.2 HDF5 generator
# ---------------------------------------------------------------------------

@pytest.fixture
def make_s102v22(tmp_path):
    """Create a minimal S102 v2.2 HDF5 file openable by GDAL's S102 driver.

    V2.2 uses ``dataCodingFormat=9`` and includes both BathymetryCoverage
    (2 bands: depth + uncertainty) and QualityOfSurvey (1 band with
    featureAttributeTable / RAT).
    """
    str_type = h5py.special_dtype(vlen=str)

    def _make(name="tile.h5", width=4, height=4, utm_zone=19, n_qos_rows=5):
        """
        Parameters
        ----------
        name : str
            Filename (created inside tmp_path).
        width, height : int
            Pixel dimensions.
        utm_zone : int
            UTM zone for the projection (sets horizontalCRS).
        n_qos_rows : int
            Number of rows in the QualityOfSurvey featureAttributeTable.

        Returns
        -------
        str
            Absolute path to the created HDF5 file.
        """
        path = str(tmp_path / name)
        epsg = 32600 + utm_zone
        origin_x, origin_y = 500000.0, 4100000.0
        spacing = 2.0

        with h5py.File(path, "w") as f:
            # Root attributes
            f.attrs["productSpecification"] = "INT.IHO.S-102.2.2"
            f.attrs["horizontalCRS"] = np.int32(epsg)
            f.attrs["eastBoundLongitude"] = -68.0
            f.attrs["westBoundLongitude"] = -69.0
            f.attrs["northBoundLatitude"] = 38.0
            f.attrs["southBoundLatitude"] = 37.0

            extent_x = origin_x + width * spacing
            extent_y = origin_y + height * spacing

            grid_attrs = {
                "numGRP": np.int32(1),
                "numPointsLatitudinal": np.int32(height),
                "numPointsLongitudinal": np.int32(width),
                "gridOriginLatitude": origin_y,
                "gridOriginLongitude": origin_x,
                "gridSpacingLatitudinal": spacing,
                "gridSpacingLongitudinal": spacing,
                "startSequence": "0,0",
                "eastBoundLongitude": extent_x,
                "westBoundLongitude": origin_x,
                "northBoundLatitude": extent_y,
                "southBoundLatitude": origin_y,
            }

            coverage_attrs = {
                "dataCodingFormat": np.int32(9),
                "dimension": np.int32(2),
                "commonPointRule": np.int32(1),
                "interpolationType": np.int32(1),
                "sequencingRule.scanDirection": "Easting, Northing",
                "sequencingRule.type": np.int32(1),
                "horizontalPositionUncertainty": np.float32(0.0),
                "verticalUncertainty": np.float32(0.0),
            }

            # --- BathymetryCoverage ---
            bc = f.create_group("BathymetryCoverage")
            for k, v in coverage_attrs.items():
                bc.attrs[k] = v
            bc.attrs["numInstances"] = np.int32(1)

            bc01 = bc.create_group("BathymetryCoverage.01")
            for k, v in grid_attrs.items():
                bc01.attrs[k] = v

            g001 = bc01.create_group("Group_001")
            g001.attrs["minimumDepth"] = np.float32(5.0)
            g001.attrs["maximumDepth"] = np.float32(20.0)
            g001.attrs["minimumUncertainty"] = np.float32(0.1)
            g001.attrs["maximumUncertainty"] = np.float32(1.0)

            dt = np.dtype([("depth", "<f4"), ("uncertainty", "<f4")])
            values = np.zeros((height, width), dtype=dt)
            values["depth"] = (np.random.rand(height, width) * 15 + 5).astype(np.float32)
            values["uncertainty"] = (np.random.rand(height, width) * 0.9 + 0.1).astype(np.float32)
            g001.create_dataset("values", data=values)

            bc01.create_dataset("extent", data=np.array(
                [[0, 0], [width - 1, height - 1]], dtype=np.int64))
            bc.create_dataset("axisNames", data=np.array(
                ["Easting", "Northing"], dtype=object))

            # --- QualityOfSurvey ---
            qos = f.create_group("QualityOfSurvey")
            for k, v in coverage_attrs.items():
                qos.attrs[k] = v
            qos.attrs["numInstances"] = np.int32(0)
            qos.attrs["timeUncertainty"] = np.float32(0.0)

            qos01 = qos.create_group("QualityOfSurvey.01")
            for k, v in grid_attrs.items():
                qos01.attrs[k] = v

            g001q = qos01.create_group("Group_001")
            qos_values = np.random.randint(
                0, n_qos_rows, (height, width), dtype=np.uint32)
            g001q.create_dataset("values", data=qos_values)

            qos.create_dataset("axisNames", data=np.array(
                ["Easting", "Northing"], dtype=object))

            # featureAttributeTable
            fat_dt = np.dtype([
                ("id", "<u4"),
                ("dataAssessment", "u1"),
                ("featuresDetected.leastDepthOfDetectedFeaturesMeasured", "u1"),
                ("featuresDetected.significantFeaturesDetected", "u1"),
                ("featuresDetected.sizeOfFeaturesDetected", "<f4"),
                ("featureSizeVar", "<f4"),
                ("fullSeafloorCoverageAchieved", "u1"),
                ("bathyCoverage", "u1"),
                ("zoneOfConfidence.horizontalPositionUncertainty.uncertaintyFixed", "<f4"),
                ("zoneOfConfidence.horizontalPositionUncertainty.uncertaintyVariableFactor", "<f4"),
                ("surveyDateRange.dateStart", str_type),
                ("surveyDateRange.dateEnd", str_type),
                ("sourceSurveyID", str_type),
                ("surveyAuthority", str_type),
                ("bathymetricUncertaintyType", "u1"),
            ])
            fat_data = np.zeros(n_qos_rows, dtype=fat_dt)
            for i in range(n_qos_rows):
                fat_data[i]["id"] = i
                fat_data[i]["dataAssessment"] = 1
                fat_data[i]["surveyDateRange.dateStart"] = "2024-01-01"
                fat_data[i]["surveyDateRange.dateEnd"] = "2024-06-01"
                fat_data[i]["sourceSurveyID"] = f"SURVEY_{i}"
                fat_data[i]["surveyAuthority"] = "NOAA"
            qos.create_dataset("featureAttributeTable", data=fat_data)

            # --- Group_F ---
            gf = f.create_group("Group_F")
            gf_dt = np.dtype([
                ("code", str_type), ("name", str_type),
                ("uom.name", str_type), ("fillValue", str_type),
                ("datatype", str_type), ("lower", str_type),
                ("upper", str_type), ("closure", str_type),
            ])
            bc_desc = np.zeros(2, dtype=gf_dt)
            bc_desc[0] = ("depth", "depth", "metres", "1000000",
                          "H5T_NATIVE_FLOAT", "-12000", "12000", "closedInterval")
            bc_desc[1] = ("uncertainty", "uncertainty", "metres", "1000000",
                          "H5T_NATIVE_FLOAT", "0", "12000", "gtLeInterval")
            gf.create_dataset("BathymetryCoverage", data=bc_desc)

            qos_desc = np.zeros(1, dtype=gf_dt)
            qos_desc[0] = ("id", "id", "", "0",
                           "H5T_NATIVE_UINT", "0", "4294967294", "closedInterval")
            gf.create_dataset("QualityOfSurvey", data=qos_desc)

            gf.create_dataset("featureCode", data=np.array(
                ["BathymetryCoverage", "QualityOfSurvey"], dtype=object))

        return path

    return _make


# ---------------------------------------------------------------------------
# Synthetic S102 v3.0 HDF5 generator
# ---------------------------------------------------------------------------


@pytest.fixture
def make_s102v30(tmp_path):
    """Create a minimal S102 v3.0 HDF5 file openable by GDAL's S102 driver.

    V3.0 uses ``dataCodingFormat=2`` for BathymetryCoverage (reverted from
    v2.2's 9) and ``dataCodingFormat=9`` for QualityOfBathymetryCoverage.
    The quality group is renamed from ``QualityOfSurvey`` (v2.2) to
    ``QualityOfBathymetryCoverage``.  RAT field 14 is renamed from
    ``bathymetricUncertaintyType`` to ``typeOfBathymetricEstimationUncertainty``.
    """
    str_type = h5py.special_dtype(vlen=str)

    def _make(name="tile.h5", width=4, height=4, utm_zone=19, n_qos_rows=5):
        """
        Parameters
        ----------
        name : str
            Filename (created inside tmp_path).
        width, height : int
            Pixel dimensions.
        utm_zone : int
            UTM zone for the projection (sets horizontalCRS).
        n_qos_rows : int
            Number of rows in the QualityOfBathymetryCoverage
            featureAttributeTable.

        Returns
        -------
        str
            Absolute path to the created HDF5 file.
        """
        path = str(tmp_path / name)
        epsg = 32600 + utm_zone
        origin_x, origin_y = 500000.0, 4100000.0
        spacing = 2.0

        with h5py.File(path, "w") as f:
            # Root attributes (v3.0: no metadata attr, same CRS attrs as v2.2)
            f.attrs["productSpecification"] = "INT.IHO.S-102.3.0.0"
            f.attrs["horizontalCRS"] = np.int32(epsg)
            f.attrs["verticalCS"] = np.int32(6498)
            f.attrs["verticalCoordinateBase"] = np.uint8(2)
            f.attrs["verticalDatum"] = np.uint16(12)
            f.attrs["verticalDatumReference"] = np.uint8(1)
            f.attrs["eastBoundLongitude"] = np.float32(-68.0)
            f.attrs["westBoundLongitude"] = np.float32(-69.0)
            f.attrs["northBoundLatitude"] = np.float32(38.0)
            f.attrs["southBoundLatitude"] = np.float32(37.0)
            f.attrs["geographicIdentifier"] = "Test"
            f.attrs["issueDate"] = "20240101"
            f.attrs["issueTime"] = "000000+0000"

            extent_x = origin_x + width * spacing
            extent_y = origin_y + height * spacing

            grid_attrs = {
                "numGRP": np.int32(1),
                "numPointsLatitudinal": np.int32(height),
                "numPointsLongitudinal": np.int32(width),
                "gridOriginLatitude": origin_y,
                "gridOriginLongitude": origin_x,
                "gridSpacingLatitudinal": spacing,
                "gridSpacingLongitudinal": spacing,
                "startSequence": "0,0",
                "eastBoundLongitude": extent_x,
                "westBoundLongitude": origin_x,
                "northBoundLatitude": extent_y,
                "southBoundLatitude": origin_y,
            }

            # --- BathymetryCoverage (dataCodingFormat=2 in v3.0) ---
            bc = f.create_group("BathymetryCoverage")
            bc.attrs["dataCodingFormat"] = np.int32(2)
            bc.attrs["dimension"] = np.int32(2)
            bc.attrs["commonPointRule"] = np.int32(2)
            bc.attrs["interpolationType"] = np.int32(1)
            bc.attrs["numInstances"] = np.int32(1)
            bc.attrs["dataOffsetCode"] = np.int32(5)
            bc.attrs["sequencingRule.scanDirection"] = "Easting, Northing"
            bc.attrs["sequencingRule.type"] = np.int32(1)
            bc.attrs["horizontalPositionUncertainty"] = np.float32(0.0)
            bc.attrs["verticalUncertainty"] = np.float32(0.0)

            bc01 = bc.create_group("BathymetryCoverage.01")
            for k, v in grid_attrs.items():
                bc01.attrs[k] = v

            g001 = bc01.create_group("Group_001")
            g001.attrs["minimumDepth"] = np.float32(5.0)
            g001.attrs["maximumDepth"] = np.float32(20.0)
            g001.attrs["minimumUncertainty"] = np.float32(0.1)
            g001.attrs["maximumUncertainty"] = np.float32(1.0)
            g001.attrs["timePoint"] = "10101T000000Z"

            dt = np.dtype([("depth", "<f4"), ("uncertainty", "<f4")])
            values = np.zeros((height, width), dtype=dt)
            values["depth"] = (np.random.rand(height, width) * 15 + 5).astype(np.float32)
            values["uncertainty"] = (np.random.rand(height, width) * 0.9 + 0.1).astype(np.float32)
            g001.create_dataset("values", data=values)

            bc01.create_dataset("extent", data=np.array(
                [[0, 0], [width - 1, height - 1]], dtype=np.int64))
            bc.create_dataset("axisNames", data=np.array(
                ["Easting", "Northing"], dtype=object))

            # --- QualityOfBathymetryCoverage (renamed from QualityOfSurvey) ---
            qobc = f.create_group("QualityOfBathymetryCoverage")
            qobc.attrs["dataCodingFormat"] = np.int32(9)
            qobc.attrs["dimension"] = np.int32(2)
            qobc.attrs["commonPointRule"] = np.int32(2)
            qobc.attrs["interpolationType"] = np.int32(1)
            qobc.attrs["numInstances"] = np.int32(1)
            qobc.attrs["dataOffsetCode"] = np.int32(5)
            qobc.attrs["sequencingRule.scanDirection"] = "Easting, Northing"
            qobc.attrs["sequencingRule.type"] = np.int32(1)
            qobc.attrs["horizontalPositionUncertainty"] = np.float32(0.0)
            qobc.attrs["verticalUncertainty"] = np.float32(0.0)

            qobc01 = qobc.create_group("QualityOfBathymetryCoverage.01")
            for k, v in grid_attrs.items():
                qobc01.attrs[k] = v

            g001q = qobc01.create_group("Group_001")
            qos_values = np.random.randint(
                0, n_qos_rows, (height, width), dtype=np.uint32)
            g001q.create_dataset("values", data=qos_values)

            qobc.create_dataset("axisNames", data=np.array(
                ["Easting", "Northing"], dtype=object))

            # featureAttributeTable (field 14 renamed)
            fat_dt = np.dtype([
                ("id", "<u4"),
                ("dataAssessment", "u1"),
                ("featuresDetected.leastDepthOfDetectedFeaturesMeasured", "u1"),
                ("featuresDetected.significantFeaturesDetected", "u1"),
                ("featuresDetected.sizeOfFeaturesDetected", "<f4"),
                ("featureSizeVar", "<f4"),
                ("fullSeafloorCoverageAchieved", "u1"),
                ("bathyCoverage", "u1"),
                ("zoneOfConfidence.horizontalPositionUncertainty.uncertaintyFixed", "<f4"),
                ("zoneOfConfidence.horizontalPositionUncertainty.uncertaintyVariableFactor", "<f4"),
                ("surveyDateRange.dateStart", str_type),
                ("surveyDateRange.dateEnd", str_type),
                ("sourceSurveyID", str_type),
                ("surveyAuthority", str_type),
                ("typeOfBathymetricEstimationUncertainty", "u1"),
            ])
            fat_data = np.zeros(n_qos_rows, dtype=fat_dt)
            for i in range(n_qos_rows):
                fat_data[i]["id"] = i
                fat_data[i]["dataAssessment"] = 1
                fat_data[i]["surveyDateRange.dateStart"] = "2024-01-01"
                fat_data[i]["surveyDateRange.dateEnd"] = "2024-06-01"
                fat_data[i]["sourceSurveyID"] = f"SURVEY_{i}"
                fat_data[i]["surveyAuthority"] = "NOAA"
            qobc.create_dataset("featureAttributeTable", data=fat_data)

            # --- Group_F ---
            gf = f.create_group("Group_F")
            gf_dt = np.dtype([
                ("code", str_type), ("name", str_type),
                ("uom.name", str_type), ("fillValue", str_type),
                ("datatype", str_type), ("lower", str_type),
                ("upper", str_type), ("closure", str_type),
            ])
            bc_desc = np.zeros(2, dtype=gf_dt)
            bc_desc[0] = ("depth", "depth", "metres", "1000000",
                          "H5T_NATIVE_FLOAT", "-14", "11050", "closedInterval")
            bc_desc[1] = ("uncertainty", "uncertainty", "metres", "1000000",
                          "H5T_NATIVE_FLOAT", "0", "", "geSemiInterval")
            gf.create_dataset("BathymetryCoverage", data=bc_desc)

            qobc_desc = np.zeros(1, dtype=gf_dt)
            qobc_desc[0] = ("iD", "ID", "", "0",
                            "H5T_INTEGER", "1", "", "geSemiInterval")
            gf.create_dataset("QualityOfBathymetryCoverage", data=qobc_desc)

            gf.create_dataset("featureCode", data=np.array(
                ["BathymetryCoverage", "QualityOfBathymetryCoverage"], dtype=object))

        return path

    return _make


# ---------------------------------------------------------------------------
# Synthetic tile-scheme geopackage generator
# ---------------------------------------------------------------------------

# Exact field schemas matching the real geopackages.
#
# BlueTopo/Modeling/HSD share the same 8-field schema:
#   tile, GeoTIFF_Link, RAT_Link, Delivered_Date, Resolution, UTM,
#   GeoTIFF_SHA256_Checksum, RAT_SHA256_Checksum
#
# Navigation (BAG, S102V21, S102V22, S102V30) uses a 14-field schema:
#   TILE_ID, REGION, SUBREGION, ISSUANCE, BAG, S102V21, S102V22, S102V30,
#   BAG_SHA256, S102V21_SHA256, S102V22_SHA256, S102V30_SHA256,
#   Resolution, UTM

_DUAL_FILE_FIELDS = [
    "tile", "GeoTIFF_Link", "RAT_Link", "Delivered_Date",
    "Resolution", "UTM", "GeoTIFF_SHA256_Checksum", "RAT_SHA256_Checksum",
]

_NAVIGATION_FIELDS = [
    "TILE_ID", "REGION", "SUBREGION", "ISSUANCE",
    "BAG", "S102V21", "S102V22", "S102V30",
    "BAG_SHA256", "S102V21_SHA256", "S102V22_SHA256", "S102V30_SHA256",
    "Resolution", "UTM",
]


@pytest.fixture
def make_tile_scheme(tmp_path):
    """Create a minimal tile-scheme geopackage matching the real NBS schemas.

    Two schema modes:

    * ``"dual_file"`` (default) -- creates the BlueTopo/Modeling/HSD schema
      with 8 String fields and Multi Polygon geometry in EPSG:4326.
    * ``"navigation"`` -- creates the Navigation schema (BAG / S102) with
      14 String fields and Multi Polygon geometry in EPSG:4326.

    Tile dicts passed in ``tiles`` must use the exact field names from the
    real geopackages (see ``_DUAL_FILE_FIELDS`` / ``_NAVIGATION_FIELDS``).
    Additional helper keys ``lon`` and ``lat`` control the tile centroid
    (default -76.0, 37.0) and are not written to the gpkg.
    """

    def _make(tiles, name="Tile_Scheme.gpkg", schema="dual_file"):
        """
        Parameters
        ----------
        tiles : list[dict]
            Feature attributes.  Use exact gpkg field names.
            ``lon`` / ``lat`` (optional) set tile centroid.
        name : str
            Output filename.
        schema : str
            ``"dual_file"`` for BlueTopo/Modeling/HSD,
            ``"navigation"`` for BAG/S102V21/S102V22/S102V30.

        Returns
        -------
        str
            Absolute path to the created geopackage.
        """
        path = str(tmp_path / name)
        drv = ogr.GetDriverByName("GPKG")
        ds = drv.CreateDataSource(path)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        layer_name = name.replace(".gpkg", "")
        lyr = ds.CreateLayer(layer_name, srs, ogr.wkbMultiPolygon)

        field_list = _DUAL_FILE_FIELDS if schema == "dual_file" else _NAVIGATION_FIELDS
        for field_name in field_list:
            lyr.CreateField(ogr.FieldDefn(field_name, ogr.OFTString))

        defn = lyr.GetLayerDefn()
        for t in tiles:
            feat = ogr.Feature(defn)
            lon = t.get("lon", -76.0)
            lat = t.get("lat", 37.0)
            size = 0.01
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint_2D(lon, lat)
            ring.AddPoint_2D(lon + size, lat)
            ring.AddPoint_2D(lon + size, lat - size)
            ring.AddPoint_2D(lon, lat - size)
            ring.AddPoint_2D(lon, lat)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)
            multipoly = ogr.Geometry(ogr.wkbMultiPolygon)
            multipoly.AddGeometry(poly)
            feat.SetGeometry(multipoly)
            for key, val in t.items():
                if key in ("lon", "lat"):
                    continue
                idx = defn.GetFieldIndex(key)
                if idx >= 0 and val is not None:
                    feat.SetField(key, str(val))
            lyr.CreateFeature(feat)

        ds = None
        return path

    return _make


# ---------------------------------------------------------------------------
# Pre-populated registry DB
# ---------------------------------------------------------------------------

@pytest.fixture
def registry_db(tmp_path):
    """Create a registry DB with tables matching a given config."""

    def _make(cfg, tiles=None, utms=None):
        """
        Parameters
        ----------
        cfg : dict
            Data source config.
        tiles : list[dict] | None
            Tile records to insert.
        utms : list[dict] | None
            UTM records to insert.

        Returns
        -------
        tuple[sqlite3.Connection, str]
            (connection, project_dir)
        """
        project_dir = str(tmp_path)
        conn = connect_to_survey_registry(project_dir, cfg)
        cursor = conn.cursor()

        if tiles:
            for tile in tiles:
                cols = ", ".join(tile.keys())
                placeholders = ", ".join(["?"] * len(tile))
                vals = list(tile.values())
                cursor.execute(
                    f"INSERT OR REPLACE INTO tiles({cols}) VALUES({placeholders})",
                    vals,
                )

        if utms:
            for utm in utms:
                cols = ", ".join(utm.keys())
                placeholders = ", ".join(["?"] * len(utm))
                vals = list(utm.values())
                cursor.execute(
                    f"INSERT OR REPLACE INTO vrt_utm({cols}) VALUES({placeholders})",
                    vals,
                )

        conn.commit()
        return conn, project_dir

    return _make


# ---------------------------------------------------------------------------
# Simple polygon geometry file
# ---------------------------------------------------------------------------

@pytest.fixture
def make_polygon(tmp_path):
    """Create a simple polygon geometry file (GeoJSON)."""

    def _make(lon=-76.0, lat=37.0, size=0.02, width=None, height=None,
              name="polygon.geojson", epsg=4326):
        w = width if width is not None else size
        h = height if height is not None else size
        path = str(tmp_path / name)
        drv = ogr.GetDriverByName("GeoJSON")
        ds = drv.CreateDataSource(path)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(epsg)
        lyr = ds.CreateLayer("polygon", srs, ogr.wkbPolygon)
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint_2D(lon, lat)
        ring.AddPoint_2D(lon + w, lat)
        ring.AddPoint_2D(lon + w, lat - h)
        ring.AddPoint_2D(lon, lat - h)
        ring.AddPoint_2D(lon, lat)
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        feat = ogr.Feature(lyr.GetLayerDefn())
        feat.SetGeometry(poly)
        lyr.CreateFeature(feat)
        ds = None
        return path

    return _make
