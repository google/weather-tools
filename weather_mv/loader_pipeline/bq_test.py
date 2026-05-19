# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import contextlib
import datetime
import json
import logging
import os
import tempfile
import typing as t
import unittest
from unittest import mock

import geojson
import numpy as np
import pandas as pd
import simplejson
import xarray as xr
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, is_not_empty
from google.cloud.bigquery import SchemaField

from .bq import (
    DEFAULT_IMPORT_TIME,
    dataset_to_table_schema,
    fetch_geo_point,
    fetch_geo_polygon,
    ToBigQuery,
)
from .sinks_test import TestDataBase, _handle_missing_grib_be
from .util import _only_target_vars

logger = logging.getLogger(__name__)


class SchemaCreationTests(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_dataset = {
            "coords": {"a": {"dims": ("a",), "data": [pd.Timestamp(0)], "attrs": {}}},
            "attrs": {"is_normalized": False},
            "dims": "a",
            "data_vars": {
                "b": {"dims": ("a",), "data": [np.float32(1.0)]},
                "c": {"dims": ("a",), "data": [np.float64(2.0)]},
                "d": {"dims": ("a",), "data": [3.0]},
            }
        }
        self.test_dataset__with_schema_normalization = {
            "coords": {"a": {"dims": ("a",), "data": [pd.Timestamp(0)], "attrs": {}}},
            "attrs": {"is_normalized": True},
            "dims": "a",
            "data_vars": {
                "e_0_00_instant_b": {"dims": ("a",), "data": [np.float32(1.0)]},
                "e_0_00_instant_c": {"dims": ("a",), "data": [np.float64(2.0)]},
                "e_0_00_instant_d": {"dims": ("a",), "data": [3.0]},
            }
        }

    def test_schema_generation(self):
        ds = xr.Dataset.from_dict(self.test_dataset)
        schema = dataset_to_table_schema(ds)
        expected_schema = [
            SchemaField('a', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('b', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('c', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('d', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__with_schema_normalization(self):
        ds = xr.Dataset.from_dict(self.test_dataset__with_schema_normalization)
        schema = dataset_to_table_schema(ds)
        expected_schema = [
            SchemaField('a', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_b', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_c', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_d', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__with_target_columns(self):
        target_variables = ['c', 'd']
        ds = _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)
        schema = dataset_to_table_schema(ds)
        expected_schema = [
            SchemaField('a', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('c', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('d', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__with_target_columns__with_schema_normalization(self):
        target_variables = ['c', 'd']
        ds = _only_target_vars(xr.Dataset.from_dict(self.test_dataset__with_schema_normalization), target_variables)
        schema = dataset_to_table_schema(ds)
        expected_schema = [
            SchemaField('a', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_c', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_d', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__no_targets_specified(self):
        target_variables = []  # intentionally empty
        ds = _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)
        schema = dataset_to_table_schema(ds)
        expected_schema = [
            SchemaField('a', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('b', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('c', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('d', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__no_targets_specified__with_schema_normalization(self):
        target_variables = []  # intentionally empty
        ds = _only_target_vars(xr.Dataset.from_dict(self.test_dataset__with_schema_normalization), target_variables)
        schema = dataset_to_table_schema(ds)
        expected_schema = [
            SchemaField('a', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_b', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_c', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('e_0_00_instant_d', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__missing_target(self):
        with self.assertRaisesRegex(AssertionError, 'Target variable must be in original dataset.'):
            target_variables = ['a', 'foobar', 'd']
            _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)

    def test_schema_generation__missing_target__with_schema_normalization(self):
        with self.assertRaisesRegex(AssertionError, 'Target variable must be in original dataset.'):
            target_variables = ['a', 'foobar', 'd']
            _only_target_vars(xr.Dataset.from_dict(self.test_dataset__with_schema_normalization), target_variables)

    @_handle_missing_grib_be
    def test_schema_generation__non_index_coords(self):
        test_single_var = xr.open_dataset(
            f'{self.test_data_folder}/test_data_grib_single_timestep',
            engine='cfgrib'
        )
        schema = dataset_to_table_schema(test_single_var)
        expected_schema = [
            SchemaField('number', 'INT64', 'NULLABLE', None, (), None),
            SchemaField('time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('step', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('surface', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('latitude', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('longitude', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('valid_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('z', 'FLOAT64', 'NULLABLE', None, (), None),
            SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('data_uri', 'STRING', 'NULLABLE', None, (), None),
            SchemaField('data_first_step', 'TIMESTAMP', 'NULLABLE', None, (), None),
            SchemaField('geo_point', 'GEOGRAPHY', 'NULLABLE', None, (), None),
            SchemaField('geo_polygon', 'GEOGRAPHY', 'NULLABLE', None, (), None)

        ]
        self.assertListEqual(schema, expected_schema)


class ExtractRowsTestBase(TestDataBase):

    def extract(self, data_path, *, variables=None, area=None, open_dataset_kwargs=None,
                import_time=DEFAULT_IMPORT_TIME, disable_grib_schema_normalization=False,
                tif_metadata_for_start_time=None, tif_metadata_for_end_time=None, zarr: bool = False, zarr_kwargs=None,
                skip_creating_polygon: bool = False, geo_data_parquet_path,
                skip_creating_geo_data_parquet : bool = False) -> t.Iterator[t.Dict]:
        if zarr_kwargs is None:
            zarr_kwargs = {}
        op = ToBigQuery.from_kwargs(
            first_uri=data_path, dry_run=True, zarr=zarr, zarr_kwargs=zarr_kwargs,
            output_table='foo.bar.baz', variables=variables, area=area,
            xarray_open_dataset_kwargs=open_dataset_kwargs, import_time=import_time, infer_schema=False,
            tif_metadata_for_start_time=tif_metadata_for_start_time,
            tif_metadata_for_end_time=tif_metadata_for_end_time, skip_region_validation=True,
            disable_grib_schema_normalization=disable_grib_schema_normalization, rows_chunk_size=1_000_000,
            skip_creating_polygon=skip_creating_polygon,
            geo_data_parquet_path=geo_data_parquet_path,
            skip_creating_geo_data_parquet=skip_creating_geo_data_parquet)
        coords = op.prepare_coordinates(data_path)
        for uri, chunk in coords:
            yield from op.extract_rows(uri, chunk)

    def assertGeopointEqual(self, actual: str, expected: str) -> None:
        expected_json, actual_json = geojson.loads(expected), geojson.loads(actual)
        self.assertEqual(actual_json['type'], expected_json['type'])
        self.assertTrue(np.allclose(actual_json['coordinates'], expected_json['coordinates']))

    def assertRowsEqual(self, actual: t.Dict, expected: t.Dict):
        self.assertEqual(expected.keys(), actual.keys())
        for key in expected.keys():
            if isinstance(expected[key], str):
                # Handle Geopoint JSON strings...
                try:
                    self.assertGeopointEqual(actual[key], expected[key])
                except (simplejson.JSONDecodeError, json.JSONDecodeError, KeyError):
                    self.assertEqual(actual[key], expected[key])
                continue
            self.assertAlmostEqual(actual[key], expected[key], places=4)
            self.assertNotIsInstance(actual[key], np.dtype)
            self.assertNotIsInstance(actual[key], np.float64)
            self.assertNotIsInstance(actual[key], np.float32)


class ExtractRowsTest(ExtractRowsTestBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_20180101.nc'
        self.geo_data_parquet_path = f'{self.test_data_folder}/test_data_20180101_geo_data.parquet'

    def test_01_extract_rows(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_polygon=False,
                skip_creating_geo_data_parquet=False
            )
        )
        expected = {
            'd2m': 242.3035430908203,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
            'v10': 0.03294110298156738,
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-108.098837, 48.900826), (-108.098837, 49.099174),
                        (-107.901163, 49.099174), (-107.901163, 48.900826),
                        (-108.098837, 48.900826)]))
        }
        self.assertRowsEqual(actual, expected)

    def test_02_extract_rows__with_subset_variables(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                skip_creating_polygon=True,
                variables=['u10']
            )
        )
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0))),
            'geo_polygon': None
        }
        self.assertRowsEqual(actual, expected)

    def test_03_extract_rows__specific_area(self):
        actual = next(
            self.extract(
                self.test_data_path,
                area=[45, -103, 33, -92],
                geo_data_parquet_path='./geo_data.parquet',
                skip_creating_polygon=True
            )
        )
        expected = {
            'd2m': 246.19993591308594,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 45.0,
            'longitude': -103.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 2.73445987701416,
            'v10': 0.08277571201324463,
            'geo_point': geojson.dumps(geojson.Point((-103.0, 45.0))),
            'geo_polygon': None
        }
        self.assertRowsEqual(actual, expected)

    def test_04_extract_rows__specific_area_float_points(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path='./geo_data.parquet',
                area=[45.34, -103.45, 33.34, -92.87]
            )
        )
        expected = {
            'd2m': 246.47116088867188,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 45.20000076293945,
            'longitude': -103.4000015258789,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.94743275642395,
            'v10': -0.19749987125396729,
            'geo_point': geojson.dumps(geojson.Point((-103.400002, 45.200001))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-103.498839, 45.100827), (-103.498839, 45.299174),
                        (-103.301164, 45.299174), (-103.301164, 45.100827),
                        (-103.498839, 45.100827)]))
        }
        self.assertRowsEqual(actual, expected)

    def test_05_extract_rows_raises_error_when_geo_data_parquet_dimensions_mismatch(self):
        with self.assertRaisesRegex(ValueError, 'Length of values '):
            next(
                self.extract(
                    self.test_data_path,
                    area=[45, -103, 33, -92],
                    geo_data_parquet_path=self.geo_data_parquet_path,
                    skip_creating_geo_data_parquet=True,
                    skip_creating_polygon=True
                )
            )

    def test_06_extract_rows__specify_import_time(self):
        now = datetime.datetime.utcnow().isoformat()
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                import_time=now
            )
        )
        expected = {
            'd2m': 242.3035430908203,
            'data_import_time': now,
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
            'v10': 0.03294110298156738,
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-108.098837, 48.900826), (-108.098837, 49.099174),
                        (-107.901163, 49.099174), (-107.901163, 48.900826),
                        (-108.098837, 48.900826)]))
        }
        self.assertRowsEqual(actual, expected)

    def test_07_extract_rows_single_point(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_single_point.nc'
        self.geo_data_parquet_path = f'{self.test_data_folder}/test_data_single_point_geo_data.parquet'
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=False,
            )
        )
        expected = {
            'd2m': 242.3035430908203,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
            'v10': 0.03294110298156738,
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0))),
            'geo_polygon': None
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows_with_lat_lon_aliases(self):
        lat = np.array([12.0])
        lon = np.array([34.0])
        temps = np.array([[[250.0]]], dtype=np.float32)
        ds = xr.Dataset(
            {"temp": (('time', 'lat', 'lon'), temps)},
            coords={
                'time': np.array(['2018-01-02T06:00:00'], dtype='datetime64[ns]'),
                'lat': lat,
                'lon': lon,
            },
            attrs={'is_normalized': False},
        )
        geo_df = pd.DataFrame({
            'lat': [lat[0]],
            'lon': [lon[0]],
            'geo_point': [geojson.dumps(geojson.Point((lon[0], lat[0])))],
            'geo_polygon': [None],
        })
        temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        temp_parquet.close()
        self.addCleanup(lambda: os.path.exists(temp_parquet.name) and os.remove(temp_parquet.name))
        geo_df.to_parquet(temp_parquet.name, index=False)

        @contextlib.contextmanager
        def _fake_open_dataset(*args, **kwargs):
            yield ds.copy(deep=True)

        @contextlib.contextmanager
        def _fake_open_local(*args, **kwargs):
            yield temp_parquet.name

        with mock.patch('weather_mv.loader_pipeline.bq.open_dataset', side_effect=_fake_open_dataset), \
                mock.patch('weather_mv.loader_pipeline.bq.open_local', side_effect=_fake_open_local):
            actual = next(
                self.extract(
                    self.test_data_path,
                    geo_data_parquet_path=temp_parquet.name,
                    skip_creating_polygon=True,
                    skip_creating_geo_data_parquet=True,
                )
            )
        expected = {
            'temp': 250.0,
            'data_import_time': DEFAULT_IMPORT_TIME,
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': lat[0],
            'longitude': lon[0],
            'time': '2018-01-02T06:00:00+00:00',
            'geo_point': geo_df.iloc[0]['geo_point'],
            'geo_polygon': None,
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows_with_xy_coordinates_and_lat_lon_parquet(self):
        lat = np.array([12.0])
        lon = np.array([34.0])
        temps = np.array([[[250.0]]], dtype=np.float32)
        ds = xr.Dataset(
            {"temp": (('time', 'y', 'x'), temps)},
            coords={
                'time': np.array(['2018-01-02T06:00:00'], dtype='datetime64[ns]'),
                'y': lat,
                'x': lon,
            },
            attrs={'is_normalized': False},
        )
        geo_df = pd.DataFrame({
            'latitude': [lat[0]],
            'longitude': [lon[0]],
            'geo_point': [geojson.dumps(geojson.Point((lon[0], lat[0])))],
            'geo_polygon': [None],
        })
        temp_parquet = tempfile.NamedTemporaryFile(suffix='.parquet', delete=False)
        temp_parquet.close()
        self.addCleanup(lambda: os.path.exists(temp_parquet.name) and os.remove(temp_parquet.name))
        geo_df.to_parquet(temp_parquet.name, index=False)

        @contextlib.contextmanager
        def _fake_open_dataset(*args, **kwargs):
            yield ds.copy(deep=True)

        @contextlib.contextmanager
        def _fake_open_local(*args, **kwargs):
            yield temp_parquet.name

        with mock.patch('weather_mv.loader_pipeline.bq.open_dataset', side_effect=_fake_open_dataset), \
                mock.patch('weather_mv.loader_pipeline.bq.open_local', side_effect=_fake_open_local):
            actual = next(
                self.extract(
                    self.test_data_path,
                    geo_data_parquet_path=temp_parquet.name,
                    skip_creating_polygon=True,
                    skip_creating_geo_data_parquet=True,
                )
            )
        expected = {
            'temp': 250.0,
            'data_import_time': DEFAULT_IMPORT_TIME,
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': lat[0],
            'longitude': lon[0],
            'time': '2018-01-02T06:00:00+00:00',
            'geo_point': geo_df.iloc[0]['geo_point'],
            'geo_polygon': None,
        }
        self.assertRowsEqual(actual, expected)

    def test_08_extract_rows_nan(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_has_nan.nc'
        self.geo_data_parquet_path = f'{self.test_data_folder}/test_data_has_nan_geo_data.parquet'
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=False,
            )
        )
        expected = {
            'd2m': 242.3035430908203,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': None,
            'v10': 0.03294110298156738,
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-108.098837, 48.900826), (-108.098837, 49.099174),
                        (-107.901163, 49.099174), (-107.901163, 48.900826),
                        (-108.098837, 48.900826)]))
        }
        self.assertRowsEqual(actual, expected)

    def test_09_extract_rows__with_valid_lat_long_with_point(self):
        valid_lat_long = [[-90, 0], [-90, 1], [-45, -180], [-45, -45], [0, 0], [45, 45], [45, -180], [90, -1],
                          [90, 0]]
        actual_val = [
            '{"type": "Point", "coordinates": [0, -90]}',
            '{"type": "Point", "coordinates": [1, -90]}',
            '{"type": "Point", "coordinates": [-180, -45]}',
            '{"type": "Point", "coordinates": [-45, -45]}',
            '{"type": "Point", "coordinates": [0, 0]}',
            '{"type": "Point", "coordinates": [45, 45]}',
            '{"type": "Point", "coordinates": [-180, 45]}',
            '{"type": "Point", "coordinates": [-1, 90]}',
            '{"type": "Point", "coordinates": [0, 90]}'
        ]
        for actual, (lat, long) in zip(actual_val, valid_lat_long):
            with self.subTest():
                expected = fetch_geo_point(lat, long)
                self.assertEqual(actual, expected)

    def test_10_extract_rows__with_valid_lat_long_with_polygon(self):
        valid_lat_long = [[-90, 0], [-90, -180], [-45, -180], [-45, 180], [0, 0], [90, 180], [45, -180], [-90, 180],
                          [90, 1], [0, 180], [1, -180], [90, -180]]
        actual_val = [
            '{"type": "Polygon", "coordinates": [[[-1, 89], [-1, -89], [1, -89], [1, 89], [-1, 89]]]}',
            '{"type": "Polygon", "coordinates": [[[179, 89], [179, -89], [-179, -89], [-179, 89], [179, 89]]]}',
            '{"type": "Polygon", "coordinates": [[[179, -46], [179, -44], [-179, -44], [-179, -46], [179, -46]]]}',
            '{"type": "Polygon", "coordinates": [[[179, -46], [179, -44], [-179, -44], [-179, -46], [179, -46]]]}',
            '{"type": "Polygon", "coordinates": [[[-1, -1], [-1, 1], [1, 1], [1, -1], [-1, -1]]]}',
            '{"type": "Polygon", "coordinates": [[[179, 89], [179, -89], [-179, -89], [-179, 89], [179, 89]]]}',
            '{"type": "Polygon", "coordinates": [[[179, 44], [179, 46], [-179, 46], [-179, 44], [179, 44]]]}',
            '{"type": "Polygon", "coordinates": [[[179, 89], [179, -89], [-179, -89], [-179, 89], [179, 89]]]}',
            '{"type": "Polygon", "coordinates": [[[0, 89], [0, -89], [2, -89], [2, 89], [0, 89]]]}',
            '{"type": "Polygon", "coordinates": [[[179, -1], [179, 1], [-179, 1], [-179, -1], [179, -1]]]}',
            '{"type": "Polygon", "coordinates": [[[179, 0], [179, 2], [-179, 2], [-179, 0], [179, 0]]]}',
            '{"type": "Polygon", "coordinates": [[[179, 89], [179, -89], [-179, -89], [-179, 89], [179, 89]]]}'
        ]
        lat_grid_resolution = 1
        lon_grid_resolution = 1
        for actual, (lat, long) in zip(actual_val, valid_lat_long):
            with self.subTest():
                expected = fetch_geo_polygon(lat, long, lat_grid_resolution, lon_grid_resolution)
                self.assertEqual(actual, expected)

    def test_11_extract_rows__with_invalid_lat_lon(self):
        invalid_lat_long = [[-100, -2000], [-100, -500], [100, 500], [100, 2000]]
        for (lat, long) in invalid_lat_long:
            with self.subTest():
                with self.assertRaises(ValueError):
                    fetch_geo_point(lat, long)

    def test_12_extract_rows_zarr(self):
        input_path = os.path.join(self.test_data_folder, 'test_data.zarr')
        geo_data_parquet_path = os.path.join(self.test_data_folder, 'test_data_zarr_geo_data.parquet')
        actual = next(
            self.extract(
                input_path,
                geo_data_parquet_path=geo_data_parquet_path,
                skip_creating_geo_data_parquet=False,
                zarr=True
            )
        )
        expected = {
            'cape': 0.623291015625,
            'd2m': 237.5404052734375,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '1959-01-01T00:00:00+00:00',
            'data_uri': input_path,
            'latitude': 90,
            'longitude': 0,
            'time': '1959-01-01T00:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((0.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-0.124913, 89.875173), (-0.124913, -89.875173),
                        (0.124913, -89.875173), (0.124913, 89.875173),
                        (-0.124913, 89.875173)]))
        }
        self.assertRowsEqual(actual, expected)

    def test_13_droping_variable_while_opening_zarr(self):
        input_path = os.path.join(self.test_data_folder, 'test_data.zarr')
        geo_data_parquet_path = os.path.join(self.test_data_folder, 'test_data_zarr_geo_data.parquet')
        actual = next(
            self.extract(
                input_path,
                geo_data_parquet_path=geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                zarr=True,
                zarr_kwargs={'drop_variables': ['cape']}
            )
        )
        expected = {
            'd2m': 237.5404052734375,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '1959-01-01T00:00:00+00:00',
            'data_uri': input_path,
            'latitude': 90,
            'longitude': 0,
            'time': '1959-01-01T00:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((0.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-0.124913, 89.875173), (-0.124913, -89.875173),
                        (0.124913, -89.875173), (0.124913, 89.875173),
                        (-0.124913, 89.875173)]))
        }
        self.assertRowsEqual(actual, expected)


class ExtractRowsTifSupportTest(ExtractRowsTestBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_tif_time.tif'
        self.geo_data_parquet_path = f'{self.test_data_folder}/test_data_tif_time_geo_data.parquet'

    def test_01_extract_rows_with_end_time(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=False,
                tif_metadata_for_start_time='start_time',
                tif_metadata_for_end_time='end_time'
            )
        )
        expected = {
            'dewpoint_temperature_2m': 281.09349060058594,
            'temperature_2m': 296.8329772949219,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2020-07-01T00:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 42.09783344918844,
            'longitude': -123.66686981141397,
            'time': '2020-07-01T00:00:00+00:00',
            'valid_time': '2020-07-01T00:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((-123.66687, 42.097833))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-123.669853, 42.095605), (-123.669853, 42.100066),
                        (-123.663885, 42.100066), (-123.663885, 42.095605),
                        (-123.669853, 42.095605)]))
        }
        self.assertRowsEqual(actual, expected)

    def test_02_extract_rows_without_end_time(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                tif_metadata_for_start_time='start_time'
            )
        )
        expected = {
            'dewpoint_temperature_2m': 281.09349060058594,
            'temperature_2m': 296.8329772949219,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2020-07-01T00:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 42.09783344918844,
            'longitude': -123.66686981141397,
            'time': '2020-07-01T00:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((-123.66687, 42.097833))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (-123.669853, 42.095605), (-123.669853, 42.100066),
                        (-123.663885, 42.100066), (-123.663885, 42.095605),
                        (-123.669853, 42.095605)]))
        }
        self.assertRowsEqual(actual, expected)


class ExtractRowsGribSupportTest(ExtractRowsTestBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        self.geo_data_parquet_path = f'{self.test_data_folder}/test_data_grib_single_timestep_geo_data.parquet'

    @_handle_missing_grib_be
    def test_01_extract_rows(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=False,
                disable_grib_schema_normalization=True
            )
        )
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'number': 0,
            'step': 0.0,
            'surface': 0.0,
            'time': '2021-10-18T06:00:00+00:00',
            'valid_time': '2021-10-18T06:00:00+00:00',
            'z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_02_extract_rows__with_vars__excludes_non_index_coords__without_schema_normalization(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                disable_grib_schema_normalization=True, variables=['z']))
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_03_extract_rows__with_vars__includes_coordinates_in_vars__without_schema_normalization(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                disable_grib_schema_normalization=True,
                variables=['z', 'step']
            )
        )
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'step': 0,
            'z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_04_extract_rows__with_vars__excludes_non_index_coords__with_schema_normalization(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                variables=['z']
            )
        )
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'surface_0_00_instant_z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_05_extract_rows__with_vars__includes_coordinates_in_vars__with_schema_normalization(self):
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                variables=['z', 'step']
            )
        )
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'step': 0,
            'surface_0_00_instant_z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_06_multiple_editions__without_schema_normalization(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        self.geo_data_parquet_path = (
            f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep_geo_data.parquet'
        )
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=False,
                disable_grib_schema_normalization=True
            )
        )
        expected = {
            'cape': 0.0,
            'cbh': None,
            'cp': 0.0,
            'crr': 0.0,
            'd2m': 248.3846893310547,
            'data_first_step': '2021-12-10T12:00:00+00:00',
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_uri': self.test_data_path,
            'depthBelowLandLayer': 0.0,
            'dsrp': 0.0,
            'fdir': 0.0,
            'hcc': 0.0,
            'hcct': None,
            'hwbt0': 0.0,
            'i10fg': 7.41250467300415,
            'latitude': 90.0,
            'longitude': -180.0,
            'lsp': 1.1444091796875e-05,
            'mcc': 0.0,
            'msl': 99867.3125,
            'number': 0,
            'p3020': 20306.701171875,
            'sd': 0.0,
            'sf': 1.049041748046875e-05,
            'sp': 99867.15625,
            'step': 28800.0,
            'stl1': 251.02520751953125,
            'surface': 0.0,
            'swvl1': -1.9539930654413618e-13,
            't2m': 251.18968200683594,
            'tcc': 0.9609375,
            'tcrw': 0.0,
            'tcw': 2.314192295074463,
            'tcwv': 2.314192295074463,
            'time': '2021-12-10T12:00:00+00:00',
            'tp': 1.1444091796875e-05,
            'tsr': 0.0,
            'u10': -4.6668853759765625,
            'u100': -7.6197662353515625,
            'u200': -9.176498413085938,
            'v10': -3.2414093017578125,
            'v100': -4.1650390625,
            'v200': -3.6647186279296875,
            'valid_time': '2021-12-10T20:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_07_multiple_editions__with_schema_normalization(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        self.geo_data_parquet_path = (
            f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep_geo_data.parquet'
        )
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
            )
        )
        expected = {
            'surface_0_00_instant_cape': 0.0,
            'surface_0_00_instant_cbh': None,
            'surface_0_00_instant_cp': 0.0,
            'surface_0_00_instant_crr': 0.0,
            'surface_0_00_instant_d2m': 248.3846893310547,
            'data_first_step': '2021-12-10T12:00:00+00:00',
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_uri': self.test_data_path,
            'surface_0_00_instant_dsrp': 0.0,
            'surface_0_00_instant_fdir': 0.0,
            'surface_0_00_instant_hcc': 0.0,
            'surface_0_00_instant_hcct': None,
            'surface_0_00_instant_hwbt0': 0.0,
            'surface_0_00_instant_i10fg': 7.41250467300415,
            'latitude': 90.0,
            'longitude': -180.0,
            'surface_0_00_instant_lsp': 1.1444091796875e-05,
            'surface_0_00_instant_mcc': 0.0,
            'surface_0_00_instant_msl': 99867.3125,
            'number': 0,
            'surface_0_00_instant_p3020': 20306.701171875,
            'surface_0_00_instant_sd': 0.0,
            'surface_0_00_instant_sf': 1.049041748046875e-05,
            'surface_0_00_instant_sp': 99867.15625,
            'step': 28800.0,
            'depthBelowLandLayer_0_00_instant_stl1': 251.02520751953125,
            'depthBelowLandLayer_0_00_instant_swvl1': -1.9539930654413618e-13,
            'depthBelowLandLayer_7_00_instant_stl2': 253.54124450683594,
            'entireAtmosphere_0_00_instant_litoti': 0.0,
            'surface_0_00_instant_t2m': 251.18968200683594,
            'surface_0_00_instant_tcc': 0.9609375,
            'surface_0_00_instant_tcrw': 0.0,
            'surface_0_00_instant_tcw': 2.314192295074463,
            'surface_0_00_instant_tcwv': 2.314192295074463,
            'time': '2021-12-10T12:00:00+00:00',
            'surface_0_00_instant_tp': 1.1444091796875e-05,
            'surface_0_00_instant_tsr': 0.0,
            'surface_0_00_instant_u10': -4.6668853759765625,
            'surface_0_00_instant_u100': -7.6197662353515625,
            'surface_0_00_instant_u200': -9.176498413085938,
            'surface_0_00_instant_v10': -3.2414093017578125,
            'surface_0_00_instant_v100': -4.1650390625,
            'surface_0_00_instant_v200': -3.6647186279296875,
            'surface_0_00_instant_ptype': 5.0,
            'surface_0_00_instant_tprate': 0.0,
            'surface_0_00_instant_ceil': 179.17018127441406,
            'valid_time': '2021-12-10T20:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_08_multiple_editions__with_vars__includes_coordinates_in_vars__with_schema_normalization(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        self.geo_data_parquet_path = (
            f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep_geo_data.parquet'
        )
        actual = next(
            self.extract(
                self.test_data_path,
                geo_data_parquet_path=self.geo_data_parquet_path,
                skip_creating_geo_data_parquet=True,
                variables=['p3020', 'depthBelowLandLayer', 'step']
            )
        )
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-12-10T12:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'step': 28800.0,
            'surface_0_00_instant_p3020': 20306.701171875,
            'depthBelowLandLayer_0_00_instant_swvl1': -1.9539930654413618e-13,
            'depthBelowLandLayer_0_00_instant_stl1': 251.02520751953125,
            'depthBelowLandLayer_7_00_instant_stl2': 253.54124450683594,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
            'geo_polygon': geojson.dumps(geojson.Polygon([
                        (179.950014, 89.950028), (179.950014, -89.950028),
                        (-179.950014, -89.950028), (-179.950014, 89.950028),
                        (179.950014, 89.950028)]))

        }
        self.assertRowsEqual(actual, expected)


class ExtractRowsFromZarrTest(ExtractRowsTestBase):

    def setUp(self) -> None:
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()

    def tearDown(self) -> None:
        super().tearDown()
        self.tmpdir.cleanup()

    def test_01_extracts_rows(self):
        input_zarr = os.path.join(self.tmpdir.name, 'air_temp.zarr')

        ds = (
            xr.tutorial.open_dataset('air_temperature', cache_dir=self.test_data_folder)
            .isel(time=slice(0, 4), lat=slice(0, 4), lon=slice(0, 4))
            .rename(dict(lon='longitude', lat='latitude'))
        )
        ds.to_zarr(input_zarr)

        op = ToBigQuery.from_kwargs(
            first_uri=input_zarr, zarr_kwargs=dict(chunks=None, consolidated=True), dry_run=True, zarr=True,
            output_table='foo.bar.baz',
            variables=list(), area=list(), xarray_open_dataset_kwargs=dict(), import_time=None, infer_schema=False,
            tif_metadata_for_start_time=None, tif_metadata_for_end_time=None, skip_region_validation=True,
            disable_grib_schema_normalization=False,
            geo_data_parquet_path=os.path.join(self.tmpdir.name, 'geo_data.parquet'),
        )

        with TestPipeline() as p:
            result = p | op
            assert_that(result, is_not_empty())


if __name__ == '__main__':
    unittest.main()
