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
import datetime
import logging
import typing as t
import unittest
from contextlib import contextmanager
from time import perf_counter
import geojson

import numpy as np
import pandas as pd
import xarray as xr
from google.cloud.bigquery import SchemaField

from .bq import (
    DEFAULT_IMPORT_TIME,
    dataset_to_table_schema,
    extract_rows,
    fetch_geo_point,
    prepare_coordinates,
)
from .sinks_test import TestDataBase, _handle_missing_grib_be
from .util import _only_target_vars

logger = logging.getLogger(__name__)


class SchemaCreationTests(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_dataset = {
            "coords": {"a": {"dims": ("a",), "data": [pd.Timestamp(0)], "attrs": {}}},
            "attrs": {},
            "dims": "a",
            "data_vars": {
                "b": {"dims": ("a",), "data": [np.float32(1.0)]},
                "c": {"dims": ("a",), "data": [np.float64(2.0)]},
                "d": {"dims": ("a",), "data": [3.0]},
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
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__missing_target(self):
        with self.assertRaisesRegex(AssertionError, 'Target variable must be in original dataset.'):
            target_variables = ['a', 'foobar', 'd']
            _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)

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

        ]
        self.assertListEqual(schema, expected_schema)


class ExtractRowsTestBase(TestDataBase):

    def extract(self, data_path, *, variables=None, area=None, open_dataset_kwargs=None,
                import_time=DEFAULT_IMPORT_TIME, tif_metadata_for_datetime=None) -> t.Iterator[t.Dict]:
        coords = prepare_coordinates(data_path, coordinate_chunk_size=1000, area=area, import_time=import_time,
                                     open_dataset_kwargs=open_dataset_kwargs, variables=variables,
                                     tif_metadata_for_datetime=tif_metadata_for_datetime)
        for uri, import_time, first_time_step, chunk in coords:
            yield from extract_rows(uri, import_time=import_time, first_time_step=first_time_step, rows=chunk)

    def assertRowsEqual(self, actual: t.Dict, expected: t.Dict):
        self.assertEqual(expected.keys(), actual.keys())
        for key in expected.keys():
            if isinstance(expected[key], str):
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

    def test_extract_rows(self):
        actual = next(self.extract(self.test_data_path))
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
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__with_subset_variables(self):
        actual = next(self.extract(self.test_data_path, variables=['u10']))
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0))),
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__specific_area(self):
        actual = next(self.extract(self.test_data_path, area=[45, -103, 33, -92]))
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
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__specify_import_time(self):
        now = datetime.datetime.utcnow().isoformat()
        actual = next(self.extract(self.test_data_path, import_time=now))
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
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0)))
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows_single_point(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_single_point.nc'
        actual = next(self.extract(self.test_data_path))
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
            'geo_point': geojson.dumps(geojson.Point((-108.0, 49.0)))
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows_nan(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_has_nan.nc'
        actual = next(self.extract(self.test_data_path))
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
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__with_valid_lat_long(self):
        valid_lat_long = [[-90, -360], [-90, -359], [-45, -180], [-45, -45], [0, 0], [45, 45], [45, 180], [90, 359],
                          [90, 360]]
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

    def test_extract_rows__with_invalid_lat(self):
        invalid_lat_long = [[-100, -2000], [-100, -500], [100, 500], [100, 2000]]
        for (lat, long) in invalid_lat_long:
            with self.subTest():
                with self.assertRaises(ValueError):
                    fetch_geo_point(lat, long)


class ExtractRowsTifSupportTest(ExtractRowsTestBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_tif_start_time.tif'

    def test_extract_rows(self):
        actual = next(self.extract(self.test_data_path, tif_metadata_for_datetime='start_time'))
        expected = {
            'dewpoint_temperature_2m': 281.09349060058594,
            'temperature_2m': 296.8329772949219,
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2020-07-01T00:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 42.09783344918844,
            'longitude': -123.66686981141397,
            'time': '2020-07-01T00:00:00+00:00',
            'geo_point': geojson.dumps(geojson.Point((-123.66687, 42.097833)))
        }
        self.assertRowsEqual(actual, expected)


@contextmanager
def timing(description: str) -> t.Iterator[None]:
    logging.info(f"{description}: Starting profiler")
    start = perf_counter()
    yield
    elapsed_time = perf_counter() - start
    logging.info(f"{description}: Elapsed time: {elapsed_time:0.4f} seconds")


class ExtractRowsGribSupportTest(ExtractRowsTestBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'

    @_handle_missing_grib_be
    def test_extract_rows(self):
        actual = next(self.extract(self.test_data_path))
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
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_extract_rows__with_vars__excludes_non_index_coords(self):
        actual = next(self.extract(self.test_data_path, variables=['z']))
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_extract_rows__with_vars__includes_coordinates_in_vars(self):
        actual = next(self.extract(self.test_data_path, variables=['z', 'step']))
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2021-10-18T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 90.0,
            'longitude': -180.0,
            'step': 0,
            'z': 1.42578125,
            'geo_point': geojson.dumps(geojson.Point((-180.0, 90.0))),
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_multiple_editions(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        actual = next(self.extract(self.test_data_path))
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
        }
        self.assertRowsEqual(actual, expected)

    @_handle_missing_grib_be
    def test_timing_profile(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        counter = 0
        i = self.extract(self.test_data_path)
        # Read once to avoid counting dataset open times, etc.
        _ = next(i)
        with timing('Loop'):
            for v in i:
                # Don't do everything, 10K coordinates is enough for testing.
                if counter >= 10000:
                    break
                if counter % 1000 == 0:
                    logging.info(f'Processed {counter // 1000}k coordinates...')
                counter += 1
        logging.info(f'Finished {counter}')


if __name__ == '__main__':
    unittest.main()
