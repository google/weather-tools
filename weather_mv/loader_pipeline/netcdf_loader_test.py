import datetime
import typing as t
import unittest
from collections import Counter

import numpy as np
import pandas as pd
import xarray as xr
from google.cloud.bigquery import SchemaField

import weather_mv
from weather_mv.loader_pipeline.netcdf_loader import (
    dataset_to_table_schema,
    _only_target_vars,
    extract_rows,
    get_coordinates
)


class SchemaCreationTests(unittest.TestCase):

    def setUp(self) -> None:
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
        ]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__missing_target(self):
        with self.assertRaisesRegex(AssertionError, 'Target variable must be in original dataset.'):
            target_variables = ['a', 'foobar', 'd']
            _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)


class ExtractRowsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_data_path = f'{next(iter(weather_mv.__path__))}/test_data/test_data_20180101.nc'

    def assertRowsEqual(self, actual: t.Dict, expected: t.Dict):
        for key in expected.keys():
            self.assertAlmostEqual(actual[key], expected[key], places=4)

    def test_extract_rows(self):
        actual = next(extract_rows(self.test_data_path))
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
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__with_subset_variables(self):
        actual = next(extract_rows(self.test_data_path, variables=['u10']))
        expected = {
            'data_import_time': '1970-01-01T00:00:00+00:00',
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__specific_area(self):
        actual = next(extract_rows(self.test_data_path, area=[45, -103, 33, -92]))
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
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__specify_import_time(self):
        now = datetime.datetime.utcnow().isoformat()
        actual = next(extract_rows(self.test_data_path, import_time=now))
        expected = {
            'd2m': 242.3035430908203,
            'data_import_time': now,
            'data_first_step': '2018-01-02T06:00:00+00:00',
            'data_uri': self.test_data_path,
            'latitude': 49.0,
            'longitude': -108.0,
            'time': '2018-01-02T06:00:00+00:00',
            'u10': 3.4776244163513184,
            'v10': 0.03294110298156738
        }
        self.assertRowsEqual(actual, expected)

    def test_extract_rows__no_duplicate_coordinates(self):
        ds = xr.open_dataset(self.test_data_path)

        # Assert that all the coordinates are unique.
        counts = Counter([tuple(c.values()) for c in get_coordinates(ds)])
        self.assertTrue(all((c == 1 for c in counts.values())))


if __name__ == '__main__':
    unittest.main()
