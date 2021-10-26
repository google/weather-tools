import unittest

import numpy as np
import pandas as pd
import xarray as xr
from google.cloud.bigquery import SchemaField

from loader_pipeline.netcdf_loader import dataset_to_table_schema, _only_target_vars


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
        expected_schema = [SchemaField('a', 'TIMESTAMP', 'REQUIRED', None, (), None),
                           SchemaField('b', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('c', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('d', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None)]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__with_target_columns(self):
        target_variables = ['c', 'd']
        ds = _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)
        schema = dataset_to_table_schema(ds)
        expected_schema = [SchemaField('a', 'TIMESTAMP', 'REQUIRED', None, (), None),
                           SchemaField('c', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('d', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None)]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__no_targets_specified(self):
        target_variables = []  # intentionally empty
        ds = _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)
        schema = dataset_to_table_schema(ds)
        expected_schema = [SchemaField('a', 'TIMESTAMP', 'REQUIRED', None, (), None),
                           SchemaField('b', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('c', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('d', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None)]
        self.assertListEqual(schema, expected_schema)

    def test_schema_generation__missing_target(self):
        with self.assertRaisesRegex(AssertionError, 'Target variable must be in original dataset.'):
            target_variables = ['a', 'foobar', 'd']
            _only_target_vars(xr.Dataset.from_dict(self.test_dataset), target_variables)


if __name__ == '__main__':
    unittest.main()
