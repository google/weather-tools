import unittest
import numpy as np
import pandas as pd
import xarray as xr
from google.cloud.bigquery import SchemaField

from loader_pipeline.netcdf_loader import dataset_to_table_schema


class NetcdfLoaderTest(unittest.TestCase):

    def test_schema_generation(self):
        d = {
            "coords": {"a": {"dims": ("a",), "data": [pd.Timestamp(0)], "attrs": {}}},
            "attrs": {},
            "dims": "a",
            "data_vars": {
                "b": {"dims": ("a",), "data": [np.float32(1.0)]},
                "c": {"dims": ("a",), "data": [np.float64(2.0)]},
                "d": {"dims": ("a",), "data": [3.0]},
            }
        }

        ds = xr.Dataset.from_dict(d)
        schema = dataset_to_table_schema(ds)
        expected_schema = [SchemaField('a', 'TIMESTAMP', 'REQUIRED', None, (), None),
                           SchemaField('b', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('c', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('d', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None)]
        self.assertListEqual(schema, expected_schema)


if __name__ == '__main__':
    unittest.main()
