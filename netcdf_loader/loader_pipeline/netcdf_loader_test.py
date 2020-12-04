import unittest
import numpy as np
import pandas as pd
from google.cloud.bigquery import SchemaField

from netcdf_loader.loader_pipeline.netcdf_loader import dataframe_to_table_schema

class NetcdfLoaderTest(unittest.TestCase):

    def test_schema_generation(self):
        d = {'a': [pd.Timestamp(0)], 'b': [np.float32(1.0)],
             'c': [np.float64(2.0)], 'd': [3.0]}
        df = pd.DataFrame(data=d)
        schema = dataframe_to_table_schema(df)
        expected_schema = [SchemaField('index', 'INT64', 'REQUIRED', None, (), None),
                           SchemaField('a', 'TIMESTAMP', 'REQUIRED', None, (), None),
                           SchemaField('b', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('c', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('d', 'FLOAT64', 'REQUIRED', None, (), None),
                           SchemaField('data_import_time', 'TIMESTAMP', 'NULLABLE', None, (), None)]
        self.assertListEqual(schema, expected_schema)


if __name__ == '__main__':
    unittest.main()
