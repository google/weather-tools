import unittest
import numpy as np
import pandas as pd

from netcdf_loader.loader_pipeline.netcdf_loader import dataframe_to_table_schema

class NetcdfLoaderTest(unittest.TestCase):

    def test_schema_generation(self):
        d = {'a': [pd.Timestamp(0)], 'b': [np.float32(1.0)],
             'c': [np.float64(2.0)], 'd': [3.0]}
        df = pd.DataFrame(data=d)
        schema = dataframe_to_table_schema(df)
        expected_schema = {'fields':
             [{'name': 'index', 'type': 'INT64', 'mode': 'REQUIRED'},
              {'name': 'a', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
              {'name': 'b', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
              {'name': 'c', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
              {'name': 'd', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
              {'name': 'data_import_time', 'type': 'TIMESTAMP',
               'mode': 'NULLABLE'}]}
        self.assertDictEqual(schema, expected_schema)


if __name__ == '__main__':
    unittest.main()
