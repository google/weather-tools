"""Pipeline for reflecting lots of NetCDF objects into a BigQuery table."""

import argparse
import io
import numpy as np
import pandas as pd
import typing as t
import xarray as xr


import apache_beam as beam
import apache_beam.metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp import gcsio
from numbers import Number

DATA_IMPORT_TIME_COLUMN = 'data_import_time'


def read_gcs_object(uri : str) -> bytes:
    """Returns the data for the storage object given by `uri`"""
    try:
        with gcsio.GcsIO().open(uri, 'rb') as f:
            data = f.read()
    except:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        raise
    else:
        beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
        return data


def netcdf_to_dataframe(netcdf_bytes : bytes) -> pd.DataFrame:
    """Returns a pandas dataframe containing the data from the given netcdf data"""
    xr_dataset: xr.Dataset = xr.load_dataset(io.BytesIO(netcdf_bytes))
    beam.metrics.Metrics.counter('Success', 'XarrayConversions').inc()
    return xr_dataset.to_dataframe()


def map_to_sql_type(value: Number) -> str:
    """Given a value, maps its python type to a suitable BigQuery column type"""
    if type(value) in {np.float32, np.float64, float}:
        return 'FLOAT64'
    elif type(value) is pd.Timestamp:
        return 'TIMESTAMP'
    elif type(value) in {int, np.int8, np.int16, np.int32, np.int64}:
        return 'INT64'
    raise ValueError(f"Unknown mapping from '{repr(type(value))}' to SQL type")


def dataframe_to_table_schema(df : pd.DataFrame) -> t.Dict:
    """Generates a BigQuery table schema from the columns and types of DataFrame `df`"""
    # Materialize the index as data columns.
    data = df.reset_index()

    fields = []
    # Examine one row of data to determine column names and types.
    index, row = next(data.iterrows())
    for column, value in row.to_dict().items():
        field = {'name': column, 'type': map_to_sql_type(value), 'mode': 'REQUIRED'}
        fields.append(field)

    # Add an extra column for recording import time.
    fields.append({'name': DATA_IMPORT_TIME_COLUMN, 'type': 'TIMESTAMP', 'mode': 'NULLABLE'})

    return {'fields': fields}


def read_netcdf_as_dataframe(uri : str) -> pd.DataFrame:
    """Returns a pandas dataframe containing the data from netcdf object at `uri`"""
    return netcdf_to_dataframe(read_gcs_object(uri))


def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    if type(value) == pd.Timestamp:
        # We use a string timestamp representation.
        if value.tzname():
            return value.isoformat()
        # We assume here that naive timestamps are in UTC timezone.
        return value.tz_localize(tz='UTC').isoformat()
    elif type(value) == np.float32 or type(value) == np.float64:
        return float(value)
    return value


def extract_rows_as_dicts(netcdf_data: bytes,
                          import_time: pd.Timestamp = pd.Timestamp(0)) -> t.Generator[t.Dict, None, None]:
    """Given netcdf object bytes, yields each of its rows as a dict mapping column names to values"""
    data_df: pd.DataFrame = netcdf_to_dataframe(netcdf_data).reset_index()

    for index, df_row in data_df.iterrows():
        row = df_row.to_dict()
        row[DATA_IMPORT_TIME_COLUMN] = import_time

        # Workaround for Beam being unable to serialize pd.Timestamp and np.float32 to JSON.
        # TODO(dlowell): find a better solution.
        for key, value in row.items():
            row[key] = to_json_serializable_type(value)

        beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()
        yield row


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--uris', type=str, required=True,
                        help="URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-")
    parser.add_argument('-o', '--output_table', type=str, required=True,
                        help=("Full name of destination BigQuery table (<project>.<dataset>.<table>). "
                              "Table will be created if it doesn't exist."))
    parser.add_argument('--import_time', type=pd.Timestamp, default=pd.Timestamp.now(tz="UTC"),
                        help=("When writing data to BigQuery, record that data import occurred at this "
                              "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    # Before starting the pipeline, read one file and generate the BigQuery
    # table schema from it. Assumes the the number of matching uris is
    # manageable.
    all_uris = gcsio.GcsIO().list_prefix(known_args.uris)
    if not all_uris:
        raise FileNotFoundError(f"File prefix '{known_args.uris}' matched no objects")
    table_schema = dataframe_to_table_schema(read_netcdf_as_dataframe(next(iter(all_uris))))

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'Create' >> beam.Create(all_uris.keys())
                | 'ReadNetcdfData' >> beam.Map(read_gcs_object)
                | 'ExtractRows' >> beam.FlatMap(extract_rows_as_dicts, import_time=known_args.import_time)
                | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                      table=known_args.output_table,
                      schema=table_schema,
                      write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                      method='STREAMING_INSERTS')
        )

