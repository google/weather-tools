"""Pipeline for reflecting lots of NetCDF objects into a BigQuery table."""

import argparse
import datetime
import itertools
import logging
import shutil
import tempfile
import typing as t

import apache_beam as beam
import apache_beam.metrics
import numpy as np
import xarray as xr
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp import gcsio
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import bigquery
from xarray.core.utils import ensure_us_time_resolution


DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()

DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    logging.basicConfig(level=(40 - verbosity * 10), format='%(asctime)-15s %(message)s')


def open_dataset(uri: str) -> xr.Dataset:
    """Open the netcdf at 'uri' and return its data as an xr.Dataset."""
    try:
        # Copy netcdf object from cloud storage, like GCS, to local file so
        # xarray can open it with mmap instead of copying the entire thing
        # into memory.
        with FileSystems().open(uri) as source_file:
            with tempfile.NamedTemporaryFile() as dest_file:
                shutil.copyfileobj(source_file, dest_file)
                dest_file.flush()
                dest_file.seek(0)
                xr_dataset: xr.Dataset = xr.open_dataset(dest_file.name)

                logging.info(f'opened dataset size: {xr_dataset.nbytes}')

                beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
                return xr_dataset
    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logging.error(f'Unable to open file from Cloud Storage: {e}')
        raise


def map_dtype_to_sql_type(var_type: np.dtype) -> str:
    """Maps a np.dtype to a suitable BigQuery column type."""
    if var_type in {np.dtype('float64'), np.dtype('float32')}:
        return 'FLOAT64'
    elif var_type in {np.dtype('<M8[ns]')}:
        return 'TIMESTAMP'
    elif var_type in {np.dtype('int8'), np.dtype('int16'), np.dtype('int32'), np.dtype('int64')}:
        return 'INT64'
    raise ValueError(f"Unknown mapping from '{var_type}' to SQL type")


def dataset_to_table_schema(ds: xr.Dataset) -> t.List[bigquery.SchemaField]:
    """Returns a BigQuery table schema able to store the data in 'ds'."""
    # Get the columns and data types for all variables in the dataframe
    columns = [
        (str(col), map_dtype_to_sql_type(ds.variables[col].dtype))
        for col in ds.variables.keys() if ds.variables[col].size != 0
    ]

    return to_table_schema(columns)


def to_table_schema(columns: t.List[t.Tuple[str, str]]) -> t.List[bigquery.SchemaField]:
    # Fields are all Nullable because data may have NANs. We treat these as null.
    fields = [
        bigquery.SchemaField(column, var_type, mode='NULLABLE')
        for column, var_type in columns
    ]

    # Add an extra columns for recording import metadata.
    fields.append(bigquery.SchemaField(DATA_IMPORT_TIME_COLUMN, 'TIMESTAMP', mode='NULLABLE'))
    fields.append(bigquery.SchemaField(DATA_URI_COLUMN, 'STRING', mode='NULLABLE'))
    fields.append(bigquery.SchemaField(DATA_FIRST_STEP, 'TIMESTAMP', mode='NULLABLE'))

    return fields


def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    logging.debug('Serializing to JSON')
    if type(value) == datetime.datetime or type(value) == str or type(value) == np.datetime64:
        # Assume strings are ISO format timestamps...
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            # ... if they are not, assume serialization is already correct.
            return value
        except TypeError:
            # ... maybe value is a numpy datetime ...
            try:
                value = ensure_us_time_resolution(value).astype(datetime.datetime)
            except AttributeError:
                # ... value is a datetime object, continue.
                pass

        # We use a string timestamp representation.
        if value.tzname():
            return value.isoformat()

        # We assume here that naive timestamps are in UTC timezone.
        return value.replace(tzinfo=datetime.timezone.utc).isoformat()
    elif np.isnan(value) or value is None:
        return None
    elif type(value) == np.float32 or type(value) == np.float64:
        return float(value)
    return value


def _only_target_vars(ds: xr.Dataset, data_vars: t.Optional[t.List[str]] = None) -> xr.Dataset:
    """If the user specifies target fields in the dataset, create a schema only from those fields."""

    # If there are no restrictions on data vars, include the whole dataset.
    if not data_vars:
        logging.info(f'target data_vars empty; using whole dataset; size: {ds.nbytes}')
        return ds

    assert all([dv in ds.data_vars for dv in data_vars]), 'Target variable must be in original dataset.'

    dropped_ds = ds.drop_vars([v for v in ds.data_vars if v not in data_vars])
    logging.info(f'target-only dataset size: {dropped_ds.nbytes}')

    return dropped_ds


def get_coordinates(ds: xr.Dataset) -> t.Iterator[t.Dict]:
    """Generates normalized coordinate dictionaries that can be used to index Datasets with `.loc[]`."""
    # Creates flattened iterator of all coordinate positions in the Dataset.
    #
    # Coordinates have been pre-processed to remove NaNs and to format datetime objects
    # to ISO format strings.
    #
    # Example: (-108.0, 49.0, '2018-01-02T22:00:00+00:00')
    coords = itertools.product(
        *(
            (
                to_json_serializable_type(v)
                for v in ensure_us_time_resolution(ds[c].variable.values).tolist()
            )
            for c in ds.coords
        )
    )
    # Give dictionary keys to a coordinate index.
    #
    # Example:
    #   {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T23:00:00+00:00'}
    yield from map(lambda it: dict(zip(ds.coords, it)), coords)


def extract_rows(uri: str, *,
                 variables: t.Optional[t.List[str]] = None,
                 area: t.Optional[t.List[int]] = None,
                 import_time: str = DEFAULT_IMPORT_TIME) -> t.Iterator[t.Dict]:
    """Reads named netcdf then yields each of its rows as a dict mapping column names to values."""
    logging.info(f'Extracting netcdf rows as dicts: {uri!r}.')
    data_ds: xr.Dataset = _only_target_vars(open_dataset(uri), variables)
    if area:
        n, w, s, e = area
        data_ds = data_ds.sel(latitude=slice(n, s), longitude=slice(w, e))
        logging.info(f'Data filtered by area, size: {data_ds.nbytes}')

    first_time_step = to_json_serializable_type(data_ds.time[0].values)

    def to_row(it: t.Dict) -> t.Dict:
        """Produce a single row, or a dictionary of all variables at a point."""

        # Use those index values to select a Dataset containing one row of data.
        row_ds = data_ds.loc[it]

        # Create a Name-Value map for data columns. Result looks like:
        # {'d': -2.0187, 'cc': 0.007812, 'z': 50049.8}
        row: t.Dict = row_ds.to_pandas().apply(to_json_serializable_type).to_dict()

        # Combine index and variable portions into a single row dict, and add import metadata.
        row.update(it)
        row[DATA_IMPORT_TIME_COLUMN] = import_time
        row[DATA_URI_COLUMN] = uri
        row[DATA_FIRST_STEP] = first_time_step

        # 'row' ends up looking like:
        # {'latitude': 88.0, 'longitude': 2.0, 'time': '2015-01-01 06:00:00', 'd': -2.0187, 'cc': 0.007812,
        #  'z': 50049.8, 'data_import_time': '2020-12-05 00:12:02.424573 UTC', ...}
        beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()
        return row

    yield from map(to_row, get_coordinates(data_ds))


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-mv',
        description='Weather Mover creates Google Cloud BigQuery tables from netcdf files in Google Cloud Storage.'
    )
    parser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=list(),
                        help='Target variables for the BigQuery schema. Default: will import all data variables as '
                             'columns.')
    parser.add_argument('-a', '--area', metavar='area', type=int, nargs='+', default=list(),
                        help='Target area in [N, W, S, E]. Default: Will include all available area.')
    parser.add_argument('-i', '--uris', type=str, required=True,
                        help="URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-")
    parser.add_argument('-o', '--output_table', type=str, required=True,
                        help=("Full name of destination BigQuery table (<project>.<dataset>.<table>). "
                              "Table will be created if it doesn't exist."))
    parser.add_argument('-t', '--temp_location', type=str, required=True,
                        help=("Cloud Storage path for temporary files. Must be a valid Cloud Storage URL"
                              ", beginning with gs://"))
    parser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                        help=("When writing data to BigQuery, record that data import occurred at this "
                              "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
    parser.add_argument('--infer_schema', action='store_true', default=False,
                        help='Download one file in the URI pattern and infer a schema from that file. Default: off')

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    if known_args.area:
        assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'
    # temp_location in known_args is passed to beam.io.WriteToBigQuery.
    # If the pipeline is run using the DataflowRunner, temp_location
    # must also be in pipeline_args.
    pipeline_args.append('--temp_location')
    pipeline_args.append(known_args.temp_location)

    # Before starting the pipeline, read one file and generate the BigQuery
    # table schema from it. Assumes the the number of matching uris is
    # manageable.
    all_uris = gcsio.GcsIO().list_prefix(known_args.uris)
    if not all_uris:
        raise FileNotFoundError(f"File prefix '{known_args.uris}' matched no objects")

    if known_args.variables and not known_args.infer_schema:
        logging.info('Creating schema from input variables.')
        table_schema = to_table_schema(
            [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
            [(var, 'FLOAT64') for var in known_args.variables]
        )
    else:
        logging.info('Inferring schema from data.')
        ds: xr.Dataset = _only_target_vars(open_dataset(next(iter(all_uris))), known_args.variables)
        table_schema = dataset_to_table_schema(ds)

    pipeline_options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Create the table in BigQuery
    try:
        table = bigquery.Table(known_args.output_table, schema=table_schema)
        table = bigquery.Client().create_table(table, exists_ok=True)
    except Exception as e:
        logging.error(f'Unable to create table in BigQuery: {e}')
        raise

    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'Create' >> beam.Create(all_uris.keys())
                | 'ExtractRows' >> beam.FlatMap(
                    extract_rows,
                    variables=known_args.variables,
                    area=known_args.area,
                    import_time=known_args.import_time)
                | 'WriteToBigQuery' >> WriteToBigQuery(
                    project=table.project,
                    dataset=table.dataset_id,
                    table=table.table_id,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_NEVER,
                    custom_gcs_temp_location=known_args.temp_location)
        )

    logging.info('Pipeline is finished.')
