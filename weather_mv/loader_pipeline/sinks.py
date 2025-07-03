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

import abc
import argparse
import contextlib
import dataclasses
import datetime
import inspect
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import typing as t
from urllib.parse import urlparse

import apache_beam as beam
import cfgrib
import numpy as np
import rasterio
import rioxarray
import xarray as xr
from apache_beam.io.filesystem import CompressionTypes, FileSystem, CompressedFile, DEFAULT_READ_BUFFER_SIZE
from apache_beam.io.filesystems import FileSystems
from google.cloud import storage
from pyproj import Transformer

TIF_TRANSFORM_CRS_TO = "EPSG:4326"
# A constant for all the things in the coords key set that aren't the level name.
DEFAULT_COORD_KEYS = frozenset(('latitude', 'time', 'step', 'valid_time', 'longitude', 'number'))
DEFAULT_TIME_ORDER_LIST = ['%Y', '%m', '%d', '%H', '%M', '%S']

logger = logging.getLogger(__name__)


class KwargsFactoryMixin:
    """Adds a factory method to classes or dataclasses for key-word args."""
    @classmethod
    def from_kwargs(cls, **kwargs):
        if dataclasses.is_dataclass(cls):
            fields = [f.name for f in dataclasses.fields(cls)]
        else:
            fields = inspect.signature(cls.__init__).parameters.keys()

        return cls(**{k: v for k, v, in kwargs.items() if k in fields})


@dataclasses.dataclass
class ToDataSink(abc.ABC, beam.PTransform, KwargsFactoryMixin):
    first_uri: str
    dry_run: bool
    zarr: bool
    zarr_kwargs: t.Dict

    @classmethod
    @abc.abstractmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser) -> None:
        pass

    @classmethod
    @abc.abstractmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_options: t.List[str]) -> None:
        pass


def _make_grib_dataset_inmem(grib_ds: xr.Dataset) -> xr.Dataset:
    """Copies all the vars in-memory to reduce disk seeks every time a single row is processed.

    This also removes the need to keep the backing temp source file around.
    """
    data_ds = grib_ds.copy(deep=True)
    for v in grib_ds.variables:
        if v not in data_ds.coords:
            data_ds[v].variable.values = grib_ds[v].variable.values
    return data_ds


def match_datetime(file_name: str, regex_expression: str) -> datetime.datetime:
    """Matches the regex string given and extracts the datetime object.

    Args:
        file_name: File name from which you want to extract datetime.
        regex_expression: Regex expression for extracting datetime from the filename.

    Returns:
        A datetime object after extracting from the filename.
    """

    def rearrange_time_list(order_list: t.List, time_list: t.List) -> t.List:
        if order_list == DEFAULT_TIME_ORDER_LIST:
            return time_list
        new_time_list = []
        for i, j in zip(order_list, time_list):
            dst = DEFAULT_TIME_ORDER_LIST.index(i)
            new_time_list.insert(dst, j)
        return new_time_list

    char_to_replace = {
        '%Y': ['([0-9]{4})', [0, 1978]],
        '%m': ['([0-9]{2})', [1, 1]],
        '%d': ['([0-9]{2})', [2, 1]],
        '%H': ['([0-9]{2})', [3, 0]],
        '%M': ['([0-9]{2})', [4, 0]],
        '%S': ['([0-9]{2})', [5, 0]],
        '*': ['.*']
    }

    missing_idx_list = []
    temp_expression = regex_expression

    for key, value in char_to_replace.items():
        if key != '*' and regex_expression.find(key) == -1:
            missing_idx_list.append(value[1])
        else:
            temp_expression = temp_expression.replace(key, value[0])

    regex_matches = re.findall(temp_expression, file_name)[0]

    order_list = [f'%{char}' for char in re.findall(r'%(\w{1})', regex_expression)]

    time_list = list(map(int, regex_matches))
    time_list = rearrange_time_list(order_list, time_list)

    if missing_idx_list:
        for [idx, val] in missing_idx_list:
            time_list.insert(idx, val)

    return datetime.datetime(*time_list)


def _preprocess_tif(
    ds: xr.Dataset,
    tif_metadata_for_start_time: str,
    tif_metadata_for_end_time: str,
    uri: str,
    initialization_time_regex: str,
    forecast_time_regex: str
) -> xr.Dataset:
    """Transforms (y, x) coordinates into (lat, long) and adds bands data in data variables.

    This also retrieves datetime from tif's metadata and stores it into dataset.
    """

    def _replace_dataarray_names_with_long_names(ds: xr.Dataset):
        rename_dict = {var_name: ds[var_name].attrs.get('long_name', var_name) for var_name in ds.variables}
        return ds.rename(rename_dict)

    y, x = np.meshgrid(ds['y'], ds['x'])
    transformer = Transformer.from_crs(ds.spatial_ref.crs_wkt, TIF_TRANSFORM_CRS_TO, always_xy=True)
    lon, lat = transformer.transform(x, y)

    ds['y'] = lat[0, :]
    ds['x'] = lon[:, 0]
    ds = ds.rename({'y': 'latitude', 'x': 'longitude'})

    ds = ds.squeeze().drop_vars('spatial_ref')

    ds = _replace_dataarray_names_with_long_names(ds)

    end_time = None
    start_time = None
    if initialization_time_regex and forecast_time_regex:
        try:
            start_time = match_datetime(uri, initialization_time_regex)
        except Exception:
            raise RuntimeError("Wrong regex passed in --initialization_time_regex.")
        try:
            end_time = match_datetime(uri, forecast_time_regex)
        except Exception:
            raise RuntimeError("Wrong regex passed in --forecast_time_regex.")
        ds.attrs['start_time'] = start_time
        ds.attrs['end_time'] = end_time

    init_time = None
    forecast_time = None
    coords = {}
    try:
        # if start_time/end_time is in integer milliseconds
        init_time = (int(start_time.timestamp()) if start_time is not None
                     else int(ds.attrs[tif_metadata_for_start_time]) / 1000.0)
        coords['time'] = datetime.datetime.utcfromtimestamp(init_time)

        if tif_metadata_for_end_time:
            forecast_time = (int(end_time.timestamp()) if end_time is not None
                             else int(ds.attrs[tif_metadata_for_end_time]) / 1000.0)
            coords['valid_time'] = datetime.datetime.utcfromtimestamp(forecast_time)

        ds = ds.assign_coords(coords)
    except KeyError as e:
        raise RuntimeError(f"Invalid datetime metadata of tif: {e}.")
    except ValueError:
        try:
            # if start_time/end_time is in UTC string format
            init_time = (int(start_time.timestamp()) if start_time is not None
                         else datetime.datetime.strptime(ds.attrs[tif_metadata_for_start_time],
                                                         '%Y-%m-%dT%H:%M:%SZ'))
            coords['time'] = init_time

            if tif_metadata_for_end_time:
                forecast_time = (int(end_time.timestamp()) if end_time is not None
                                 else datetime.datetime.strptime(ds.attrs[tif_metadata_for_end_time],
                                                                 '%Y-%m-%dT%H:%M:%SZ'))
                coords['valid_time'] = forecast_time

            ds = ds.assign_coords(coords)
        except ValueError as e:
            raise RuntimeError(f"Invalid datetime value in tif's metadata: {e}.")

    return ds


def _to_utc_timestring(np_time: np.datetime64) -> str:
    """Turn a numpy datetime64 into UTC timestring."""
    timestamp = float((np_time - np.datetime64(0, 's')) / np.timedelta64(1, 's'))
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def _add_is_normalized_attr(ds: xr.Dataset, value: bool) -> xr.Dataset:
    """Adds is_normalized to the attrs of the xarray.Dataset.

    This attribute represents if the dataset is the merged dataset (i.e. created by combining N datasets,
    specifically for normalizing grib's schema) or not.
    """
    ds.attrs['is_normalized'] = value
    return ds


def _is_3d_da(da):
    """Checks whether data array is 3d or not."""
    return len(da.shape) == 3


def __normalize_grib_dataset(filename: str,
                             group_common_hypercubes: t.Optional[bool] = False) -> t.Union[xr.Dataset,
                                                                                           t.List[xr.Dataset]]:
    """Reads a list of datasets and merge them into a single dataset."""
    _level_data_dict = {}

    list_ds = cfgrib.open_datasets(filename)
    ds_attrs = list_ds[0].attrs
    dv_units_dict = {}

    for ds in list_ds:
        coords_set = set(ds.coords.keys())
        level_set = coords_set.difference(DEFAULT_COORD_KEYS)
        level = level_set.pop()

        # Now look at what data vars are in each level.
        for key in ds.data_vars.keys():
            da = ds[key]  # The data array
            attrs = da.attrs  # The metadata for this dataset.

            # Also figure out the forecast hour for this file.
            forecast_hour = int(da.step.values / np.timedelta64(1, 'h'))

            # We are going to treat the time field as start_time and the
            # valid_time field as the end_time for EE purposes. Also, get the
            # times into UTC timestrings.
            start_time = _to_utc_timestring(da.time.values)
            end_time = _to_utc_timestring(da.valid_time.values)

            attrs['forecast_hour'] = forecast_hour  # Stick the forecast hour in the metadata as well, that's useful.
            attrs['start_time'] = start_time
            attrs['end_time'] = end_time

            if group_common_hypercubes:
                attrs['level'] = level  # Adding the level in the metadata, will remove in further steps.
                attrs['is_normalized'] = True  # Adding the 'is_normalized' attribute in the metadata.

            if level not in _level_data_dict:
                _level_data_dict[level] = []

            no_of_levels = da.shape[0] if _is_3d_da(da) else 1

            # Deal with the randomness that is 3d data interspersed with 2d.
            # For 3d data, we need to extract ds for each value of level.
            for sub_c in range(no_of_levels):
                copied_da = da.copy(deep=True)
                height = copied_da.coords[level].data.flatten()[sub_c]

                # Some heights are super small, but we can't have decimal points
                # in channel names & schema fields for Earth Engine & BigQuery respectively , so mostly cut off the
                # fractional part, unless we are forced to keep it. If so,
                # replace the decimal point with yet another underscore.
                if height >= 10:
                    height_string = f'{height:.0f}'
                else:
                    height_string = f'{height:.2f}'.replace('.', '_')

                channel_name = f'{level}_{height_string}_{attrs["GRIB_stepType"]}_{key}'
                logger.debug('Found channel %s', channel_name)

                # Add the height as a metadata field, that seems useful.
                copied_da.attrs['height'] = height_string

                # Add the units of each band as a metadata field.
                dv_units_dict['unit_'+channel_name] = None
                if 'units' in attrs:
                    dv_units_dict['unit_'+channel_name] = attrs['units']

                copied_da.name = channel_name
                if _is_3d_da(da):
                    copied_da = copied_da.sel({level: height})
                copied_da = copied_da.drop_vars(level)

                _level_data_dict[level].append(copied_da)

    _data_array_list = []
    _data_array_list = [xr.merge(list_da) for list_da in _level_data_dict.values()]

    if not group_common_hypercubes:
        # Stick the forecast hour, start_time, end_time, data variables units
        # in the ds attrs as well, that's useful.
        ds_attrs['forecast_hour'] = _data_array_list[0].attrs['forecast_hour']
        ds_attrs['start_time'] = _data_array_list[0].attrs['start_time']
        ds_attrs['end_time'] = _data_array_list[0].attrs['end_time']
        ds_attrs.update(**dv_units_dict)

        merged_dataset = xr.merge(_data_array_list)
        merged_dataset.attrs.clear()
        merged_dataset.attrs.update(ds_attrs)
        return merged_dataset

    return _data_array_list


def __open_dataset_file(filename: str,
                        uri_extension: str,
                        disable_grib_schema_normalization: bool,
                        open_dataset_kwargs: t.Optional[t.Dict] = None,
                        group_common_hypercubes: t.Optional[bool] = False) -> t.Union[xr.Dataset, t.List[xr.Dataset]]:
    """Opens the dataset at 'uri' and returns a xarray.Dataset."""
    # add a flag to group common hypercubes
    if group_common_hypercubes:
        return __normalize_grib_dataset(filename, group_common_hypercubes)

    if open_dataset_kwargs:
        return _add_is_normalized_attr(xr.open_dataset(filename, **open_dataset_kwargs), False)

    # If URI extension is .tif, try opening file by specifying engine="rasterio".
    if uri_extension in ['.tif', '.tiff']:
        return _add_is_normalized_attr(rioxarray.open_rasterio(filename, band_as_variable=True), False)

    # If no open kwargs are available and URI extension is other than tif, make educated guesses about the dataset.
    try:
        return _add_is_normalized_attr(xr.open_dataset(filename), False)
    except ValueError as e:
        e_str = str(e)
        if not ("Consider explicitly selecting one of the installed engines" in e_str and "cfgrib" in e_str):
            raise

    if not disable_grib_schema_normalization:
        logger.warning("Assuming grib.")
        logger.info("Normalizing the grib schema, name of the data variables will look like "
                    "'<level>_<height>_<attrs['GRIB_stepType']>_<key>'.")
        return _add_is_normalized_attr(__normalize_grib_dataset(filename), True)

    # Trying with explicit engine for cfgrib.
    try:
        return _add_is_normalized_attr(
            xr.open_dataset(filename, engine='cfgrib', backend_kwargs={'indexpath': ''}),
            False)
    except ValueError as e:
        if "multiple values for key 'edition'" not in str(e):
            raise
    logger.warning("Assuming grib edition 1.")
    # Try with edition 1
    # Note: picking edition 1 for now as it seems to get the most data/variables for ECMWF realtime data.
    return _add_is_normalized_attr(
        xr.open_dataset(filename, engine='cfgrib', backend_kwargs={'filter_by_keys': {'edition': 1}, 'indexpath': ''}),
        False)


def upload(src: str, dst: str) -> None:
    """Uploads a file to the specified GCS bucket destination."""
    subprocess.run(f'gsutil -m cp {src} {dst}'.split(), check=True, capture_output=True, text=True, input="n/n")

def path_exists(path: str, force_regrid: bool = False) -> bool:
    """Check if path exists. Pass force_regrid to skip checking."""
    if force_regrid:
        return False
    matches = FileSystems().match([path])
    assert len(matches) == 1
    return len(matches[0].metadata_list) > 0

def copy(src: str, dst: str, apply_bz2_compression: bool = False) -> None:
    """Copy data via `gsutil`."""
    logger.info(f'Copying {src} to {dst} ...')
    errors: t.List[subprocess.CalledProcessError] = []

    if apply_bz2_compression:
        logger.info(f'Applying bz2 compression over the {src} file ...')
        subprocess.run(
            f"bzip2 -k {src}".split()
        )
        # Now it would have .bz2 file...
        src = src + '.bz2'

    for cmd in ['gsutil -m cp']:
        try:
            subprocess.run(cmd.split() + [src, dst], check=True, capture_output=True, text=True, input="n/n")
            if apply_bz2_compression:
                logger.info(f'Cleaning up {src} ...')
                os.remove(src)
            return
        except subprocess.CalledProcessError as e:
            errors.append(e)

    msg = f'Failed to copy file {src!r} to {dst!r}'
    err_msgs = ', '.join(map(lambda err: repr(err.stderr), errors))
    logger.error(f'{msg} due to {err_msgs}.')
    raise EnvironmentError(msg, errors)


@contextlib.contextmanager
def open_local(uri: str) -> t.Iterator[str]:
    """Copy a cloud object (e.g. a netcdf, grib, or tif file) from cloud storage, like GCS, to local file."""
    with tempfile.NamedTemporaryFile() as dest_file:
        # Transfer data with gsutil.
        copy(uri, dest_file.name)

        # Check if data is compressed. Decompress the data using the same methods that beam's
        # FileSystems interface uses.
        compression_type = FileSystem._get_compression_type(uri, CompressionTypes.AUTO)
        if compression_type == CompressionTypes.UNCOMPRESSED:
            yield dest_file.name
            return

        dest_file.seek(0)
        with tempfile.NamedTemporaryFile() as dest_uncompressed:
            with CompressedFile(open(dest_file.name, 'rb'), compression_type=compression_type) as dcomp:
                shutil.copyfileobj(dcomp, dest_uncompressed, DEFAULT_READ_BUFFER_SIZE)
                yield dest_uncompressed.name


@contextlib.contextmanager
def open_dataset(uri: str,
                 open_dataset_kwargs: t.Optional[t.Dict] = None,
                 disable_grib_schema_normalization: bool = False,
                 tif_metadata_for_start_time: t.Optional[str] = None,
                 tif_metadata_for_end_time: t.Optional[str] = None,
                 initialization_time_regex: t.Optional[str] = None,
                 forecast_time_regex: t.Optional[str] = None,
                 group_common_hypercubes: t.Optional[bool] = False,
                 is_zarr: bool = False) -> t.Iterator[xr.Dataset]:
    """Open the dataset at 'uri' and return a xarray.Dataset."""
    try:
        local_open_dataset_kwargs = start_date = end_date = None
        if open_dataset_kwargs is not None:
            local_open_dataset_kwargs = open_dataset_kwargs.copy()
            start_date = local_open_dataset_kwargs.pop('start_date', None)
            end_date = local_open_dataset_kwargs.pop('end_date', None)

        if is_zarr:
            ds: xr.Dataset = _add_is_normalized_attr(xr.open_dataset(uri, engine='zarr',
                                                                     **local_open_dataset_kwargs), False)
            if start_date is not None and end_date is not None:
                ds = ds.sel(time=slice(start_date, end_date))
            beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
            yield ds
            ds.close()
            return
        with open_local(uri) as local_path:
            _, uri_extension = os.path.splitext(uri)
            xr_datasets: xr.Dataset = __open_dataset_file(local_path,
                                                          uri_extension,
                                                          disable_grib_schema_normalization,
                                                          local_open_dataset_kwargs,
                                                          group_common_hypercubes)
            # Extracting dtype, crs and transform from the dataset.
            rasterio_error = False
            try:
                with rasterio.open(local_path, 'r') as f:
                    dtype, crs, transform = (f.profile.get(key) for key in ['dtype', 'crs', 'transform'])
            except rasterio.errors.RasterioIOError:
                rasterio_error = True
                logger.warning('Cannot parse projection and data type information for Dataset %r.', uri)

            if group_common_hypercubes:
                total_size_in_bytes = 0

                for xr_dataset in xr_datasets:
                    if not rasterio_error:
                        xr_dataset.attrs.update({'dtype': dtype, 'crs': crs, 'transform': transform})
                    total_size_in_bytes += xr_dataset.nbytes

                logger.info(f'opened dataset size: {total_size_in_bytes}')
            else:
                xr_dataset = xr_datasets
                if start_date is not None and end_date is not None:
                    xr_dataset = xr_datasets.sel(time=slice(start_date, end_date))
                if uri_extension in ['.tif', '.tiff']:
                    xr_dataset = _preprocess_tif(xr_dataset,
                                                 tif_metadata_for_start_time,
                                                 tif_metadata_for_end_time,
                                                 uri,
                                                 initialization_time_regex,
                                                 forecast_time_regex)

                if not rasterio_error:
                    # Extracting dtype, crs and transform from the dataset & storing them as attributes.
                    xr_dataset.attrs.update({'dtype': dtype, 'crs': crs, 'transform': transform})
                logger.info(f'opened dataset size: {xr_dataset.nbytes}')

            beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
            yield xr_datasets if group_common_hypercubes else xr_dataset

            # Releasing any resources linked to the object(s).
            if group_common_hypercubes:
                for xr_dataset in xr_datasets:
                    xr_dataset.close()
            else:
                xr_dataset.close()

    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logger.error(f'Unable to open file {uri!r}: {e}')
        raise


def get_file_time(element: t.Any) -> int:
    """Calculates element file's write timestamp in UTC."""
    try:
        element_parsed = urlparse(element)
        if element_parsed.scheme == "gs":  # For file in Google cloud storage.
            client = storage.Client()
            bucket = client.get_bucket(element_parsed.netloc)
            blob = bucket.get_blob(element_parsed.path[1:])

            updated_time = int(blob.updated.timestamp())
        else:  # For file in local.
            file_stats = os.stat(element)
            updated_time = int(time.mktime(time.gmtime(file_stats.st_mtime)))

        return updated_time
    except Exception as e:
        raise ValueError(
            f"Error fetching raw data file bucket time for {element!r}. Error: {str(e)}"
        )
