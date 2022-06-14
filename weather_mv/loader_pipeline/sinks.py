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
import contextlib
import dataclasses
import logging
import shutil
import tempfile
import typing as t
import os
from pyproj import Transformer
import numpy as np
import datetime

import apache_beam as beam
import rasterio
import xarray as xr
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import DEFAULT_READ_BUFFER_SIZE

TIF_TRANSFORM_CRS_TO = "EPSG:4326"

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclasses.dataclass
class ToDataSink(abc.ABC, beam.PTransform):
    variables: t.List[str]
    area: t.Tuple[int, int, int, int]
    xarray_open_dataset_kwargs: t.Dict
    dry_run: bool
    disable_in_memory_copy: bool
    tif_metadata_for_datetime: t.Optional[str]

    @classmethod
    def from_kwargs(cls, **kwargs):
        fields = [f.name for f in dataclasses.fields(cls)]
        return cls(**{k: v for k, v, in kwargs.items() if k in fields})


def _make_grib_dataset_inmem(grib_ds: xr.Dataset) -> xr.Dataset:
    """Copies all the vars in-memory to reduce disk seeks every time a single row is processed.

    This also removes the need to keep the backing temp source file around.
    """
    data_ds = grib_ds.copy(deep=True)
    for v in grib_ds.variables:
        if v not in data_ds.coords:
            data_ds[v].variable.values = grib_ds[v].variable.values
    return data_ds


def _preprocess_tif(ds: xr.Dataset, filename: str, tif_metadata_for_datetime: str) -> xr.Dataset:
    """Transforms (y, x) coordinates into (lat, long) and adds bands data in data variables.

    This also retrieves datetime from tif's metadata and stores it into dataset.
    """
    def _get_band_data(i):
        band = ds.band_data[i]
        band.name = ds.band_data.attrs['long_name'][i]
        return band

    y, x = np.meshgrid(ds['y'], ds['x'])
    transformer = Transformer.from_crs(ds.spatial_ref.crs_wkt, TIF_TRANSFORM_CRS_TO, always_xy=True)
    lon, lat = transformer.transform(x, y)

    ds['y'] = lat[0, :]
    ds['x'] = lon[:, 0]
    ds = ds.rename({'y': 'latitude', 'x': 'longitude'})

    band_length = len(ds.band)
    ds = ds.squeeze().drop_vars('band').drop_vars('spatial_ref')

    band_data_list = [_get_band_data(i) for i in range(band_length)]
    ds = xr.merge(band_data_list)

    # TODO(#159): Explore ways to capture required metadata using xarray.
    with rasterio.open(filename) as f:
        datetime_value_ms = None
        try:
            datetime_value_ms = f.tags()[tif_metadata_for_datetime]
            ds = ds.assign_coords({'time': datetime.datetime.utcfromtimestamp(int(datetime_value_ms)/1000.0)})
        except KeyError:
            raise RuntimeError(f"Invalid datetime metadata of tif: {tif_metadata_for_datetime}.")
        except ValueError:
            raise RuntimeError(f"Invalid datetime value in tif's metadata: {datetime_value_ms}.")
        ds = ds.expand_dims('time')

    return ds


def __open_dataset_file(filename: str, uri_extension: str, open_dataset_kwargs: t.Optional[t.Dict] = None):
    """Open the dataset at 'uri'"""
    if open_dataset_kwargs:
        return xr.open_dataset(filename, **open_dataset_kwargs)

    # If URI extension is .tif, try opening file by specifying engine="rasterio".
    if uri_extension == '.tif':
        return xr.open_dataset(filename, engine='rasterio')

    # If no open kwargs are available and URI extension is other than tif, make educated guesses about the dataset.
    try:
        return xr.open_dataset(filename)
    except ValueError as e:
        e_str = str(e)
        if not ("Consider explicitly selecting one of the installed engines" in e_str and "cfgrib" in e_str):
            raise

    # Trying with explicit engine for cfgrib.
    try:
        return xr.open_dataset(filename, engine='cfgrib', backend_kwargs={'indexpath': ''})
    except ValueError as e:
        if "multiple values for key 'edition'" not in str(e):
            raise
    logger.warning("Assuming grib edition 1.")
    # Try with edition 1
    # Note: picking edition 1 for now as it seems to get the most data/variables for ECMWF realtime data.
    return xr.open_dataset(filename, engine='cfgrib',
                           backend_kwargs={'filter_by_keys': {'edition': 1}, 'indexpath': ''})


@contextlib.contextmanager
def open_dataset(uri: str, open_dataset_kwargs: t.Optional[t.Dict] = None, disable_in_memory_copy: bool = False,
                 tif_metadata_for_datetime: t.Optional[str] = None) -> t.Iterator[xr.Dataset]:
    """Open the dataset at 'uri' and return a xarray.Dataset."""
    try:
        # Copy netcdf or grib object from cloud storage, like GCS, to local file
        # so xarray can open it with mmap instead of copying the entire thing
        # into memory.
        with FileSystems().open(uri) as source_file:
            with tempfile.NamedTemporaryFile() as dest_file:
                shutil.copyfileobj(source_file, dest_file, DEFAULT_READ_BUFFER_SIZE)
                dest_file.flush()
                dest_file.seek(0)

                _, uri_extension = os.path.splitext(uri)
                xr_dataset: xr.Dataset = __open_dataset_file(dest_file.name, uri_extension, open_dataset_kwargs)

                if uri_extension == '.tif':
                    xr_dataset = _preprocess_tif(xr_dataset, dest_file.name, tif_metadata_for_datetime)

                if not disable_in_memory_copy:
                    xr_dataset = _make_grib_dataset_inmem(xr_dataset)

                logger.info(f'opened dataset size: {xr_dataset.nbytes}')

                beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
                yield xr_dataset
    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logger.error(f'Unable to open file {uri!r}: {e}')
        raise
