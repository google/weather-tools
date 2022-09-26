# Copyright 2021 Google LLC
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

import os
import shutil
from collections import defaultdict

import numpy as np
import pygrib
import pytest
import xarray as xr

import weather_sp
from .file_name_utils import OutFileInfo
from .file_name_utils import get_output_file_info
from .file_splitters import DrySplitter, GribSplitterV2
from .file_splitters import GribSplitter
from .file_splitters import NetCdfSplitter
from .file_splitters import get_splitter


@pytest.fixture()
def data_dir():
    data_dir = f'{next(iter(weather_sp.__path__))}/test_data'
    yield data_dir
    split_dir = f'{data_dir}/split_files/'
    if os.path.exists(split_dir):
        shutil.rmtree(split_dir)


@pytest.fixture(params=[GribSplitter, GribSplitterV2])
def grib_splitter(request):
    return request.param


class TestGetSplitter:

    def test_get_splitter_grib(self, data_dir):
        splitter = get_splitter(f'{data_dir}/era5_sample.grib',
                                OutFileInfo(
                                    file_name_template='some_out_{split}',
                                    ending='.grib',
                                    formatting='',
                                    template_folders=[]),
                                dry_run=False)
        assert isinstance(splitter, GribSplitter)

    def test_get_splitter_nc(self, data_dir):
        splitter = get_splitter(f'{data_dir}/era5_sample.nc',
                                OutFileInfo(
                                    file_name_template='some_out_{split}',
                                    ending='.nc',
                                    formatting='',
                                    template_folders=[]),
                                dry_run=False)

        assert isinstance(splitter, NetCdfSplitter)

    def test_get_splitter_undetermined_grib(self, data_dir):
        splitter = get_splitter(f'{data_dir}/era5_sample_grib',
                                OutFileInfo(
                                    file_name_template='some_out_{split}',
                                    ending='',
                                    formatting='',
                                    template_folders=[]),
                                dry_run=False)
        assert isinstance(splitter, GribSplitter)

    def test_get_splitter_dryrun(self):
        splitter = get_splitter('some/file/path/data.grib',
                                OutFileInfo(
                                    file_name_template='some_out_{split}',
                                    ending='.grib',
                                    formatting='',
                                    template_folders=[]),
                                dry_run=True)
        assert isinstance(splitter, DrySplitter)


class TestGribSplitter:
    def test_get_output_file_path(self, grib_splitter):
        splitter = grib_splitter(
            'path/to/input',
            OutFileInfo(
                file_name_template='path/output/file.{typeOfLevel}_{shortName}',
                ending='.grib',
                formatting='',
                template_folders=[])
        )
        out = splitter.output_info.formatted_output_path(
            {'typeOfLevel': 'surface', 'shortName': 'cc'})
        assert out == 'path/output/file.surface_cc.grib'

    def test_split_data(self, data_dir, grib_splitter):
        input_path = f'{data_dir}/era5_sample.grib'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = grib_splitter(
            input_path,
            OutFileInfo(
                output_base,
                formatting='_{typeOfLevel}_{shortName}',
                ending='.grib',
                template_folders=[])
        )
        splitter.split_data()
        assert os.path.exists(f'{data_dir}/split_files/') is True

        short_names = ['z', 'r', 'cc', 'd']
        input_data = defaultdict(list)
        split_data = defaultdict(list)

        input_grbs = pygrib.open(input_path)
        for grb in input_grbs:
            input_data[grb.shortName].append(grb.values)

        for sn in short_names:
            split_file = f'{data_dir}/split_files/era5_sample_isobaricInhPa_{sn}.grib'
            split_grbs = pygrib.open(split_file)
            for grb in split_grbs:
                split_data[sn].append(grb.values)

        for sn in short_names:
            orig = np.array(input_data[sn])
            split = np.array(split_data[sn])
            assert orig.shape == split.shape
            np.testing.assert_allclose(orig, split)

    def test_skips_existing_split(self, data_dir, grib_splitter):
        input_path = f'{data_dir}/era5_sample.grib'
        splitter = grib_splitter(
            input_path,
            OutFileInfo(f'{data_dir}/split_files/era5_sample',
                        formatting='_{typeOfLevel}_{shortName}',
                        ending='.grib',
                        template_folders=[])
        )
        assert not splitter.should_skip()
        splitter.split_data()
        assert os.path.exists(f'{data_dir}/split_files/')
        assert splitter.should_skip()

    def test_does_not_skip__if_forced(self, data_dir, grib_splitter):
        input_path = f'{data_dir}/era5_sample.grib'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = grib_splitter(
            input_path,
            OutFileInfo(
                output_base,
                formatting='_{levelType}_{shortName}',
                ending='.grib',
                template_folders=[]),
            force_split=True
        )
        assert not splitter.should_skip()
        splitter.split_data()
        assert os.path.exists(f'{data_dir}/split_files/')
        assert not splitter.should_skip()

    @pytest.mark.limit_memory('2.5 MB')
    def test_split__fits_memory_bounds(self, data_dir, grib_splitter):
        input_path = f'{data_dir}/era5_sample.grib'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = grib_splitter(
            input_path,
            OutFileInfo(
                output_base,
                formatting='_{typeOfLevel}_{shortName}',
                ending='.grib',
                template_folders=[])
        )
        splitter.split_data()


class TestNetCdfSplitter:

    def test_get_output_file_path(self):
        splitter = NetCdfSplitter(
            'path/to/input',
            OutFileInfo('path/output/file_{variable}',
                        ending='.nc',
                        formatting='',
                        template_folders=[]))
        out = splitter.output_info.formatted_output_path({'variable': 'cc'})
        assert out == 'path/output/file_cc.nc'

    def test_split_data(self, data_dir):
        input_path = f'{data_dir}/era5_sample.nc'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(
            input_path,
            OutFileInfo(output_base,
                        formatting='_{time}_{variable}',
                        ending='.nc',
                        template_folders=[]))

        splitter.split_data()
        assert os.path.exists(f'{data_dir}/split_files/')
        input_data = xr.open_dataset(input_path, engine='netcdf4')
        for time in ['2015-01-15T00:00', '2015-01-15T06:00', '2015-01-15T12:00', '2015-01-15T18:00']:
            expect = input_data.sel(time=time)
            for sn in ['d', 'cc', 'z']:
                split_file = f'{data_dir}/split_files/era5_sample_{time}_{sn}.nc'
                split_data = xr.open_dataset(split_file, engine='netcdf4')
                xr.testing.assert_allclose(expect[sn], split_data[sn])

    def test_split_data__not_in_dims_raises(self, data_dir):
        input_path = f'{data_dir}/era5_sample.nc'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(input_path,
                                  OutFileInfo(output_base,
                                              formatting='_{level}',
                                              ending='.nc',
                                              template_folders=[]))
        with pytest.raises(ValueError):
            splitter.split_data()

    def test_split_data__unsupported_dim_raises(self, data_dir):
        input_path = f'{data_dir}/era5_sample.nc'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(input_path,
                                  OutFileInfo(output_base,
                                              formatting='_{longitude}',
                                              ending='.nc',
                                              template_folders=[]))
        with pytest.raises(ValueError):
            splitter.split_data()

    def test_skips_existing_split(self, data_dir):
        input_path = f'{data_dir}/era5_sample.nc'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(input_path,
                                  OutFileInfo(output_base,
                                              formatting='_{variable}',
                                              ending='.nc',
                                              template_folders=[]))
        assert not splitter.should_skip()
        splitter.split_data()
        assert os.path.exists(f'{data_dir}/split_files/')
        assert splitter.should_skip()

    def test_does_not_skip__if_forced(self, data_dir):
        input_path = f'{data_dir}/era5_sample.nc'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(input_path,
                                  OutFileInfo(
                                      output_base,
                                      formatting='_{variable}',
                                      ending='.nc',
                                      template_folders=[]),
                                  force_split=True)
        assert not splitter.should_skip()
        splitter.split_data()
        assert os.path.exists(f'{data_dir}/split_files/')
        assert not splitter.should_skip()

    @pytest.mark.limit_memory('6 MB')
    def test_split_data__fits_memory_bounds(self, data_dir):
        input_path = f'{data_dir}/era5_sample.nc'
        output_base = f'{data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(
            input_path,
            OutFileInfo(output_base,
                        formatting='_{time}_{variable}',
                        ending='.nc',
                        template_folders=[]))

        splitter.split_data()

    def test_split_data__is_netcdf4(self):
        input_path = f'{self._data_dir}/era5_sample.nc'
        self.assertFalse(h5py.is_hdf5(input_path))
        output_base = f'{self._data_dir}/split_files/era5_sample'
        splitter = NetCdfSplitter(
            input_path,
            OutFileInfo(output_base,
                        formatting='_{time}_{variable}',
                        ending='.nc',
                        template_folders=[]))

        splitter.split_data()
        self.assertTrue(os.path.exists(f'{self._data_dir}/split_files/'))
        for time in ['2015-01-15T00:00', '2015-01-15T06:00', '2015-01-15T12:00', '2015-01-15T18:00']:
            for sn in ['d', 'cc', 'z']:
                split_file = f'{self._data_dir}/split_files/era5_sample_{time}_{sn}.nc'
                self.assertTrue(h5py.is_hdf5(split_file))



class TestDrySplitter:

    def test_path_with_output_pattern(self):
        input_path = 'a/b/c/d/file.nc'
        out_pattern = 'gs://my_bucket/splits/{2}-{1}-{0}_old_data.{variable}.cd'
        out_info = get_output_file_info(
            filename=input_path, out_pattern=out_pattern)
        splitter = DrySplitter(input_path, out_info)
        keys = splitter._get_keys()
        assert keys == {'variable': 'variable'}
        out_file = splitter.output_info.formatted_output_path(keys)
        assert out_file == 'gs://my_bucket/splits/c-d-file_old_data.variable.cd'

    def test_path_with_output_pattern_no_formatting(self):
        # OutFileInfo using pattern but without any formatting marks.
        splitter = DrySplitter("input_path/file.grib",
                               OutFileInfo("some/out/no/formatting",
                                           formatting='',
                                           ending='',
                                           template_folders=[]))
        with pytest.raises(ValueError):
            splitter.split_data()
