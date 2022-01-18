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

import logging
import os
import typing as t

GRIB_FILE_ENDINGS = ('.grib', '.grb', '.grb2', '.grib2', '.gb')
NETCDF_FILE_ENDINGS = ('.nc', '.cd')

logger = logging.getLogger(__name__)


class OutFileInfo(t.NamedTuple):
    file_name_base: str
    ending: str

    def __str__(self):
        return f'{self.file_name_base}/*/{self.ending}'


def get_output_file_base_name(filename: str,
                              out_pattern: t.Optional[str],
                              out_dir: t.Optional[str],
                              input_base_dir: str) -> OutFileInfo:
    """Construct the base output file name by applying the out_pattern to the
    filename.

    Example:
        filename = 'gs://my_bucket/data_to_split/2020/01/21.nc'
        out_pattern = 'gs://my_bucket/splits/{2}-{1}-{0}_old_data.'
        resulting output base = 'gs://my_bucket/splits/2020-01-21_old_data.'
        resulting file ending = '.nc'

    Args:
        filename: input file to be split
        out_pattern: pattern to apply when creating output file
        out_dir: directory to replace input base directory
        input_base_dir: used if out_pattern does not contain any '{}' substitutions.
            The output file is then created by replacing this part of the input name
            with the output pattern.
    """
    file_ending = ''
    split_name, ending = os.path.splitext(filename)
    if ending in [*GRIB_FILE_ENDINGS, *NETCDF_FILE_ENDINGS]:
        file_ending = ending
        filename = split_name

    if out_dir:
        return OutFileInfo(f'{filename.replace(input_base_dir, out_dir)}_',
                           file_ending)
    if out_pattern:
        in_sections = []
        path = filename
        while path:
            path, tail = os.path.split(path)
            in_sections.append(tail)
        return OutFileInfo(out_pattern.format(*in_sections), file_ending)

    raise ValueError('no output specified')
