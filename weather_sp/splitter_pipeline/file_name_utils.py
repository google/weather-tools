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

logger = logging.getLogger(__name__)

GRIB_FILE_ENDINGS = ('.grib', '.grb', '.grb2', '.grib2', '.gb')
NETCDF_FILE_ENDINGS = ('.nc', '.cd')


class OutFileInfo:
    """Holds data required to construct an output file name."""

    def __init__(self, file_name_template: str,
                 formatting: str = '',
                 ending: str = '',
                 template_folders: t.List[str] = [],
                 requires_formatting: bool = False):
        self.file_name_template = file_name_template
        self.formatting = formatting
        self.ending = ending
        self.template_folders = template_folders
        self.requires_formatting = requires_formatting

    def __str__(self):
        return self.get_unformatted_output_name()

    def get_unformatted_output_name(self):
        """Construct base output file name with formatting marks."""
        return self.file_name_template + self.formatting + self.ending

    def set_formatting_if_needed(self, formatting: str):
        """If formatting string is required but not present, set formatting.

        Does not overwrite already present formatting.
        """
        if self.requires_formatting and not self.formatting:
            self.formatting = formatting

    def get_formatted_output_file_path(self, splits: t.Dict[str, str]) -> str:
        """Construct output file name with formatting applied"""
        return self.get_unformatted_output_name().format(*self.template_folders, **splits)


def get_output_file_info(filename: str,
                         input_base_dir: str = '',
                         out_pattern: t.Optional[str] = None,
                         out_dir: t.Optional[str] = None,
                         formatting: str = '') -> OutFileInfo:
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
        formatting: output formatting of split fields. Required when using
            out_dir, ignored when using out_pattern.
        input_base_dir: used if out_pattern does not contain any '{}'
            substitutions.
            The output file is then created by replacing this part of the input
            name with the output pattern.
    """
    split_name, ending = os.path.splitext(filename)
    if ending in GRIB_FILE_ENDINGS or ending in NETCDF_FILE_ENDINGS:
        filename = split_name
    else:
        ending = ''

    if out_dir:
        return OutFileInfo(
            f'{filename.replace(input_base_dir, out_dir)}',
            formatting,
            ending,
            [],
            requires_formatting=True
        )

    if out_pattern:
        in_sections = []
        path = filename
        while path:
            path, tail = os.path.split(path)
            in_sections.append(tail)
        # setting formatting and ending to empty strings since they are
        # part of the specified pattern.
        return OutFileInfo(out_pattern, '', '', in_sections)

    raise ValueError('no output specified.')
