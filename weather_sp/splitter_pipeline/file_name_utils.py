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

from dataclasses import dataclass
import logging
import os
import string
import typing as t

logger = logging.getLogger(__name__)

GRIB_FILE_ENDINGS = ('.grib', '.grb', '.grb2', '.grib2', '.gb')
NETCDF_FILE_ENDINGS = ('.nc', '.cd')


@dataclass
class OutFileInfo:
    """Holds data required to construct an output file name.

    Attributes:
        file_name_template: base output path, may contain python-style formatting
                            marks. This can be a base directory or a full name.
        formatting: added after file_name_template to add formatting. Only used
                    when using --output-dir.
        ending: file ending.
        template_folders: list of input file directory structure. Only used with
                          --output-template
    """
    file_name_template: str
    formatting: str
    ending: str
    template_folders: t.List[str]

    def __repr__(self):
        return self.unformatted_output_path()

    def unformatted_output_path(self):
        """Construct output file name with formatting marks."""
        return self.file_name_template + self.formatting + self.ending

    def split_dims(self) -> t.List[str]:
        all_format = list(filter(None, [field[1] for field in string.Formatter().parse(
            self.unformatted_output_path())]))
        return [key for key in all_format if not key.isdigit()]

    def formatted_output_path(self, splits: t.Dict[str, str]) -> str:
        """Construct output file name with formatting applied"""
        return self.unformatted_output_path().format(*self.template_folders, **splits)


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

    if out_dir and not formatting:
        raise ValueError('No formatting specified when using --output-dir.')
    if out_dir:
        return OutFileInfo(
            f'{filename.replace(input_base_dir, out_dir)}',
            formatting,
            ending,
            []
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
