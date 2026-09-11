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
import ast
import datetime
import logging
import os
import re
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
        keys = []
        try:
            parsed = list(string.Formatter().parse(self.unformatted_output_path()))
        except ValueError:
            parsed = []
        for field in parsed:
            if field[1] is not None and not field[1].isdigit():
                try:
                    tree = ast.parse(field[1], mode='eval')
                    for node in ast.walk(tree):
                        if isinstance(node, ast.Name) and node.id not in ('datetime',):
                            keys.append(node.id)
                except Exception:
                    keys.append(field[1])
        return list(dict.fromkeys(keys))

    def _eval_field(self, field_expr: str, variables: t.Dict[str, str]) -> str:
        """Evaluate a single template field expression safely.

        Supports both simple variable references and datetime expressions
        like ``datetime.datetime.strptime(time, "%Y-%m").strftime("%Y")``.
        """
        try:
            tree = ast.parse(field_expr, mode='eval')
        except SyntaxError:
            return '{' + field_expr + '}'

        safe_globals = {"datetime": datetime}

        try:
            result = eval(compile(tree, '<template>', 'eval'), safe_globals, variables)
            return str(result)
        except Exception:
            return '{' + field_expr + '}'

    def formatted_output_path(self, splits: t.Dict[str, str]) -> str:
        """Construct output file name with formatting applied.

        Handles:
        - Simple variable substitution: ``{variable}``
        - Positional template folders: ``{0}``, ``{}``
        - Datetime expressions: ``{datetime.datetime.strptime(time, "%Y").strftime("%Y%m%d")}``
        - Mixed templates with quotes and special characters
        """
        template = self.unformatted_output_path()

        variables = dict(splits)
        for i, folder in enumerate(self.template_folders):
            variables[str(i)] = folder
            variables[f'_{i}'] = folder

        result = []
        last_end = 0

        for match in re.finditer(r'\{([^{}]*)\}', template):
            result.append(template[last_end:match.start()])
            field_expr = match.group(1)

            if not field_expr:
                result.append(match.group(0))
            elif field_expr.isdigit() and int(field_expr) < len(self.template_folders):
                result.append(self.template_folders[int(field_expr)])
            else:
                result.append(self._eval_field(field_expr, variables))

            last_end = match.end()

        result.append(template[last_end:])
        return ''.join(result)


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
