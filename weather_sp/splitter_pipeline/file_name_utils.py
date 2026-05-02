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
import datetime as _datetime
import logging
import os
import typing as t

logger = logging.getLogger(__name__)

GRIB_FILE_ENDINGS = ('.grib', '.grb', '.grb2', '.grib2', '.gb')
NETCDF_FILE_ENDINGS = ('.nc', '.cd')


class _DateTimeNamespace:
    date = _datetime.date
    datetime = _datetime.datetime
    time = _datetime.time
    timedelta = _datetime.timedelta
    timezone = _datetime.timezone
    strptime = staticmethod(_datetime.datetime.strptime)


_TEMPLATE_GLOBALS = {
    '__builtins__': {'__import__': __import__},
    'datetime': _DateTimeNamespace,
    'float': float,
    'int': int,
    'str': str,
}


def _iter_template_fields(template: str) -> t.Iterator[t.Tuple[str, str]]:
    """Yield literal text and replacement fields from a format template."""
    literal = []
    i = 0
    while i < len(template):
        char = template[i]
        if char == '{':
            if i + 1 < len(template) and template[i + 1] == '{':
                literal.append('{')
                i += 2
                continue
            if literal:
                yield ''.join(literal), ''
                literal = []
            field, i = _read_template_field(template, i + 1)
            yield '', field
        elif char == '}':
            if i + 1 < len(template) and template[i + 1] == '}':
                literal.append('}')
                i += 2
                continue
            raise ValueError("single '}' encountered in format string")
        else:
            literal.append(char)
            i += 1
    if literal:
        yield ''.join(literal), ''


def _read_template_field(template: str, start: int) -> t.Tuple[str, int]:
    field = []
    quote = ''
    escaped = False
    depth = 0
    i = start
    while i < len(template):
        char = template[i]
        if quote:
            field.append(char)
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = ''
            i += 1
            continue
        if char in ('"', "'"):
            quote = char
            field.append(char)
        elif char == '{':
            depth += 1
            field.append(char)
        elif char == '}':
            if depth == 0:
                return ''.join(field), i + 1
            depth -= 1
            field.append(char)
        else:
            field.append(char)
        i += 1
    raise ValueError("expected '}' before end of string")


def _split_field(field: str) -> t.Tuple[str, str, str]:
    quote = ''
    escaped = False
    depth = 0
    conversion_at = None
    format_at = None
    for i, char in enumerate(field):
        if quote:
            if escaped:
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == quote:
                quote = ''
            continue
        if char in ('"', "'"):
            quote = char
        elif char in '([{':
            depth += 1
        elif char in ')]}':
            depth -= 1
        elif depth == 0 and char == '!' and conversion_at is None and format_at is None:
            conversion_at = i
        elif depth == 0 and char == ':' and format_at is None:
            format_at = i
            break

    field_name_end = min(x for x in (conversion_at, format_at, len(field)) if x is not None)
    field_name = field[:field_name_end]
    conversion = ''
    if conversion_at is not None:
        conversion_end = format_at if format_at is not None else len(field)
        conversion = field[conversion_at + 1:conversion_end]
    format_spec = field[format_at + 1:] if format_at is not None else ''
    return field_name, conversion, format_spec


def _names_in_expression(expression: str) -> t.List[str]:
    if expression.isdigit():
        return []
    if expression.isidentifier():
        return [expression]

    tree = ast.parse(expression, mode='eval')
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id not in _TEMPLATE_GLOBALS:
            names.append(node.id)
    return names


def _validate_expression(expression: str) -> None:
    tree = ast.parse(expression, mode='eval')
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id.startswith('__'):
            raise ValueError(f'Disallowed name in output template: {node.id!r}')
        if isinstance(node, ast.Attribute) and node.attr.startswith('__'):
            raise ValueError(f'Disallowed attribute in output template: {node.attr!r}')


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
        dims = []
        for _, field in _iter_template_fields(self.unformatted_output_path()):
            if not field:
                continue
            field_name, _, _ = _split_field(field)
            for name in _names_in_expression(field_name):
                if name not in dims:
                    dims.append(name)
        return dims

    def formatted_output_path(self, splits: t.Dict[str, str]) -> str:
        """Construct output file name with formatting applied"""
        output = []
        for literal, field in _iter_template_fields(self.unformatted_output_path()):
            output.append(literal)
            if not field:
                continue
            field_name, conversion, format_spec = _split_field(field)
            if field_name.isdigit():
                value = self.template_folders[int(field_name)]
            elif field_name.isidentifier():
                value = splits[field_name]
            else:
                _validate_expression(field_name)
                value = eval(field_name, _TEMPLATE_GLOBALS, dict(splits))
            if conversion == 's':
                value = str(value)
            elif conversion == 'r':
                value = repr(value)
            elif conversion == 'a':
                value = ascii(value)
            elif conversion:
                raise ValueError(f'Unknown conversion specifier {conversion!r}')
            output.append(format(value, format_spec))
        return ''.join(output)


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
