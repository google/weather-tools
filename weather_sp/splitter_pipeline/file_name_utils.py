import logging
import os
import typing as t

_WEATHER_FILE_ENDINGS = ('.nc', '.cd', '.grib', '.grb', '.grb2', '.grib2')

logger = logging.getLogger(__name__)


class OutFileInfo(t.NamedTuple):
    file_name_base: str
    ending: str

    def __str__(self):
        return f'{self.file_name_base}/*/{self.ending}'


def get_output_file_base_name(filename: str, out_pattern: str, input_base_dir: str) -> OutFileInfo:
    '''
    Construct the base output file name by applying the out_pattern to the
    filename.

    Example:
        filename = 'gs://my_bucket/data_to_split/2020/01/21.nc'
        out_pattern = 'gs://my_bucket/splits/{2}-{1}-{0}_old_data.'
        resulting output base = 'gs://my_bucket/splits/2020-01-21_old_data.'
        resulting file ending = '.nc'

    Args:
        filename: input file to be split
        out_pattern: pattern to apply when creating output file
        input_base_dir: used if out_pattern does not contain any '{}' substitutions.
            The output file is then created by replacing this part of the input name
            with the output pattern.
    '''
    file_ending = ''
    for ending in _WEATHER_FILE_ENDINGS:
        if filename.endswith(ending):
            file_ending = ending
            filename = filename[:-len(ending)]
            break
    if '{' not in out_pattern:
        return OutFileInfo(f'{filename.replace(input_base_dir, out_pattern)}_', file_ending)
    in_sections = []
    path = filename
    while path:
        path, tail = os.path.split(path)
        in_sections.append(tail)
    return OutFileInfo(out_pattern.format(*in_sections), file_ending)
