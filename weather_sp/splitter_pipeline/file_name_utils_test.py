import unittest

from .file_name_utils import get_output_file_base_name


class FileNameUtilsTest(unittest.TestCase):

    def test_get_output_file_base_name_format(self):
        out_info = get_output_file_base_name(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                             out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.',
                                             input_base_dir='ignored')
        self.assertEqual(out_info.file_name_base, 'gs://my_bucket/splits/2020-01-21_old_data.')
        self.assertEqual(out_info.ending, '.nc')

    def test_get_output_file_base_name_replace(self):
        out_info = get_output_file_base_name(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                             out_pattern='gs://my_bucket/splits/',
                                             input_base_dir='gs://my_bucket/data_to_split/')
        self.assertEqual(out_info.file_name_base, 'gs://my_bucket/splits/2020/01/21_')
        self.assertEqual(out_info.ending, '.nc')

    def test_get_output_file_base_name_format_no_fileending(self):
        out_info = get_output_file_base_name(filename='gs://my_bucket/data_to_split/2020/01/21',
                                             out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.',
                                             input_base_dir='ignored')
        self.assertEqual(out_info.file_name_base, 'gs://my_bucket/splits/2020-01-21_old_data.')
        self.assertEqual(out_info.ending, '')

    def test_get_output_file_base_name_format_filecontainsdots(self):
        out_info = get_output_file_base_name(filename='gs://my_bucket/data_to_split/2020/01/21.T00z.stuff',
                                             out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.',
                                             input_base_dir='ignored')
        self.assertEqual(out_info.file_name_base, 'gs://my_bucket/splits/2020-01-21.T00z.stuff_old_data.')
        self.assertEqual(out_info.ending, '')
