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

import unittest

from .file_name_utils import get_output_file_info, OutFileInfo


class FileNameUtilsTest(unittest.TestCase):

    def test_get_output_file_info_pattern(self):
        actual = get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                      out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.{variable}',
                                      out_dir='',
                                      input_base_dir='ignored')
        expected = OutFileInfo(
            file_name_template='gs://my_bucket/splits/{2}-{1}-{0}_old_data.{variable}',
            template_folders=['21', '01', '2020',
                              'data_to_split', 'my_bucket', 'gs:'],
            ending='',
            formatting='')
        self.assertEqual(actual, expected)

    def test_get_output_file_info_dir(self):
        actual = get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                      out_pattern='',
                                      out_dir='gs://my_bucket/splits/',
                                      input_base_dir='gs://my_bucket/data_to_split/',
                                      formatting='_{foo}')
        expected = OutFileInfo(
            file_name_template='gs://my_bucket/splits/2020/01/21',
            template_folders=[],
            ending='.nc',
            formatting='_{foo}')
        self.assertEqual(actual, expected)

    def test_get_output_file_info_no_fileending(self):
        actual = get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21',
                                      out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.',
                                      out_dir='',
                                      input_base_dir='ignored')
        expected = OutFileInfo(
            file_name_template='gs://my_bucket/splits/{2}-{1}-{0}_old_data.',
            template_folders=['21', '01', '2020',
                              'data_to_split', 'my_bucket', 'gs:'],
            ending='',
            formatting='')
        self.assertEqual(actual, expected)

    def test_get_output_file_info_filecontainsdots(self):
        actual = get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21.T00z.stuff',
                                      out_dir='gs://my_bucket/splits/',
                                      input_base_dir='gs://my_bucket/data_to_split/',
                                      formatting='.{foo}')
        expected = OutFileInfo(
            file_name_template='gs://my_bucket/splits/2020/01/21.T00z.stuff',
            template_folders=[],
            ending='',
            formatting='.{foo}')
        self.assertEqual(actual, expected)

    def test_get_output_file_info_dir_no_formatting(self):
        with self.assertRaises(ValueError):
            get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                 out_pattern='',
                                 out_dir='gs://my_bucket/splits/',
                                 input_base_dir='gs://my_bucket/data_to_split/')

    def test_output_pattern_ignores_formatting(self):
        actual = get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                      out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.{variable}',
                                      out_dir=None,
                                      input_base_dir='ignored',
                                      formatting='_{time}_{level}hPa')
        expected = OutFileInfo(
            file_name_template='gs://my_bucket/splits/{2}-{1}-{0}_old_data.{variable}',
            template_folders=['21', '01', '2020',
                              'data_to_split', 'my_bucket', 'gs:'],
            ending='',
            formatting='')
        self.assertEqual(actual, expected)

    def test_split_dims(self):
        actual = get_output_file_info(filename='gs://my_bucket/data_to_split/2020/01/21.nc',
                                      out_pattern='gs://my_bucket/splits/{2}-{1}-{0}_old_data.{variable}',
                                      out_dir=None,
                                      input_base_dir='ignored')
        self.assertEqual(actual.split_dims(), ['variable'])
