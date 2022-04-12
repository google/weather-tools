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

import unittest
from unittest.mock import patch

from .file_name_utils import OutFileInfo
from .pipeline import _get_base_input_directory
from .pipeline import get_output_base_name
from .pipeline import split_file


class PipelineTest(unittest.TestCase):

    def test_get_base_input_directory(self):
        self.assertEqual(
            _get_base_input_directory(
                '/path/to/some/wild/*/card/??[0-1].nc'),
            '/path/to/some')
        self.assertEqual(
            _get_base_input_directory(
                '/path/to/some/wild/??/card/*[0-1].nc'),
            '/path/to/some')
        self.assertEqual(
            _get_base_input_directory(
                '/path/to/some/wild/201[8,9]/card/??.nc'),
            '/path/to/some')

    def test_get_output_base_name(self):
        self.assertEqual(get_output_base_name(
            input_path='somewhere/somefile',
            input_base='somewhere',
            output_template=None,
            formatting='.{shortName}',
            output_dir='out/there').file_name_template, 'out/there/somefile')

    @patch('weather_sp.splitter_pipeline.file_splitters.get_splitter')
    @unittest.skip('bad mocks')
    def test_split_file(self, mock_get_splitter):
        split_file(input_file='somewhere/somefile',
                   input_base_dir='somewhere',
                   output_dir='out/there',
                   output_template=None,
                   formatting='_{variable}',
                   dry_run=True)
        mock_get_splitter.assert_called_with('somewhere/somefile',
                                             OutFileInfo('out/there/somefile',
                                                         formatting='_{variable}',
                                                         ending='',
                                                         template_folders=[]),
                                             True)


if __name__ == '__main__':
    unittest.main()
