import unittest
from unittest.mock import patch

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
        self.assertEqual(get_output_base_name('somewhere/somefile',
                                              'somewhere', 'out/there'),
                         'out/there/somefile')

    @patch('weather_sp.splitter_pipeline.pipeline.get_splitter')
    def test_split_file(self, mock_get_splitter):
        split_file('somewhere/somefile', 'somewhere', 'out/there', dry_run=True)
        mock_get_splitter.assert_called_with('somewhere/somefile',
                                             'out/there/somefile',
                                             True)


if __name__ == '__main__':
    unittest.main()
