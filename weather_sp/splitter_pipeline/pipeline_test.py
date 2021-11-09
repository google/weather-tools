import unittest
from unittest.mock import patch

from .pipeline import split_file


class PipelineTest(unittest.TestCase):

    @patch('weather_sp.splitter_pipeline.pipeline.get_splitter')
    def test_skip_already_split(self, mock_splitter):
        split_file('somewhere/split_files/somefile')
        mock_splitter.assert_not_called()

    @patch('weather_sp.splitter_pipeline.pipeline.get_splitter')
    def test_split_non_split(self, mock_get_splitter):
        split_file('somewhere/somefile')
        mock_get_splitter.assert_called_with('somewhere/somefile')


if __name__ == '__main__':
    unittest.main()
