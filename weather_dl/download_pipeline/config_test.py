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
import calendar
import itertools
import os
import unittest

import weather_dl
from .config import optimize_selection_partition
from .pipeline import run


class ConfigTest(unittest.TestCase):

    def setUp(self):
        self.data_dir = f'{next(iter(weather_dl.__path__))}/../configs'

    def test_process_config_files(self):
        for filename in os.listdir(self.data_dir):
            if filename.startswith('.'):
                continue
            # only files, no directories
            if os.path.isdir(os.path.join(self.data_dir, filename)):
                continue
            with self.subTest(filename=filename):
                config = os.path.join(self.data_dir, filename)
                try:
                    run(["weather-dl", config, "--dry-run"])
                except:  # noqa: E722
                    self.fail(f'Config {filename!r} is incorrect.')

    def test_process_multi_config_files(self):
        for filename in os.listdir(self.data_dir):
            if filename.startswith('.'):
                continue
            # only directories, no files
            if not os.path.isdir(os.path.join(self.data_dir, filename)):
                continue
            with self.subTest(dirname=filename):
                configs_dir = os.path.join(self.data_dir, filename)
                configs = [os.path.join(configs_dir, c) for c in os.listdir(configs_dir)]
                try:
                    run(["weather-dl"] + configs + ["--dry-run"])
                except:  # noqa: E722
                    self.fail(f'Configs {filename!r} is incorrect.')


if __name__ == '__main__':
    unittest.main()


class SelectionSyntaxTest(unittest.TestCase):

    def test_all_days__invalid_year(self):
        selection_with_multiple_years = {'year': '2020/2021', 'month': '2', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "When using day='all' in selection, '/' is not allowed in year."):
            optimize_selection_partition(selection_with_multiple_years)

        selection_with_multiple_years = {'year': ['2020/2021'], 'month': '2', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "When using day='all' in selection, '/' is not allowed in year."):
            optimize_selection_partition(selection_with_multiple_years)

    def test_all_days__invalid_month(self):
        selection_with_multiple_years = {'year': '2020', 'month': '1/2/3', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "When using day='all' in selection, '/' is not allowed in month."):
            optimize_selection_partition(selection_with_multiple_years)

        selection_with_multiple_years = {'year': '2020', 'month': ['1/2/3'], 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "When using day='all' in selection, '/' is not allowed in month."):
            optimize_selection_partition(selection_with_multiple_years)

    def test_date_range(self):
        selection_with_date_range = {'date_range': ['2017-01-01/to/2017-01-10']}
        actual = optimize_selection_partition(selection_with_date_range)
        self.assertEqual(actual['date'], selection_with_date_range['date_range'][0])

    def test_with_year_month_as_string(self):
        selection_with_multiple_months = {'year': '2017', 'month': '12', 'day':'all'}
        actual = optimize_selection_partition(selection_with_multiple_months)

        expected = []
        _, n_days_in_month = calendar.monthrange(2017, 12)
        expected = [f'2017-12-{day:02d}' for day in range(1, n_days_in_month + 1)]

        self.assertEqual(actual['date'], expected)
        self.assertNotIn('day', actual)
        self.assertNotIn('month', actual)
        self.assertNotIn('year', actual)

    def test_with_multiple_months(self):
        selection_with_multiple_months = {'year': ['2017'], 'month': ['2', '4', '6', '8'], 'day':'all'}
        actual = optimize_selection_partition(selection_with_multiple_months)

        expected = []
        for y, m in itertools.product(selection_with_multiple_months['year'], selection_with_multiple_months['month']):
            y, m = int(y), int(m)
            _, n_days_in_month = calendar.monthrange(y, m)
            date_range = [f'{y:04d}-{m:02d}-{day:02d}' for day in range(1, n_days_in_month + 1)]
            expected.extend(date_range)

        self.assertEqual(actual['date'], expected)
        self.assertNotIn('day', actual)
        self.assertNotIn('month', actual)
        self.assertNotIn('year', actual)

    def test_with_multiple_years(self):
        selection_with_multiple_years = {'year': ['2017', '2018'], 'month': ['2', '4', '6', '8'], 'day':'all'}
        actual = optimize_selection_partition(selection_with_multiple_years)

        expected = []
        for y, m in itertools.product(selection_with_multiple_years['year'], selection_with_multiple_years['month']):
            y, m = int(y), int(m)
            _, n_days_in_month = calendar.monthrange(y, m)
            date_range = [f'{y:04d}-{m:02d}-{day:02d}' for day in range(1, n_days_in_month + 1)]
            expected.extend(date_range)

        self.assertEqual(actual['date'], expected)
        self.assertNotIn('day', actual)
        self.assertNotIn('month', actual)
        self.assertNotIn('year', actual)
