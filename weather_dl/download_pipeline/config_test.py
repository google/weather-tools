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
import copy
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
            with self.subTest(filename=filename):
                config = os.path.join(self.data_dir, filename)
                try:
                    run(["weather-dl", config, "--dry-run"])
                except:  # noqa: E722
                    self.fail(f'Config {filename!r} is incorrect.')


if __name__ == '__main__':
    unittest.main()


class SelectionSyntaxTest(unittest.TestCase):

    ALL_DAYS = [
        ({'year': f'{y:04d}', 'month': f'{m:02d}', 'day': 'all'}, calendar.monthrange(y, m)[1])
        for y in range(1900, 2023)
        for m in range(1, 13)
    ]

    def test_all_days__as_strings(self):
        for selection, n_days in self.ALL_DAYS:
            year, month = selection["year"], selection["month"]
            expected = f'{year}-{month}-01/to/{year}-{month}-{n_days:02d}'
            with self.subTest(msg=f'{year}-{month} == {n_days}'):
                actual = optimize_selection_partition(selection)
                self.assertEqual(actual['date'], expected)
                self.assertNotIn('day', actual)
                self.assertNotIn('month', actual)
                self.assertNotIn('year', actual)

    def test_all_days__as_lists(self):
        for selection_, n_days in self.ALL_DAYS:
            selection = copy.deepcopy(selection_)
            year, month = selection["year"], selection["month"]
            expected = f'{year}-{month}-01/to/{year}-{month}-{n_days:02d}'
            selection['year'] = [year]
            selection['month'] = [month]
            with self.subTest(msg=f'{year}-{month} == {n_days}'):
                actual = optimize_selection_partition(selection)
                self.assertEqual(actual['date'], expected)
                self.assertNotIn('day', actual)
                self.assertNotIn('month', actual)
                self.assertNotIn('year', actual)

    def test_all_days__invalid_year(self):
        selection_with_multiple_years = {'year': '2020/2021', 'month': '2', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "Cannot use keyword .* 'year's."):
            optimize_selection_partition(selection_with_multiple_years)

        selection_with_multiple_years = {'year': ['2020/2021'], 'month': '2', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "Cannot use keyword .* 'year's."):
            optimize_selection_partition(selection_with_multiple_years)

        selection_with_multiple_years = {'year': ['2020', '2021'], 'month': '2', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "Cannot use keyword .* 'year's."):
            optimize_selection_partition(selection_with_multiple_years)

    def test_all_days__invalid_month(self):
        selection_with_multiple_years = {'year': '2020', 'month': '1/2/3', 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "Cannot use keyword .* 'month's."):
            optimize_selection_partition(selection_with_multiple_years)

        selection_with_multiple_years = {'year': '2020', 'month': ['1/2/3'], 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "Cannot use keyword .* 'month's."):
            optimize_selection_partition(selection_with_multiple_years)

        selection_with_multiple_years = {'year': '2020', 'month': ['1', '2', '3'], 'day': 'all'}
        with self.assertRaisesRegex(AssertionError, "Cannot use keyword .* 'month's."):
            optimize_selection_partition(selection_with_multiple_years)
