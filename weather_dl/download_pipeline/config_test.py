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
        actual = optimize_selection_partition(selection_with_date_range, client_name='mars')
        self.assertEqual(actual['date'], selection_with_date_range['date_range'][0])
        self.assertNotIn('date_range', actual)

    def test_date_range_cds_standard_expands_dates(self):
        selection = {'date_range': ['2025-01-01/to/2025-01-03'], 'variable': '2m_temperature'}
        actual = optimize_selection_partition(selection, client_name='cds')
        self.assertEqual(actual['date'], ['2025-01-01', '2025-01-02', '2025-01-03'])
        self.assertNotIn('date_range', actual)

        stepped = {'date_range': ['2025-01-01/to/2025-01-05/by/2'], 'variable': '2m_temperature'}
        actual_stepped = optimize_selection_partition(stepped, client_name='cds')
        self.assertEqual(actual_stepped['date'], ['2025-01-01', '2025-01-03', '2025-01-05'])

    def test_date_range_cds_mars_brokered_keeps_mars_syntax(self):
        selection = {
            'date_range': ['2025-01-01/to/2025-01-02'],
            'class': 'ea',
            'stream': 'oper',
        }
        actual = optimize_selection_partition(selection, client_name='cds')
        self.assertEqual(actual['date'], '2025-01-01/to/2025-01-02')
        self.assertNotIn('date_range', actual)

    def test_year_month(self):
        selection_with_year_month = {'year-month': ['2024-02']}
        actual = optimize_selection_partition(selection_with_year_month)
        expected_leap = [f'2024-02-{day:02d}' for day in range(1, 30)]
        self.assertEqual(actual['date'], expected_leap)
        self.assertNotIn('year-month', actual)

        selection_with_year_month_str = {'year-month': '2025-02'}
        actual_str = optimize_selection_partition(selection_with_year_month_str, client_name='cds')
        expected_non_leap = [f'2025-02-{day:02d}' for day in range(1, 29)]
        self.assertEqual(actual_str['date'], expected_non_leap)
        self.assertNotIn('year-month', actual_str)

    def test_explicit_year_month_day_mars_and_cds(self):
        mars_sel = {'year': ['2025'], 'month': ['01'], 'day': ['01', '02'], 'param': '167.128'}
        actual_mars = optimize_selection_partition(mars_sel, client_name='mars')
        self.assertEqual(actual_mars['date'], ['2025-01-01', '2025-01-02'])
        self.assertNotIn('day', actual_mars)
        self.assertNotIn('month', actual_mars)
        self.assertNotIn('year', actual_mars)

        mars_brok_sel = {
            'year': ['2025'],
            'month': ['02'],
            'day': ['27', '28', '29', '30', '31'],
            'class': 'ea',
            'stream': 'oper',
        }
        actual_brok = optimize_selection_partition(mars_brok_sel, client_name='cds')
        self.assertEqual(actual_brok['date'], ['2025-02-27', '2025-02-28'])
        self.assertNotIn('day', actual_brok)

        cds_std_sel = {
            'year': ['2025'],
            'month': ['01'],
            'day': ['01', '02'],
            'variable': '2m_temperature',
        }
        actual_cds = optimize_selection_partition(cds_std_sel, client_name='cds')
        self.assertEqual(actual_cds['year'], ['2025'])
        self.assertEqual(actual_cds['month'], ['01'])
        self.assertEqual(actual_cds['day'], ['01', '02'])
        self.assertNotIn('date', actual_cds)

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
