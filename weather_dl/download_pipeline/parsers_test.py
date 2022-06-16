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
import datetime
import io
import unittest

from .manifest import MockManifest, Location
from .parsers import (
    date,
    parse_config,
    process_config,
    _number_of_replacements,
    parse_subsections,
    prepare_target_name,
)
from .config import Config


class DateTest(unittest.TestCase):

    def test_parses_relative_date(self):
        self.assertEqual(date('-0'), datetime.date.today())
        self.assertEqual(date('-2'), datetime.date.today() + datetime.timedelta(days=-2))

    def test_parses_kebob_date(self):
        self.assertEqual(date('2020-08-22'), datetime.date(year=2020, month=8, day=22))
        self.assertEqual(date('1900-11-01'), datetime.date(year=1900, month=11, day=1))

    def test_parses_smooshed_date(self):
        self.assertEqual(date('20200822'), datetime.date(year=2020, month=8, day=22))
        self.assertEqual(date('19001101'), datetime.date(year=1900, month=11, day=1))

    def test_parses_year_and_day_of_year(self):
        self.assertEqual(date('2020-235'), datetime.date(year=2020, month=8, day=22))
        self.assertEqual(date('2021-007'), datetime.date(year=2021, month=1, day=7))
        self.assertEqual(date('1900-305'), datetime.date(year=1900, month=11, day=1))

    def test_throws_error(self):
        with self.assertRaises(ValueError):
            date('2020-08-22-12')

        with self.assertRaises(ValueError):
            date('2020-0822')

        with self.assertRaises(ValueError):
            date('20-08-22')

        with self.assertRaises(ValueError):
            date('')


class ParseConfigTest(unittest.TestCase):

    def _assert_no_newlines_in_section(self, dictionary) -> None:
        for val in dictionary['section'].values():
            self.assertNotIn('\n', val)

    def test_json(self):
        with io.StringIO('{"section": {"key": "value"}}') as f:
            actual = parse_config(f)
            self.assertDictEqual(actual, {'section': {'key': 'value'}})

    def test_bad_json(self):
        with io.StringIO('{"section": {"key": "value", "brokenKey": }}') as f:
            actual = parse_config(f)
            self.assertDictEqual(actual, {})

    def test_json_produces_lists(self):
        with io.StringIO('{"section": {"key": "value", "list": [0, 10, 20, 30, 40]}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], [0, 10, 20, 30, 40])

    def test_json_parses_mars_list(self):
        with io.StringIO('{"section": {"key": "value", "list": "1/2/3"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '2', '3'])

    def test_json_parses_mars_int_range(self):
        with io.StringIO('{"section": {"key": "value", "list": "1/to/5"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '2', '3', '4', '5'])

    def test_json_parses_mars_int_range_padded(self):
        with io.StringIO('{"section": {"key": "value", "list": "00/to/05"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['00', '01', '02', '03', '04', '05'])

    def test_json_parses_mars_int_range_incremented(self):
        with io.StringIO('{"section": {"key": "value", "list": "1/to/5/by/2"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '3', '5'])

    def test_json_parses_mars_float_range(self):
        with io.StringIO('{"section": {"key": "value", "list": "1.0/to/5.0"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1.0', '2.0', '3.0', '4.0', '5.0'])

    def test_json_parses_mars_float_range_incremented(self):
        with io.StringIO('{"section": {"key": "value", "list": "1.0/to/5.0/by/2.0"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1.0', '3.0', '5.0'])

    def test_json_parses_mars_float_range_incremented_by_float(self):
        with io.StringIO('{"section": {"key": "value", "list": "0.0/to/0.5/by/0.1"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertEqual(actual['section']['list'], ['0.0', '0.1', '0.2', '0.3', '0.4', '0.5'])

    def test_json_parses_mars_date_range(self):
        with io.StringIO('{"section": {"key": "value", "list": "2020-01-07/to/2020-01-09"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['2020-01-07', '2020-01-08', '2020-01-09'])

    def test_json_parses_mars_relative_date_range(self):
        with io.StringIO('{"section": {"key": "value", "list": "-3/to/-1"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)

            dates = [
                datetime.date.today() + datetime.timedelta(-3),
                datetime.date.today() + datetime.timedelta(-2),
                datetime.date.today() + datetime.timedelta(-1),
            ]

            self.assertListEqual(actual['section']['list'], [d.strftime("%Y-%m-%d") for d in dates])

    def test_json_parses_mars_date_range_incremented(self):
        with io.StringIO('{"section": {"key": "value", "list": "2020-01-07/to/2020-01-12/by/2"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['2020-01-07', '2020-01-09', '2020-01-11'])

    def test_json_raises_syntax_error_missing_right(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '2020-01-07/to/'."):
            with io.StringIO('{"section": {"key": "value", "list": "2020-01-07/to/"}}') as f:
                parse_config(f)

    def test_json_raises_syntax_error_missing_left(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '/to/2020-01-07'."):
            with io.StringIO('{"section": {"key": "value", "list": "/to/2020-01-07"}}') as f:
                parse_config(f)

    def test_json_raises_syntax_error_missing_increment(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '2020-01-07/to/2020-01-11/by/'."):
            with io.StringIO('{"section": {"key": "value", "list": "2020-01-07/to/2020-01-11/by/"}}') as f:
                parse_config(f)

    def test_json_raises_syntax_error_no_range(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '2020-01-07/by/2020-01-11'."):
            with io.StringIO('{"section": {"key": "value", "list": "2020-01-07/by/2020-01-11"}}') as f:
                parse_config(f)

    def test_json_raises_value_error_date_types(self):
        with self.assertRaisesRegex(
            ValueError,
            "Increments on a date range must be integer number of days, '2.0' is invalid."
        ):
            with io.StringIO('{"section": {"key": "value", "list": "2020-01-07/to/2020-01-11/by/2.0"}}') as f:
                parse_config(f)

    def test_json_raises_value_error_float_types(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '1.0/to/10.0/by/2020-01-07'."):
            with io.StringIO('{"section": {"key": "value", "list": "1.0/to/10.0/by/2020-01-07"}}') as f:
                parse_config(f)

    def test_json_raises_value_error_int_types(self):
        with self.assertRaisesRegex(ValueError, "inconsistent types."):
            with io.StringIO('{"section": {"key": "value", "list": "1/to/10/by/2.0"}}') as f:
                parse_config(f)

    def test_json_parses_accidental_extra_whitespace(self):
        with io.StringIO('{"section": {"key": "value", "list": "1/to/5"}}') as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '2', '3', '4', '5'])

    def test_json_parses_parameter_subsections(self):
        with io.StringIO('{"parameters": {"api_url": "https://google.com/", \
                                          "alice": {"api_key": "123"}, \
                                          "bob": {"api_key": "456"}}}') as f:
            actual = parse_config(f)
            self.assertEqual(actual, {
                'parameters': {
                    'api_url': 'https://google.com/',
                    'alice': {'api_key': '123'},
                    'bob': {'api_key': '456'},
                },
            })

    def test_cfg(self):
        with io.StringIO(
                """
                [section]
                key=value
                """
        ) as f:
            actual = parse_config(f)
            self.assertDictEqual(actual, {'section': {'key': 'value'}})

    def test_bad_cfg(self):
        with io.StringIO(
                """
                key=value
                """
        ) as f:
            actual = parse_config(f)
            self.assertDictEqual(actual, {})

    def test_cfg_produces_lists(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=00
                    10
                    20
                    30
                    40
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['00', '10', '20', '30', '40'])

    def test_cfg_parses_mars_list(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=1/2/3
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '2', '3'])

    def test_cfg_parses_mars_int_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=1/to/5
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '2', '3', '4', '5'])

    def test_cfg_parses_mars_int_range_padded(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=00/to/05
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['00', '01', '02', '03', '04', '05'])

    def test_cfg_parses_mars_int_range_incremented(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=1/to/5/by/2
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '3', '5'])

    def test_cfg_parses_mars_int_reverse_range_incremented(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=5/to/1/by/-2
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['5', '3', '1'])

    def test_cfg_parses_mars_float_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=1.0/to/5.0
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1.0', '2.0', '3.0', '4.0', '5.0'])

    def test_cfg_parses_mars_float_range_incremented(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=1.0/to/5.0/by/2.0
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1.0', '3.0', '5.0'])

    def test_cfg_parses_mars_float_range_incremented_by_float(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=0.0/to/0.5/by/0.1
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['0.0', '0.1', '0.2', '0.3', '0.4', '0.5'])

    def test_cfg_parses_mars_float_reverse_range_incremented_by_float(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=0.5/to/0.0/by/-0.1
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['0.5', '0.4', '0.3', '0.2', '0.1', '0.0'])

    def test_cfg_parses_mars_date_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=2020-01-07/to/2020-01-09
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['2020-01-07', '2020-01-08', '2020-01-09'])

    def test_cfg_parses_mars_relative_date_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=-3/to/-1
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)

            dates = [
                datetime.date.today() + datetime.timedelta(-3),
                datetime.date.today() + datetime.timedelta(-2),
                datetime.date.today() + datetime.timedelta(-1),
            ]

            self.assertListEqual(actual['section']['list'], [d.strftime("%Y-%m-%d") for d in dates])

    def test_cfg_parses_mars_relative_date_reverse_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=-1/to/-3/by/-1
                """
        ) as f:
            actual = parse_config(f)
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)

            dates = [
                datetime.date.today() + datetime.timedelta(-1),
                datetime.date.today() + datetime.timedelta(-2),
                datetime.date.today() + datetime.timedelta(-3),
            ]

            self.assertListEqual(actual['section']['list'], [d.strftime("%Y-%m-%d") for d in dates])

    def test_cfg_parses_mars_date_range_incremented(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=2020-01-07/to/2020-01-12/by/2
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['2020-01-07', '2020-01-09', '2020-01-11'])

    def test_cfg_parses_mars_date_reverse_range_incremented(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=2020-01-12/to/2020-01-07/by/-2
                """
        ) as f:
            actual = parse_config(f)
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
            self.assertListEqual(actual['section']['list'], ['2020-01-12', '2020-01-10', '2020-01-08'])

    def test_cfg_raises_syntax_error_missing_right(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '2020-01-07/to/'."):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/to/
                    """
            ) as f:
                parse_config(f)

    def test_cfg_raises_syntax_error_missing_left(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '/to/2020-01-07'."):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=/to/2020-01-07
                    """
            ) as f:
                parse_config(f)

    def test_cfg_raises_syntax_error_missing_increment(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '2020-01-07/to/2020-01-11/by/'."):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/to/2020-01-11/by/
                    """
            ) as f:
                parse_config(f)

    def test_cfg_raises_syntax_error_no_range(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '2020-01-07/by/2020-01-11'."):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/by/2020-01-11
                    """
            ) as f:
                parse_config(f)

    def test_cfg_raises_value_error_date_types(self):
        with self.assertRaisesRegex(
            ValueError,
            "Increments on a date range must be integer number of days, '2.0' is invalid."
        ):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/to/2020-01-11/by/2.0
                    """
            ) as f:
                parse_config(f)

    def test_cfg_raises_value_error_float_types(self):
        with self.assertRaisesRegex(SyntaxError, "Improper range syntax in '1.0/to/10.0/by/2020-01-07'."):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=1.0/to/10.0/by/2020-01-07
                    """
            ) as f:
                parse_config(f)

    def test_cfg_raises_value_error_int_types(self):
        with self.assertRaisesRegex(ValueError, "inconsistent types."):
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=1/to/10/by/2.0
                    """
            ) as f:
                parse_config(f)

    def test_cfg_parses_accidental_extra_whitespace(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=
                    1/to/5
                """
        ) as f:
            actual = parse_config(f)
            self._assert_no_newlines_in_section(actual)
            self.assertListEqual(actual['section']['list'], ['1', '2', '3', '4', '5'])

    def test_cfg_parses_parameter_subsections(self):
        with io.StringIO(
                """
                [parameters]
                api_url=https://google.com/
                [parameters.alice]
                api_key=123
                [parameters.bob]
                api_key=456
                """
        ) as f:
            actual = parse_config(f)
            self.assertEqual(actual, {
                'parameters': {
                    'api_url': 'https://google.com/',
                    'alice': {'api_key': '123'},
                    'bob': {'api_key': '456'},
                },
            })


class HelpersTest(unittest.TestCase):

    CASES = [('', 0), ('{} blah', 1), ('{} {}', 2),
             ('{0}, {1}', 2), ('%s hello', 0), ('hello {.2f}', 1),
             ('ear5-{year}{year}-{month}', 2), ('era5-{year}/{year}-{}-{}', 3),
             ('{year}{year}{year}{year}', 1)]

    def test_number_of_replacements(self):
        for s, want in self.CASES:
            with self.subTest(s=s, want=want):
                actual = _number_of_replacements(s)
                self.assertEqual(actual, want)


class SubsectionsTest(unittest.TestCase):
    def test_parses_config_subsections(self):
        config = {"parsers": {'a': 1, 'b': 2}, "parsers.1": {'b': 3}}

        actual = parse_subsections(config)

        self.assertEqual(actual, {'parsers': {'a': 1, 'b': 2, '1': {'b': 3}}})


class ApiKeyCountingTest(unittest.TestCase):
    def test_no_keys(self):
        config = {"parameters": {'a': 1, 'b': 2}, "parameters.1": {'b': 3}}

        actual = parse_subsections(config)

        self.assertEqual(actual, {'parameters': {'a': 1, 'b': 2, '1': {'b': 3}}})

    def test_api_keys(self):
        config = {"parameters": {'a': 1, 'b': 2},
                  "parameters.param1": {'api_key': 'key1'},
                  "parameters.param2": {'api_key': 'key2'}}

        actual = parse_subsections(config)

        self.assertEqual(actual,
                         {'parameters': {'a': 1, 'b': 2,
                                         'param1': {'api_key': 'key1'},
                                         'param2': {'api_key': 'key2'}}})


class ProcessConfigTest(unittest.TestCase):

    def test_parse_config(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    key=value
                    """
            ) as f:
                process_config(f)

        self.assertEqual("Unable to parse configuration file.", ctx.exception.args[0])

    def test_require_params_section(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [selection]
                    key=value
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'parameters' section required in configuration file.",
            ctx.exception.args[0])

    def test_accepts_parameters_section(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    key=value
                    """
            ) as f:
                process_config(f)

        self.assertNotIn(
            "'parameters' section required in configuration file.",
            ctx.exception.args[0])

    def test_requires_target_path_param(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target=bar
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'parameters' section requires a 'target_path' key.",
            ctx.exception.args[0])

    def test_requires_target_template_param_not_present(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_template=bar
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'target_template' is deprecated, use 'target_path' instead.",
            ctx.exception.args[0])

    def test_accepts_target_path_param(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_path
                    """
            ) as f:
                process_config(f)

        self.assertNotIn(
            "'parameters' section requires a 'target_path' key.",
            ctx.exception.args[0])

    def test_requires_partition_keys_to_match_sections(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_path=bar-{}-{}
                    partition_keys=
                        year
                        month
                    [selection]
                    day=
                        01
                        02
                        03
                    decade=
                        1950
                        1960
                        1970
                        1980
                        1990
                        2000
                        2010
                        2020
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "All 'partition_keys' must appear in the 'selection' section.",
            ctx.exception.args[0])

    def test_accepts_partition_keys_matching_sections(self):
        with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=bar-{}-{}
                partition_keys=
                    year
                    month
                [selection]
                month=
                    01
                    02
                    03
                year=
                    1950
                    1960
                    1970
                    1980
                    1990
                    2000
                    2010
                    2020
                """
        ) as f:
            config = process_config(f)
            self.assertTrue(bool(config))

    def test_accepts_partition_keys_not_present(self):
        with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=bar
                [selection]
                month=
                    01
                    02
                    03
                year=
                    1950
                    1960
                    1970
                    1980
                    1990
                    2000
                    2010
                    2020
                """
        ) as f:
            config = process_config(f)
            self.assertTrue(bool(config))

    def test_treats_partition_keys_as_list(self):
        with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=bar-{}
                partition_keys=month
                [selection]
                month=
                    01
                    02
                    03
                """
        ) as f:
            config = process_config(f)
            self.assertIsInstance(config.partition_keys, list)

    def test_mismatched_template_partition_keys(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_path=bar-{}
                    partition_keys=
                        year
                        month
                    [selection]
                    month=
                        01
                        02
                        03
                    year=
                        1950
                        1960
                        1970
                        1980
                        1990
                        2000
                        2010
                        2020
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'target_path' has 1 replacements. Expected 2",
            ctx.exception.args[0])

    def test_append_date_dirs_raise_error(self):
        with self.assertRaises(NotImplementedError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_path=somewhere/bar-{}
                    append_date_dirs=true
                    partition_keys=
                        date
                    [selection]
                    date=2017-01-01/to/2017-01-01
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "The current version of 'google-weather-tools' no longer supports 'append_date_dirs'!"
            "\n\nPlease refer to documentation for creating date-based directory hierarchy :\n"
            "https://weather-tools.readthedocs.io/en/latest/Configuration.html"
            "#creating-a-date-based-directory-hierarchy.",
            ctx.exception.args[0])

    def test_target_filename_raise_error(self):
        with self.assertRaises(NotImplementedError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_path=somewhere/
                    target_filename=bar-{}
                    partition_keys=
                        date
                    [selection]
                    date=2017-01-01/to/2017-01-01
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "The current version of 'google-weather-tools' no longer supports 'target_filename'!"
            "\n\nPlease refer to documentation :\n"
            "https://weather-tools.readthedocs.io/en/latest/Configuration.html#parameters-section.",
            ctx.exception.args[0])

    def test_client_not_set(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    target_path=bar-{}
                    partition_keys=
                        year
                    [selection]
                    year=
                        1969
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'parameters' section requires a 'client' key.",
            ctx.exception.args[0])

    def test_client_invalid(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=nope
                    target_path=bar-{}
                    partition_keys=
                        year
                    [selection]
                    year=
                        1969
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "Invalid 'client' parameter.",
            ctx.exception.args[0])

    def test_partition_cannot_include_all(self):
        with self.assertRaisesRegex(ValueError, 'cannot appear as a partition key.'):
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo
                    client=cds
                    target_path=bar-{}-{}
                    partition_keys=
                        month
                        day
                    [selection]
                    year=2012
                    month=
                        01
                        02
                        03
                    day=all
                    """
            ) as f:
                process_config(f)

    def test_singleton_partitions_are_converted_to_lists(self):
        with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=bar-{}-{}
                partition_keys=
                    month
                    year
                [selection]
                month=01
                year=2018
                """
        ) as f:
            config = process_config(f)
            self.assertEqual(config.selection['month'], ['01'])
            self.assertEqual(config.selection['year'], ['2018'])


class PrepareTargetNameTest(unittest.TestCase):
    TEST_CASES = [
        dict(case='No date.',
             config={
                 'parameters': {
                     'partition_keys': ['year', 'month'],
                     'target_path': 'download-{}-{}.nc',
                     'force_download': False
                 },
                 'selection': {
                     'features': ['pressure'],
                     'month': ['12'],
                     'year': ['02']
                 }
             },
             expected='download-2-12.nc'),
        dict(case='Has date but no target directory.',
             config={
                 'parameters': {
                     'partition_keys': ['date'],
                     'target_path': 'download-{}.nc',
                     'force_download': False
                 },
                 'selection': {
                     'features': ['pressure'],
                     'date': ['2017-01-15'],
                 }
             },
             expected='download-2017-01-15.nc'),
        dict(case='Has Directory, but no date',
             config={
                 'parameters': {
                     'target_path': 'somewhere/download/{:02d}/{:02d}.nc',
                     'partition_keys': ['year', 'month'],
                     'force_download': False
                 },
                 'selection': {
                     'features': ['pressure'],
                     'month': ['02'],
                     'year': ['02']
                 }
             },
             expected='somewhere/download/02/02.nc'),
        dict(case='Had date and target directory',
             config={
                 'parameters': {
                     'partition_keys': ['date'],
                     'target_path': 'somewhere/{date:%Y/%m/%d}-download.nc',
                     'force_download': False
                 },
                 'selection': {
                     'date': ['2017-01-15'],
                 }
             },
             expected='somewhere/2017/01/15-download.nc'),
        dict(case='Had date, target directory, and additional params.',
             config={
                 'parameters': {
                     'partition_keys': ['date', 'pressure_level'],
                     'target_path': 'somewhere/{date:%Y/%m/%d}-pressure-{pressure_level}.nc',
                     'force_download': False
                 },
                 'selection': {
                     'features': ['pressure'],
                     'pressure_level': ['500'],
                     'date': ['2017-01-15'],
                 }
             },
             expected='somewhere/2017/01/15-pressure-500.nc'),
        dict(case='Has date and target directory, including parameters in path.',
             config={
                 'parameters': {
                     'partition_keys': ['date', 'expver', 'pressure_level'],
                     'target_path': 'somewhere/expver-{expver}/{date:%Y/%m/%d}-pressure-{pressure_level}.nc',
                     'force_download': False
                 },
                 'selection': {
                     'features': ['pressure'],
                     'pressure_level': ['500'],
                     'date': ['2017-01-15'],
                     'expver': ['1'],
                 }
             },
             expected='somewhere/expver-1/2017/01/15-pressure-500.nc'),

    ]

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('dummy-manifest'))

    def test_target_name(self):
        for it in self.TEST_CASES:
            with self.subTest(msg=it['case'], **it):
                actual = prepare_target_name(Config.from_dict(it['config']))
                self.assertEqual(actual, it['expected'])


if __name__ == '__main__':
    unittest.main()
