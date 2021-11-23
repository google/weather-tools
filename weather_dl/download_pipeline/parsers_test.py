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

from .parsers import date, parse_config, process_config, _number_of_replacements, parse_subsections


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

    def test_json(self):
        with io.StringIO('{"key": "value"}') as f:
            actual = parse_config(f)
            self.assertDictEqual(actual, {'key': 'value'})

    def test_bad_json(self):
        with io.StringIO('{"key": "value", "brokenKey": }') as f:
            actual = parse_config(f)
            self.assertDictEqual(actual, {})

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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
            self.assertListEqual(actual['section']['list'], ['1', '3', '5'])

    def test_cfg_parses_mars_float_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=1.0/to/5.0
                """
        ) as f:
            actual = parse_config(f)
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
            self.assertEqual(actual['section']['list'], ['0.0', '0.1', '0.2', '0.30000000000000004', '0.4', '0.5'])

    def test_cfg_parses_mars_date_range(self):
        with io.StringIO(
                """
                [section]
                key=value
                list=2020-01-07/to/2020-01-09
                """
        ) as f:
            actual = parse_config(f)
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)

            dates = [
                datetime.date.today() + datetime.timedelta(-3),
                datetime.date.today() + datetime.timedelta(-2),
                datetime.date.today() + datetime.timedelta(-1),
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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
            self.assertListEqual(actual['section']['list'], ['2020-01-07', '2020-01-09', '2020-01-11'])

    def test_cfg_raises_syntax_error_missing_right(self):
        with self.assertRaises(SyntaxError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/to/
                    """
            ) as f:
                parse_config(f)

        self.assertEqual("Improper range syntax in '2020-01-07/to/'.", ctx.exception.args[0])

    def test_cfg_raises_syntax_error_missing_left(self):
        with self.assertRaises(SyntaxError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=/to/2020-01-07
                    """
            ) as f:
                parse_config(f)

        self.assertEqual("Improper range syntax in '/to/2020-01-07'.", ctx.exception.args[0])

    def test_cfg_raises_syntax_error_missing_increment(self):
        with self.assertRaises(SyntaxError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/to/2020-01-11/by/
                    """
            ) as f:
                parse_config(f)

        self.assertEqual("Improper range syntax in '2020-01-07/to/2020-01-11/by/'.", ctx.exception.args[0])

    def test_cfg_raises_syntax_error_no_range(self):
        with self.assertRaises(SyntaxError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/by/2020-01-11
                    """
            ) as f:
                parse_config(f)

        self.assertEqual("Improper range syntax in '2020-01-07/by/2020-01-11'.", ctx.exception.args[0])

    def test_cfg_raises_value_error_date_types(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=2020-01-07/to/2020-01-11/by/2.0
                    """
            ) as f:
                parse_config(f)

        self.assertEqual(
            "Increments on a date range must be integer number of days, '2.0' is invalid.",
            ctx.exception.args[0]
        )

    def test_cfg_raises_value_error_float_types(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=1.0/to/10.0/by/2020-01-07
                    """
            ) as f:
                parse_config(f)

        self.assertEqual(
            "Range tokens (start='1.0', end='10.0', increment='2020-01-07') are inconsistent types.",
            ctx.exception.args[0]
        )

    def test_cfg_raises_value_error_int_types(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [section]
                    key=value
                    list=1/to/10/by/2.0
                    """
            ) as f:
                parse_config(f)

        self.assertEqual(
            "Range tokens (start='1', end='10', increment='2.0') are inconsistent types.",
            ctx.exception.args[0]
        )

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
            for key, val in actual['section'].items():
                self.assertNotIn('\n', val)
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
                    'num_api_keys': 2,
                },
            })


class HelpersTest(unittest.TestCase):

    def test_number_of_replacements(self):
        for (s, want) in [('', 0), ('{} blah', 1), ('{} {}', 2),
                          ('{0}, {1}', 2), ('%s hello', 0), ('hello {.2f}', 1)]:
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
                                         'param2': {'api_key': 'key2'},
                                         'num_api_keys': 2}})


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
                    [otherSection]
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
            params = config.get('parameters', {})
            self.assertIsInstance(params['partition_keys'], list)

    def test_params_in_config(self):
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
            self.assertIn('parameters', config)

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

    def test_date_as_directory_key_mismatch(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=somewhere/
                target_filename=bar-{}
                append_date_dirs=true
                partition_keys=
                    date
                [selection]
                date=2017-01-01/to/2017-01-01
                """
            ) as f:
                process_config(f)

        self.assertIn(
            "'target_path' has 1 replacements. Expected 0",
            ctx.exception.args[0])

    def test_append_date_dirs_without_filename(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=somewhere/
                append_date_dirs=true
                partition_keys=
                    date
                [selection]
                date=2017-01-01/to/2017-01-01
                """
            ) as f:
                process_config(f)

        self.assertIn(
            "'append_date_dirs' set to true, but creating the date directory hierarchy",
            ctx.exception.args[0])

    def test_append_date_dirs_without_date_partition(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                """
                [parameters]
                dataset=foo
                client=cds
                target_path=somewhere/
                target_filename=bar
                append_date_dirs=true
                partition_keys=
                    pressure
                [selection]
                pressure=500
                """
            ) as f:
                process_config(f)

        self.assertIn(
            "'append_date_dirs' set to true, but creating the date directory hierarchy",
            ctx.exception.args[0])

    def test_date_as_directory_target_directory_ends_in_slash(self):
        with io.StringIO(
            """
            [parameters]
            dataset=foo
            client=cds
            target_path=somewhere/
            target_filename=bar
            append_date_dirs=true
            partition_keys=
                date
            [selection]
            date=2017-01-01/to/2017-01-01
            """
        ) as f:
            config = process_config(f)
            self.assertEqual(config['parameters']['target_path'], "somewhere")

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


if __name__ == '__main__':
    unittest.main()
