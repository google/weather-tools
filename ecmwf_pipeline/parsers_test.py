import argparse
import datetime
import io
import unittest

from ecmwf_pipeline.parsers import date, parse_config, process_config, _number_of_replacements


class DateTest(unittest.TestCase):
    def test_parses_correct_date(self):
        self.assertEqual(date('2020-08-22'), datetime.date(year=2020, month=8, day=22))
        self.assertEqual(date('1900-11-01'), datetime.date(year=1900, month=11, day=1))

    def test_throws_argparse_error(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            date('2020-08')

        with self.assertRaises(argparse.ArgumentTypeError):
            date('2020-08-22-12')

        with self.assertRaises(argparse.ArgumentTypeError):
            date('2020-0822')

        with self.assertRaises(argparse.ArgumentTypeError):
            date('20-08-22')

        with self.assertRaises(argparse.ArgumentTypeError):
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
            self.assertIsInstance(actual['section']['list'], list)


class HelpersTest(unittest.TestCase):

    def test_number_of_replacements(self):
        for (s, want) in [('', 0), ('{} blah', 1), ('{} {}', 2),
                          ('{0}, {1}', 2), ('%s hello', 0), ('hello {.2f}', 1)]:
            with self.subTest(s=s, want=want):
                actual = _number_of_replacements(s)
                self.assertEqual(actual, want)


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

    def test_requires_dataset_param(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    notDataset=foo 
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'parameters' section requires a 'dataset' key.",
            ctx.exception.args[0])

    def test_accepts_dataset_param(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo 
                    """
            ) as f:
                process_config(f)

        self.assertNotIn(
            "'parameters' section requires a 'dataset' key.",
            ctx.exception.args[0])

    def test_requires_target_template_param(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo 
                    target=bar
                    """
            ) as f:
                process_config(f)

        self.assertIn(
            "'parameters' section requires a 'target_template' key.",
            ctx.exception.args[0])

    def test_accepts_target_template_param(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo 
                    target_template
                    """
            ) as f:
                process_config(f)

        self.assertNotIn(
            "'parameters' section requires a 'target_template' key.",
            ctx.exception.args[0])

    def test_requires_partition_keys_to_match_sections(self):
        with self.assertRaises(ValueError) as ctx:
            with io.StringIO(
                    """
                    [parameters]
                    dataset=foo 
                    target_template=bar-{}-{}
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
                target_template=bar-{}-{}
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
                target_template=bar-{}
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
                target_template=bar-{}-{}
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
                    target_template=bar-{}
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

        self.assertIn(
            "target_template` has 1 replacements. Expected 2",
            ctx.exception.args[0])


if __name__ == '__main__':
    unittest.main()
