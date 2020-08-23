import argparse
import datetime
import io
import unittest

from ecmwf_pipeline.parsers import date, parse_config


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


if __name__ == '__main__':
    unittest.main()
