import unittest

from .util import fetch_geo_polygon, generate_hdate, ichunked


# TODO(#245): Duplicate tests; remove.
class IChunksTests(unittest.TestCase):
    def setUp(self) -> None:
        self.items = range(20)

    def test_even_chunks(self):
        actual = []
        for chunk in ichunked(self.items, 4):
            actual.append(list(chunk))

        self.assertEqual(actual, [
            [0, 1, 2, 3],
            [4, 5, 6, 7],
            [8, 9, 10, 11],
            [12, 13, 14, 15],
            [16, 17, 18, 19],
        ])

    def test_odd_chunks(self):
        actual = []
        for chunk in ichunked(self.items, 7):
            actual.append(list(chunk))

        self.assertEqual(actual, [
            [0, 1, 2, 3, 4, 5, 6],
            [7, 8, 9, 10, 11, 12, 13],
            [14, 15, 16, 17, 18, 19]
        ])


class TestFetchGeoPolygon(unittest.TestCase):
    def test_valid_area(self):
        # Test with valid area values.
        area = ['40', '-75', '39', '-74']
        expected_result = (
            '{"type": "Polygon", "coordinates": '
            '[[[-75.0, 40.0], [-75.0, 39.0], [-74.0, 39.0], [-74.0, 40.0], [-75.0, 40.0]]]}'
        )
        self.assertEqual(fetch_geo_polygon(area), expected_result)

    def test_valid_string_area_value(self):
        # Test with valid string area value.
        area = 'E'
        expected_result = (
            '{"type": "Polygon", "coordinates": '
            '[[[-27.0, 73.5], [-27.0, 33.0], [45.0, 33.0], [45.0, 73.5], [-27.0, 73.5]]]}'
        )
        self.assertEqual(fetch_geo_polygon(area), expected_result)

    def test_invalid_string_area_value(self):
        # Test with invalid string area value.
        area = 'B'
        with self.assertRaises(RuntimeError):
            fetch_geo_polygon(area)

    def test_invalid_latitude_south(self):
        # Test with invalid south latitude value
        area = [40, -75, -91, -74]
        with self.assertRaises(ValueError):
            fetch_geo_polygon(area)

    def test_invalid_latitude_north(self):
        # Test with invalid north latitude value
        area = [91, -75, 39, -74]
        with self.assertRaises(ValueError):
            fetch_geo_polygon(area)

    def test_invalid_longitude_west(self):
        # Test with invalid west longitude value
        area = [40, -181, 39, -74]
        with self.assertRaises(ValueError):
            fetch_geo_polygon(area)

    def test_invalid_longitude_east(self):
        # Test with invalid east longitude value
        area = [40, -75, 39, 181]
        with self.assertRaises(ValueError):
            fetch_geo_polygon(area)


class TestGenerateHdate(unittest.TestCase):
    def test_valid_hdate(self):
        date = '2020-01-02'
        substract_year = '4'
        expected_result = '2016-01-02'
        self.assertEqual(generate_hdate(date, substract_year), expected_result)
