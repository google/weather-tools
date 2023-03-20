import unittest

from .util import fetch_geo_polygon, ichunked


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
        area = [40, -75, 39, -74]
        expected_result = (
            '{"type": "Polygon", "coordinates": '
            '[[[-75, 40], [-75, 39], [-74, 39], [-74, 40], [-75, 40]]]}'
        )
        self.assertEqual(fetch_geo_polygon(area), expected_result)

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
