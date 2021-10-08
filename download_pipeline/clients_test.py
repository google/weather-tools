import unittest

from .clients import FakeClient, CdsClient, MarsClient


class MaxWorkersTest(unittest.TestCase):
    def test_cdsclient_internal(self):
        client = CdsClient({'parameters': {'api_url': 'url', 'api_key': 'key'}})
        self.assertEqual(
            client.num_requests_per_key("reanalysis-era5-some-data"), 3)

    def test_cdsclient_mars_hosted(self):
        client = CdsClient({'parameters': {'api_url': 'url', 'api_key': 'key'}})
        self.assertEqual(
            client.num_requests_per_key("reanalysis-carra-height-levels"), 1)

    def test_marsclient(self):
        client = MarsClient({'parameters': {}})
        self.assertEqual(
            client.num_requests_per_key("reanalysis-era5-some-data"), 1)

    def test_fakeclient(self):
        client = FakeClient({'parameters': {}})
        self.assertEqual(
            client.num_requests_per_key("reanalysis-era5-some-data"), 1)


if __name__ == '__main__':
    unittest.main()
