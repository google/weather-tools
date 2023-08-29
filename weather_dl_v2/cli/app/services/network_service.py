import requests
import json
import logging
from app.utils import Loader, timeit

logger = logging.getLogger(__name__)


class NetworkService:

    def parse_response(self, response: requests.Response):
        try:
            parsed = json.loads(response.text)
        except Exception as e:
            logger.info(f"Parsing error: {e}.")
            logger.info(f"Status code {response.status_code}")
            logger.info(f"Response {response.text}")
            return

        if isinstance(parsed, list):
            print(f"[Total {len(parsed)} items.]")

        return json.dumps(parsed, indent=3)

    @timeit
    def get(self, uri, header, query=None, payload=None):
        try:
            with Loader("Sending request..."):
                x = requests.get(uri, params=query, headers=header, data=payload)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            logger.error(f"request error: {e}")
            raise SystemExit(e)

    @timeit
    def post(self, uri, header, query=None, payload=None, file=None):
        try:
            with Loader("Sending request..."):
                x = requests.post(
                    uri, params=query, headers=header, data=payload, files=file
                )
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            logger.error(f"request error: {e}")
            raise SystemExit(e)

    @timeit
    def put(self, uri, header, query=None, payload=None, file=None):
        try:
            with Loader("Sending request..."):
                x = requests.put(
                    uri, params=query, headers=header, data=payload, files=file
                )
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            logger.error(f"request error: {e}")
            raise SystemExit(e)

    @timeit
    def delete(self, uri, header, query=None):
        try:
            with Loader("Sending request..."):
                x = requests.delete(uri, params=query, headers=header)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            logger.error(f"request error: {e}")
            raise SystemExit(e)


network_service = NetworkService()
