import requests
import json
from time import time

def timeit(func):
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f'[executed in {(t2-t1):.4f}s.]')
        return result
    return wrap_func


class NetworkService:
    def parse_response(self, response: requests.Response):
        parsed = json.loads(response.text)
        return json.dumps(parsed, indent=3)
        
    @timeit
    def get(self, uri, header, query=None, payload=None):
        try:
            x = requests.get(uri, params=query, headers=header, data=payload)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    @timeit
    def post(self, uri, header, query=None, payload=None, file=None):
        try:
            x = requests.post(uri, params=query, headers=header, data=payload, files=file)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    @timeit
    def put(self, uri, header, query=None, payload=None, file=None):
        try:
            x = requests.put(uri, params=query, headers=header, data=payload, files=file)

            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    @timeit
    def delete(self, uri, header, query=None):
        try:
            x =  requests.delete(uri, params=query, headers=header)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
network_service = NetworkService()