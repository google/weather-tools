import requests
import json

class NetworkService:
    def parse_response(self, response: requests.Response):
        parsed = json.loads(response.text)
        return json.dumps(parsed, indent=3)
        
    def get(self, uri, header, query=None, payload=None):
        try:
            x = requests.get(uri, params=query, headers=header, data=payload)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    def post(self, uri, header, query=None, payload=None, file=None):
        try:
            x = requests.post(uri, params=query, headers=header, data=payload, files=file)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    def put(self, uri, header, query=None, payload=None, file=None):
        try:
            x = requests.put(uri, params=query, headers=header, data=payload, files=file)

            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
    def delete(self, uri, header, query=None):
        try:
            x =  requests.delete(uri, params=query, headers=header)
            return self.parse_response(x)
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)
        
network_service = NetworkService()