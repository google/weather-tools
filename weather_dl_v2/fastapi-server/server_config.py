import dataclasses
import typing as t
import json

@dataclasses.dataclass
class ServerConfig:
    DOWNLOAD_COLLECTION: str = ""
    QUEUES_COLLECTION: str = ""
    LICENSE_COLLECTION: str = ""

    @classmethod
    def from_dict(cls, config: t.Dict):
        config_instance = cls()

        for key, value in config.items():
            if hasattr(config_instance, key):
                setattr(config_instance, key, value)
            else:
                config_instance.kwargs[key] = value

        return config_instance

server_config = None

def get_config():
    global server_config
    server_config_json = "server_config.json"

    if(server_config is None):
        with open(server_config_json) as file:
            firestore_dict = json.load(file)
            server_config = ServerConfig.from_dict(firestore_dict)

    return server_config
