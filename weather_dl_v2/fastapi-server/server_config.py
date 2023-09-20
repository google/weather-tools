import dataclasses
import typing as t
import json
import os
import logging

logger = logging.getLogger(__name__)

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class ServerConfig:
    download_collection: str = ""
    queues_collection: str = ""
    license_collection: str = ""
    manifest_collection: str = ""
    storage_bucket: str = ""
    gcs_project: str = ""
    license_deployment_image: str = ""
    welcome_message: str = ""
    kwargs: t.Optional[t.Dict[str, Values]] = dataclasses.field(default_factory=dict)

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
    if server_config:
        return server_config

    server_config_json = "config/config.json"
    if not os.path.exists(server_config_json):
        server_config_json = os.environ.get('CONFIG_PATH', None)
    
    if server_config_json is None:
        logger.error(f"Couldn't load config file for fastAPI server.")
        raise FileNotFoundError("Couldn't load config file for fastAPI server.")

    with open(server_config_json) as file:
        config_dict = json.load(file)
        server_config = ServerConfig.from_dict(config_dict)

    return server_config
