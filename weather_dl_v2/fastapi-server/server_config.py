import dataclasses
import typing as t
import json

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class ServerConfig:
    download_collection: str = ""
    queues_collection: str = ""
    license_collection: str = ""
    manifest_collection: str = ""
    license_deployment_image: str = ""
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
    server_config_json = "server_config.json"

    if server_config is None:
        with open(server_config_json) as file:
            config_dict = json.load(file)
            server_config = ServerConfig.from_dict(config_dict)

    return server_config
