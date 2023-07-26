import dataclasses
import typing as t


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


firestore_dict = {
    "DOWNLOAD_COLLECTION": "download",
    "QUEUES_COLLECTION": "queues",
    "LICENSE_COLLECTION": "license",
}


def get_config():
    return ServerConfig.from_dict(firestore_dict)
