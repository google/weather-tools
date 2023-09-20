import dataclasses
import typing as t
import json
import os
import logging

logger = logging.getLogger(__name__)

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class DownloaderConfig:
    manifest_collection: str = ""
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


downloader_config = None


def get_config():
    global downloader_config
    if downloader_config:
        return downloader_config

    downloader_config_json = "config/config.json"
    if not os.path.exists(downloader_config_json):
        downloader_config_json = os.environ.get('CONFIG_PATH', None)

    if downloader_config_json is None:
        logger.error(f"Couldn't load config file for downloader.")
        raise FileNotFoundError("Couldn't load config file for downloader.")

    with open(downloader_config_json) as file:
        config_dict = json.load(file)
        downloader_config = DownloaderConfig.from_dict(config_dict)

    return downloader_config
