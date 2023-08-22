import dataclasses
import typing as t
import json

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
    downloader_config_json = "downloader_config.json"

    if downloader_config is None:
        with open(downloader_config_json) as file:
            firestore_dict = json.load(file)
            downloader_config = DownloaderConfig.from_dict(firestore_dict)

    return downloader_config
