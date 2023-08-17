import dataclasses
import typing as t
import json

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class DeploymentConfig:
    download_collection: str = ""
    queues_collection: str = ""
    license_collection: str = ""
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


deployment_config = None


def get_config():
    global deployment_config
    deployment_config_json = "deployment_config.json"

    if deployment_config is None:
        with open(deployment_config_json) as file:
            firestore_dict = json.load(file)
            deployment_config = DeploymentConfig.from_dict(firestore_dict)

    return deployment_config
