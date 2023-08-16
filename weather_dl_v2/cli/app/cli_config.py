import dataclasses
import typing as t
import json

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class CliConfig:
    pod_ip: str = ""
    port: str = ""

    @property
    def BASE_URI(self) -> str:
        return f"http://{self.pod_ip}:{self.port}"

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


cli_config = None


def get_config():
    global cli_config
    cli_config_json = "./cli_config.json"

    if cli_config is None:
        with open(cli_config_json) as file:
            firestore_dict = json.load(file)
            cli_config = CliConfig.from_dict(firestore_dict)

    return cli_config
