import abc
import logging
import dataclasses
import typing as t
import json

logger = logging.getLogger(__name__)

@dataclasses.dataclass
class Validator(abc.ABC):

    valid_keys: t.List[str]

    def validate(self, filters: t.List[str], show_valid_filters=True):
        filter_dict = {}

        for filter in filters:
            _filter = filter.split("=")

            if(len(_filter)!=2):
                if show_valid_filters:
                    logger.info(f"valid filters are: {self.valid_keys}.")
                raise ValueError("Incorrect Filter. Please Try again.")

            key, value = _filter
            filter_dict[key] = value

        data_set = set(filter_dict.keys())
        valid_set = set(self.valid_keys)

        if self._validate_keys(data_set, valid_set):
            return filter_dict

    def validate_json(self, file_path):
        try:
            with open(file_path) as f:
                data: dict = json.load(f)
                data_keys = data.keys()

                data_set = set(data_keys)
                valid_set = set(self.valid_keys)

                if self._validate_keys(data_set, valid_set):
                    return data

        except FileNotFoundError:
            logger.info("file not found.")
            raise FileNotFoundError

    def _validate_keys(self, data_set: set, valid_set: set):
        if data_set == valid_set:
            return True

        missing_keys = valid_set.difference(data_set)
        invalid_keys = data_set.difference(valid_set)

        if len(missing_keys) > 0:
            raise ValueError(f"keys {missing_keys} are missing in file.")

        if len(invalid_keys) > 0:
            raise ValueError(f"keys {invalid_keys} are invalid keys.")

        return False
