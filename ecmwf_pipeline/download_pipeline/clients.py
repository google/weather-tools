import abc
import cdsapi
import collections
import contextlib
import logging
import os

import typing as t

from ecmwfapi import ECMWFService
from .logging import LoggerIO


class Client(abc.ABC):
    """Downloader client interface.

    Defines allowed operations on clients.
    """

    @abc.abstractmethod
    def __init__(self, config: t.Dict) -> None:
        """Clients are initialized with the general CLI configuration."""
        pass

    @abc.abstractmethod
    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        """Download from data source."""
        pass


class CdsClient(Client):
    """Cloud Data Store Client"""

    def __init__(self, config: t.Dict) -> None:
        self.c = cdsapi.Client(
            url=config['parameters'].get('api_url', os.environ.get('CDSAPI_URL')),
            key=config['parameters'].get('api_key', os.environ.get('CDSAPI_KEY')),
        )

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        self.c.retrieve(dataset, selection, output)


class MarsClient(Client):
    """MARS Client"""

    def __init__(self, config: t.Dict) -> None:
        self.c = ECMWFService(
            "mars",
            key=config['parameters'].get('api_key', os.environ.get("ECMWF_API_KEY")),
            url=config['parameters'].get('api_url', os.environ.get("ECMWF_API_URL")),
            email=config['parameters'].get('api_email', os.environ.get("ECMWF_API_EMAIL")),
            log=logging.INFO
        )

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        with LoggerIO(logging.getLogger(), logging.ERROR) as e, contextlib.redirect_stderr(e):
            with LoggerIO(logging.getLogger(), logging.INFO) as f, contextlib.redirect_stdout(f):
                self.c.execute(req=selection, target=output)


CLIENTS = collections.OrderedDict(
    cds=CdsClient,
    mars=MarsClient,
)
