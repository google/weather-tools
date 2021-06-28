"""ECMWF Downloader Clients."""

import abc
import cdsapi
import collections
import contextlib
import io
import logging
import os
import json

import typing as t

import apache_beam as beam
from ecmwfapi import ECMWFService

logger = logging.getLogger(__name__)


class Client(abc.ABC):
    """Downloader client interface.

    Defines allowed operations on clients.
    """

    @abc.abstractmethod
    def __init__(self, config: t.Dict) -> None:
        """Clients are initialized with the general CLI configuration."""
        pass

    @abc.abstractmethod
    def retrieve(self, dataset: str, selection: t.Dict, output: str, log_prepend: str = "") -> None:
        """Download from data source."""
        pass


class CdsClient(Client):
    """Cloud Data Store Client"""

    def __init__(self, config: t.Dict) -> None:
        self.c = cdsapi.Client(
            url=config['parameters'].get('api_url', os.environ.get('CDSAPI_URL')),
            key=config['parameters'].get('api_key', os.environ.get('CDSAPI_KEY')),
        )

    def retrieve(self, dataset: str, selection: t.Dict, target: str, log_prepend: str = "") -> None:
        # TODO(b/171910744): implement log_prepend and more sophisticated CDS logging
        self.c.retrieve(dataset, selection, target)


class MarsLogger(io.StringIO):
    """Special logger to redirect ecmwf api's stdout to dataflow logs."""

    def __init__(self, prepend=""):
        super().__init__()
        self._redirector = contextlib.redirect_stdout(self)
        self.prepend = prepend

    def log(self, msg) -> None:
        """Prepends current file being retrieved and monitors for special ECMWF msgs."""

        logger.info(self.prepend + " - " + msg)

        if msg == "Request is active":
            logger.info("Incrementing count of Active ECMWF Requests")
            beam.metrics.Metrics.counter('weather-dl', 'Active ECMWF Requests').inc()
        elif msg == "Done.":
            logger.info("Incrementing count of Active ECMWF Requests")
            beam.metrics.Metrics.counter('weather-dl', 'Complete ECMWF Requests').inc()

    def write(self, msg):
        if msg and not msg.isspace():
            self.log(msg)

    def __enter__(self):
        self._redirector.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # let contextlib do any exception handling here
        self._redirector.__exit__(exc_type, exc_value, traceback)


class MarsClient(Client):
    """MARS Client"""

    def __init__(self, config: t.Dict) -> None:

        with MarsLogger() as logger:
            self.c = ECMWFService(
                "mars",
                key=config['parameters'].get('api_key', os.environ.get("ECMWF_API_KEY")),
                url=config['parameters'].get('api_url', os.environ.get("ECMWF_API_URL")),
                email=config['parameters'].get('api_email', os.environ.get("ECMWF_API_EMAIL")),
                log=logger.log,
                verbose=True
            )

    def retrieve(self, dataset: str, selection: t.Dict, output: str, log_prepend: str = "") -> None:

        with MarsLogger(log_prepend):
            self.c.execute(req=selection, target=output)


class FakeClient(Client):
    """A client that writes the selection arguments to the output file. """

    def __init__(self, config: t.Dict) -> None:
        self.config = config

    def retrieve(self, dataset: str, selection: t.Dict, output: str, log_prepend: str = "") -> None:
        logger.debug(f'Downloading {dataset} to {output}')
        with open(output, 'w') as f:
            json.dump({dataset: selection}, f)


CLIENTS = collections.OrderedDict(
    cds=CdsClient,
    mars=MarsClient,
)
