# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""ECMWF Downloader Clients."""

import abc
import collections
import contextlib
import io
import json
import logging
import os
import typing as t

import cdsapi
from ecmwfapi import ECMWFService


class Client(abc.ABC):
    """Downloader client interface.

    Defines allowed operations on clients.
    """

    def __init__(self, config: t.Dict, level: int = logging.INFO) -> None:
        """Clients are initialized with the general CLI configuration."""
        self.config = config
        self.logger = logging.getLogger(f'{__name__}.{type(self).__name__}')
        self.logger.setLevel(level)

    @abc.abstractmethod
    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        """Download from data source."""
        pass

    @abc.abstractmethod
    def num_requests_per_key(self, dataset: str) -> int:
        """Specifies the number of workers to be used per api key for the dataset."""
        pass


class CdsClient(Client):
    """Cloud Data Store Client"""

    """Name patterns of datasets that are hosted internally on CDS servers."""
    cds_hosted_datasets = {'reanalysis-era'}

    def __init__(self, config: t.Dict) -> None:
        super().__init__(config)
        self.c = cdsapi.Client(
            url=config['parameters'].get('api_url', os.environ.get('CDSAPI_URL')),
            key=config['parameters'].get('api_key', os.environ.get('CDSAPI_KEY')),
            debug_callback=self.logger.debug,
            info_callback=self.logger.info,
            warning_callback=self.logger.warning,
            error_callback=self.logger.error,
        )

    def retrieve(self, dataset: str, selection: t.Dict, target: str) -> None:
        self.c.retrieve(dataset, selection, target)

    def num_requests_per_key(self, dataset: str) -> int:
        """Number of requests per key from the CDS API.

        CDS has dynamic, data-specific limits, defined here:
          https://cds.climate.copernicus.eu/live/limits

        Typically, the reanalysis dataset allows for 3-5 simultaneous requets.
        For all standard CDS data (backed on disk drives), it's common that 2
        requests are allowed, though this is dynamically set, too.

        If the Beam pipeline encounters a user request limit error, please cancel
        all outstanding requests (per each user account) at the following link:
        https://cds.climate.copernicus.eu/cdsapp#!/yourrequests
        """
        # TODO(#15): Parse live CDS limits API to set data-specific limits.
        for internal_set in self.cds_hosted_datasets:
            if dataset.startswith(internal_set):
                return 5
        return 2


class StdoutLogger(io.StringIO):
    """Special logger to redirect stdout to logs."""

    def __init__(self, logger_: t.Optional[logging.Logger] = None, level: int = logging.INFO):
        super().__init__()
        if logger_ is None:
            logger_ = logging.getLogger(__name__)
        self.logger = logger_
        self.level = level
        self._redirector = contextlib.redirect_stdout(self)

    def log(self, msg) -> None:
        self.logger.log(self.level, msg)

    def write(self, msg):
        if msg and not msg.isspace():
            self.logger.log(self.level, msg)

    def __enter__(self):
        self._redirector.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # let contextlib do any exception handling here
        self._redirector.__exit__(exc_type, exc_value, traceback)


class MarsClient(Client):
    """MARS Client"""

    def __init__(self, config: t.Dict) -> None:
        super().__init__(config)
        self.c = ECMWFService(
            "mars",
            key=config['parameters'].get('api_key', os.environ.get("ECMWF_API_KEY")),
            url=config['parameters'].get('api_url', os.environ.get("ECMWF_API_URL")),
            email=config['parameters'].get('api_email', os.environ.get("ECMWF_API_EMAIL")),
            log=self.logger.debug,
            verbose=True
        )

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        with StdoutLogger(self.logger, level=logging.DEBUG):
            self.c.execute(req=selection, target=output)

    def num_requests_per_key(self, dataset: str) -> int:
        """Number of requests per key (or user) for the Mars API.

        Mars allows 2 active requests per user and 20 queued requests per user, as of Sept 27, 2021.
        To ensure we never hit a rate limit error during download, we return a slightly smaller
        number of requests than the possible limit (20/22 requets).
        See: https://confluence.ecmwf.int/display/UDOC/Total+number+of+requests+a+user+can+submit+-+Web+API+FAQ

        Queued requests can _only_ be canceled manually from a web dashboard. If the
        `ERROR 101 (USER_QUEUED_LIMIT_EXCEEDED)` error occurs in the Beam pipeline, then go to
        http://apps.ecmwf.int/webmars/joblist/ and cancel queued jobs.
        """
        return 20


class FakeClient(Client):
    """A client that writes the selection arguments to the output file. """

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        self.logger.debug(f'Downloading {dataset} to {output}')
        with open(output, 'w') as f:
            json.dump({dataset: selection}, f)

    def num_requests_per_key(self, dataset: str) -> int:
        return 1


CLIENTS = collections.OrderedDict(
    cds=CdsClient,
    mars=MarsClient,
    fake=FakeClient,
)
