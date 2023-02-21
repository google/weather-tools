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
import datetime
import io
import json
import logging
import os
import subprocess
import typing as t
import warnings
from urllib.parse import urljoin

import cdsapi
import urllib3
from ecmwfapi import api

from .config import Config, optimize_selection_partition
from .manifest import Manifest, Stage
from .util import retry_with_exponential_backoff

warnings.simplefilter(
    "ignore", category=urllib3.connectionpool.InsecureRequestWarning)


class Client(abc.ABC):
    """Weather data provider client interface.

    Defines methods and properties required to efficiently interact with weather
    data providers.

    Attributes:
        config: A config that contains pipeline parameters, such as API keys.
        level: Default log level for the client.
    """

    def __init__(self, config: Config, level: int = logging.INFO) -> None:
        """Clients are initialized with the general CLI configuration."""
        self.config = config
        self.logger = logging.getLogger(f'{__name__}.{type(self).__name__}')
        self.logger.setLevel(level)

    @abc.abstractmethod
    def retrieve(self, dataset: str, selection: t.Dict, output: str, manifest: Manifest) -> None:
        """Download from data source."""
        pass

    @classmethod
    @abc.abstractmethod
    def num_requests_per_key(cls, dataset: str) -> int:
        """Specifies the number of workers to be used per api key for the dataset."""
        pass

    @property
    @abc.abstractmethod
    def license_url(self):
        """Specifies the License URL."""
        pass


class CdsClient(Client):
    """A client to access weather data from the Cloud Data Store (CDS).

    Datasets on CDS can be found at:
      https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset

    The parameters section of the input `config` requires two values: `api_url` and
    `api_key`. Or, these values can be set as the environment variables: `CDSAPI_URL`
    and `CDSAPI_KEY`. These can be acquired from the following URL, which requires
    creating a free account: https://cds.climate.copernicus.eu/api-how-to

    The CDS global queues for data access has dynamic rate limits. These can be viewed
    live here: https://cds.climate.copernicus.eu/live/limits.

    Attributes:
        config: A config that contains pipeline parameters, such as API keys.
        level: Default log level for the client.
    """

    """Name patterns of datasets that are hosted internally on CDS servers."""
    cds_hosted_datasets = {'reanalysis-era'}

    def __init__(self, config: Config, level: int = logging.INFO) -> None:
        super().__init__(config, level)
        self.c = cdsapi.Client(
            url=config.kwargs.get('api_url', os.environ.get('CDSAPI_URL')),
            key=config.kwargs.get('api_key', os.environ.get('CDSAPI_KEY')),
            debug_callback=self.logger.debug,
            info_callback=self.logger.info,
            warning_callback=self.logger.warning,
            error_callback=self.logger.error,
        )

    def retrieve(self, dataset: str, selection: t.Dict, output: str, manifest: Manifest) -> None:
        selection_ = optimize_selection_partition(selection)
        manifest.set_stage(Stage.RETRIEVE)
        precise_retrieve_start_time = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec='seconds')
        )
        manifest.prev_stage_precise_start_time = precise_retrieve_start_time
        self.c.retrieve(dataset, selection_, output)

    @property
    def license_url(self):
        return 'https://cds.climate.copernicus.eu/api/v2/terms/static/licence-to-use-copernicus-products.pdf'

    @classmethod
    def num_requests_per_key(cls, dataset: str) -> int:
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
        for internal_set in cls.cds_hosted_datasets:
            if dataset.startswith(internal_set):
                return 5
        return 2


class StdoutLogger(io.StringIO):
    """Special logger to redirect stdout to logs."""

    def __init__(self, logger_: logging.Logger, level: int = logging.INFO):
        super().__init__()
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


class SplitMARSRequest(api.APIRequest):
    """Extended MARS APIRequest class that separates fetch and download stage."""
    @retry_with_exponential_backoff
    def _download(self, url, path: str, size: int) -> None:
        self.log(
            "Transferring %s into %s" % (self._bytename(size), path)
        )
        self.log("From %s" % (url,))

        dir_path, file_name = os.path.split(path)
        try:
            subprocess.run(
                ['aria2c', '-x', '16', '-s', '16', url, '-d', dir_path, '-o', file_name, '--allow-overwrite'],
                check=True,
                capture_output=True)
        except subprocess.CalledProcessError as e:
            self.log(f'Failed download from ECMWF server {url!r} to {path!r} due to {e.stderr.decode("utf-8")}')

    def fetch(self, request: t.Dict) -> t.Dict:
        status = None

        self.connection.submit("%s/%s/requests" % (self.url, self.service), request)
        self.log("Request submitted")
        self.log("Request id: " + self.connection.last.get("name"))
        if self.connection.status != status:
            status = self.connection.status
            self.log("Request is %s" % (status,))

        while not self.connection.ready():
            if self.connection.status != status:
                status = self.connection.status
                self.log("Request is %s" % (status,))
            self.connection.wait()

        if self.connection.status != status:
            status = self.connection.status
            self.log("Request is %s" % (status,))

        result = self.connection.result()
        return result

    def download(self, result: t.Dict, target: t.Optional[str] = None) -> None:
        if target:
            if os.path.exists(target):
                # Empty the target file, if it already exists, otherwise the
                # transfer below might be fooled into thinking we're resuming
                # an interrupted download.
                open(target, "w").close()

            self._download(urljoin(self.url, result["href"]), target, result["size"])
        self.connection.cleanup()


class SplitRequestMixin:
    c = None

    def fetch(self, req: t.Dict) -> t.Dict:
        return self.c.fetch(req)

    def download(self, res: t.Dict, target: str) -> None:
        self.c.download(res, target)


class MARSECMWFServiceExtended(api.ECMWFService, SplitRequestMixin):
    """Extended MARS ECMFService class that separates fetch and download stage."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.c = SplitMARSRequest(
            self.url,
            "services/%s" % (self.service,),
            email=self.email,
            key=self.key,
            log=self.log,
            verbose=self.verbose,
            quiet=self.quiet,
        )


class PublicECMWFServerExtended(api.ECMWFDataServer, SplitRequestMixin):
    def __init__(self, *args, dataset='', **kwargs):
        super().__init__(*args, **kwargs)
        self.c = SplitMARSRequest(
            self.url,
            "datasets/%s" % (dataset,),
            email=self.email,
            key=self.key,
            log=self.log,
            verbose=self.verbose,
        )


class MarsClient(Client):
    """A client to access data from the Meteorological Archival and Retrieval System (MARS).

    See https://www.ecmwf.int/en/forecasts/datasets for a summary of datasets available
    on MARS. Most notable, MARS provides access to ECMWF's Operational Archive
    https://www.ecmwf.int/en/forecasts/dataset/operational-archive.

    The client config must contain three parameters to autheticate access to the MARS archive:
    `api_key`, `api_url`, and `api_email`. These can also be configued by setting the
    commensurate environment variables: `MARSAPI_KEY`, `MARSAPI_URL`, and `MARSAPI_EMAIL`.
    These credentials can be looked up by after registering for an ECMWF account
    (https://apps.ecmwf.int/registration/) and visitng: https://api.ecmwf.int/v1/key/.

    MARS server activity can be observed at https://apps.ecmwf.int/mars-activity/.

    Attributes:
        config: A config that contains pipeline parameters, such as API keys.
        level: Default log level for the client.
    """
    def retrieve(self, dataset: str, selection: t.Dict, output: str, manifest: Manifest) -> None:
        c = MARSECMWFServiceExtended(
            "mars",
            key=self.config.kwargs.get('api_key', os.environ.get("MARSAPI_KEY")),
            url=self.config.kwargs.get('api_url', os.environ.get("MARSAPI_URL")),
            email=self.config.kwargs.get('api_email', os.environ.get("MARSAPI_EMAIL")),
            log=self.logger.debug,
            verbose=True,
        )
        selection_ = optimize_selection_partition(selection)
        with StdoutLogger(self.logger, level=logging.DEBUG):
            manifest.set_stage(Stage.FETCH)
            precise_fetch_start_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec='seconds')
            )
            manifest.prev_stage_precise_start_time = precise_fetch_start_time
            result = c.fetch(req=selection_)
            manifest.set_stage(Stage.DOWNLOAD)
            precise_download_start_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec='seconds')
            )
            manifest.prev_stage_precise_start_time = precise_download_start_time
            c.download(result, target=output)

    @property
    def license_url(self):
        return 'https://apps.ecmwf.int/datasets/licences/general/'

    @classmethod
    def num_requests_per_key(cls, dataset: str) -> int:
        """Number of requests per key (or user) for the Mars API.

        Mars allows 2 active requests per user and 20 queued requests per user, as of Sept 27, 2021.
        To ensure we never hit a rate limit error during download, we only make use of the active
        requests.
        See: https://confluence.ecmwf.int/display/UDOC/Total+number+of+requests+a+user+can+submit+-+Web+API+FAQ

        Queued requests can _only_ be canceled manually from a web dashboard. If the
        `ERROR 101 (USER_QUEUED_LIMIT_EXCEEDED)` error occurs in the Beam pipeline, then go to
        http://apps.ecmwf.int/webmars/joblist/ and cancel queued jobs.
        """
        return 2


class ECMWFPublicClient(Client):
    """A client for ECMWF's public datasets, like TIGGE."""
    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        c = PublicECMWFServerExtended(
            url=self.config.kwargs.get('api_url', os.environ.get("MARSAPI_URL")),
            key=self.config.kwargs.get('api_key', os.environ.get("MARSAPI_KEY")),
            email=self.config.kwargs.get('api_email', os.environ.get("MARSAPI_EMAIL")),
            log=self.logger.debug,
            verbose=True,
            dataset=dataset,
        )
        selection_ = optimize_selection_partition(selection)
        with StdoutLogger(self.logger, level=logging.DEBUG):
            result = c.fetch(req=selection_)
            c.download(result, target=output)

    @classmethod
    def num_requests_per_key(cls, dataset: str) -> int:
        # Experimentally validated request limit.
        return 5

    @property
    def license_url(self):
        if not self.config.dataset:
            raise ValueError('must specify a dataset for this client!')
        return f'https://apps.ecmwf.int/datasets/data/{self.config.dataset.lower()}/licence/'


class FakeClient(Client):
    """A client that writes the selection arguments to the output file."""

    def retrieve(self, dataset: str, selection: t.Dict, output: str, manifest: Manifest) -> None:
        manifest.set_stage(Stage.RETRIEVE)
        precise_retrieve_start_time = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec='seconds')
        )
        manifest.prev_stage_precise_start_time = precise_retrieve_start_time
        self.logger.debug(f'Downloading {dataset} to {output}')
        with open(output, 'w') as f:
            json.dump({dataset: selection}, f)

    @property
    def license_url(self):
        return 'lorem ipsum'

    @classmethod
    def num_requests_per_key(cls, dataset: str) -> int:
        return 1


CLIENTS = collections.OrderedDict(
    cds=CdsClient,
    mars=MarsClient,
    ecpublic=ECMWFPublicClient,
    fake=FakeClient,
)
