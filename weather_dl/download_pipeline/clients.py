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
import numpy as np
import io
import json
import logging
import os
import typing as t
import warnings
import shutil
import time

import cdsapi
import urllib3
from ecmwfapi import ECMWFService
import eumdac

from apache_beam.io.gcp.gcsio import DEFAULT_READ_BUFFER_SIZE

from .config import Config, optimize_selection_partition

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
    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        """Download from data source."""
        pass

    @abc.abstractmethod
    def num_requests_per_key(self, dataset: str, eumetsat_control_num_requests: bool) -> int:
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

    def retrieve(self, dataset: str, selection: t.Dict, target: str) -> None:
        selection_ = optimize_selection_partition(selection)
        self.c.retrieve(dataset, selection_, target)

    @property
    def license_url(self):
        return 'https://cds.climate.copernicus.eu/api/v2/terms/static/licence-to-use-copernicus-products.pdf'

    def num_requests_per_key(self, dataset: str, eumetsat_control_num_requests: bool) -> int:
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

    def __init__(self, config: Config, level: int = logging.INFO) -> None:
        super().__init__(config, level)
        self.c = ECMWFService(
            "mars",
            key=config.kwargs.get('api_key', os.environ.get("MARSAPI_KEY")),
            url=config.kwargs.get('api_url', os.environ.get("MARSAPI_URL")),
            email=config.kwargs.get('api_email', os.environ.get("MARSAPI_EMAIL")),
            log=self.logger.debug,
            verbose=True
        )

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        selection_ = optimize_selection_partition(selection)
        with StdoutLogger(self.logger, level=logging.DEBUG):
            self.c.execute(req=selection_, target=output)

    @property
    def license_url(self):
        return 'https://apps.ecmwf.int/datasets/licences/general/'

    def num_requests_per_key(self, dataset: str, eumetsat_control_num_requests: bool) -> int:
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


class EumetsatClient(Client):
    def __init__(self, config: Config, level: int = logging.INFO) -> None:
        super().__init__(config, level)
        self.key = config.kwargs.get('api_key', os.environ.get("EUMETSATAPI_KEY"))
        self.secret = config.kwargs.get('api_secret', os.environ.get("EUMETSATAPI_SECRET"))

    def products(self, time_slices: t.List[t.Tuple[np.datetime64, np.datetime64]], dataset: str) -> t.List:
        """Lists the products that are available for that dataset in the date ranges.

        Args:
            time_slices: list of tuples of start and end time.
            dataset: dataset name as listed in eumetsat catalogue.
        Returns:
            list of products within the specified times.
        """
        datastore = eumdac.DataStore(self._token())
        collection = datastore.get_collection(dataset)
        product_list = []
        for start, end in time_slices:
            product_list.extend([product._id for product in collection.search(dtstart=start, dtend=end)])
        return product_list

    def _token(self) -> eumdac.AccessToken:
        credentials = (self.key, self.secret)
        return eumdac.AccessToken(credentials)

    def download_native(self, product: eumdac.product.Product, output: str) -> None:
        """Downloads the entry file from the product."""
        entry = f'{product}.nat'
        with product.open(entry=entry) as fsrc:
            with open(output, 'wb') as fdst:
                shutil.copyfileobj(fsrc, fdst, DEFAULT_READ_BUFFER_SIZE)

    def download_custom(self, product: eumdac.product.Product, token: eumdac.AccessToken,
                        chain_config: eumdac.tailor_models.Chain, output: str) -> None:
        """Downloads the prduct after customisation."""
        datatailor = eumdac.DataTailor(token)
        with datatailor.new_customisation(product, chain_config) as customisation:
            customisation.update_margin = 0
            timeout = 900  # customisation can be slow
            tic = time.time()
            while time.time() - tic < timeout:
                if customisation.status == 'DONE':
                    break
            else:
                raise TimeoutError(f'Customisation took longer than {timeout}s')
            self.logger.info('Customisation for product %s: output %s', product, customisation.outputs)
            with customisation.stream_output(customisation.outputs[0]) as stream:
                with open(output, 'wb') as fdst:
                    shutil.copyfileobj(stream, fdst, DEFAULT_READ_BUFFER_SIZE)

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        selection_ = optimize_selection_partition(selection)
        with StdoutLogger(self.logger, level=logging.DEBUG):
            datastore = eumdac.DataStore(self._token())
            product = datastore.get_product(product_id=selection_['product_id'][0], collection_id=dataset)
            if not selection_.get('eumetsat_format_conversion_to'):
                self.download_native(product, output)
            else:
                chain_config = eumdac.tailor_models.Chain(product=selection_['product'],
                                                          format=selection_['eumetsat_format_conversion_to'])
                self.download_custom(product, self._token(), chain_config, output)

    @property
    def license_url(self):
        return 'https://www.eumetsat.int/eumetsat-data-licensing'

    def num_requests_per_key(self, dataset: str, eumetsat_control_num_requests: bool) -> int:
        """Number of requests per key (or user) for the EUMETSAT API.

        EUMETSAT Data Tailor allows maximum 3 (active and/or queued) customisations per user, as of Jun 06, 2022.
        See: https://eumetsatspace.atlassian.net/wiki/spaces/DSDT/pages/1589379078

        Above limitation is not applicable in case of native downloads.

        If maximum number of (active and/or queued) customizations exceeds 3, then -
        EUMETSAT Data Tailor API throws "You are exceeding your maximum number 3 of queued+running customisations."
        error.

        User's personal workspace is restricted to 20 GB. To resolve workspace size exhaust error, please delete
        old customisations to make space for new ones.
        See: https://gitlab.eumetsat.int/eumetlab/data-services/eumdac_data_tailor/-/blob/master/2_Cleaning_the_Data_Tailor_workspace.ipynb # noqa: E501

        If the workspace becomes full, then -
        EUMETSAT Data Tailor API throws "the user quota (20.0 GB) is fully used" error.

        However, for both of the above error cases, EUMETSAT API (eumdac) incorrectly throws "Invalid Credentials. Make
        sure your API invocation call has a header: 'Authorization : Bearer ACCESS_TOKEN' or 'Authorization : Basic
        ACCESS_TOKEN' or 'apikey: API_KEY'” error.
        """
        if eumetsat_control_num_requests:
            return 3
        return 15


class FakeClient(Client):
    """A client that writes the selection arguments to the output file."""

    def retrieve(self, dataset: str, selection: t.Dict, output: str) -> None:
        self.logger.debug(f'Downloading {dataset} to {output}')
        with open(output, 'w') as f:
            json.dump({dataset: selection}, f)

    @property
    def license_url(self):
        return 'lorem ipsum'

    def num_requests_per_key(self, dataset: str, eumetsat_control_num_requests: bool) -> int:
        return 1


CLIENTS = collections.OrderedDict(
    cds=CdsClient,
    mars=MarsClient,
    eumetsat=EumetsatClient,
    fake=FakeClient,
)
