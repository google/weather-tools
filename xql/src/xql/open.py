#!/usr/bin/env python3
# Copyright 2024 Google LLC
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

import ee
import logging
import xarray as xr

logger = logging.getLogger(__name__)

OPENER_MAP = {
    "zarr": "zarr",
    "ee": "ee"
}

def open_dataset(uri: str) -> xr.Dataset:
    """
    Open a dataset from the given URI using the appropriate engine.

    Parameters:
    - uri (str): The URI of the dataset to open.

    Returns:
    - xr.Dataset: The opened dataset.

    Raises:
    - RuntimeError: If unable to open the dataset.
    """

    try:
        # Check if the URI starts with "ee://"
        if uri.startswith("ee://"):
            # If yes, initialize Earth Engine
            ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
            # Open dataset using Earth Engine engine
            ds = xr.open_dataset(uri, engine=OPENER_MAP["ee"])
        else:
            # If not, open dataset using zarr engine
            ds = xr.open_dataset(uri, engine=OPENER_MAP["zarr"], chunks="auto")
    except Exception:
        # If opening fails, raise RuntimeError
        raise RuntimeError("Unable to open dataset. [zarr, ee] are the only supported dataset types.")

    return ds
