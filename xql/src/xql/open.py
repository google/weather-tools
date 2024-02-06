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
import xarray as xr

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
    # Flag to track if zarr opening failed
    invalid_zarr = False

    # Attempt to open dataset with zarr engine
    try:
        ds = xr.open_dataset(uri, engine=OPENER_MAP["zarr"])
    except Exception:
        # Set flag to True if zarr opening failed
        invalid_zarr = True

    # If zarr opening failed and URI starts with "ee://", try opening with "ee" engine
    if invalid_zarr and uri.startswith("ee://"):
        ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')
        ds = xr.open_dataset(uri, engine=OPENER_MAP["ee"])
    else:
        # If both attempts fail, raise RuntimeError
        raise RuntimeError("Unable to open dataset.")

    return ds
