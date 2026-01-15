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
"""Setup weather-mv.

This setup.py script makes use of Apache Beam's recommended way to install non-python dependencies to worker images.
This is employed to enable a portable installation of cfgrib, which requires ecCodes.

Please see this documentation and example code:
- https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#nonpython
- https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py
"""

from setuptools import setup, find_packages

beam_gcp_requirements = [
    "google-cloud-bigquery==3.39.0",
    "google-cloud-bigquery-storage==2.36.0",
    "google-cloud-bigtable==2.35.0",
    "google-cloud-core==2.5.0",
    "google-cloud-datastore==2.23.0",
    "google-cloud-dlp==3.33.0",
    "google-cloud-language==2.18.0",
    "google-cloud-pubsub==2.34.0",
    "google-cloud-pubsublite==1.13.0",
    "google-cloud-recommendations-ai==0.10.18",
    "google-cloud-spanner==3.61.0",
    "google-cloud-videointelligence==2.17.0",
    "google-cloud-vision==3.11.0",
    "apache-beam[gcp]==2.70.0",
]

base_requirements = [
    "dataclasses",
    "numpy==2.2.6",
    "pandas==2.3.3",
    "xarray==2025.12.0",
    "xarray-beam==0.6.3",
    "cfgrib==0.9.15.1",
    "netcdf4==1.7.2",
    "geojson==3.2.0",
    "simplejson==3.20.2",
    "rioxarray==0.19.0",
    "metview==1.16.2",
    "rasterio==1.4.3",
    "earthengine-api>=0.1.263",
    "pyproj==3.7.1",  # requires separate binary installation!
    "gdal==3.10.0",  # requires separate binary installation!
    "gcsfs==2025.12.0",
    "zarr==3.1.5",
]

setup(
    name='loader_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    version='0.2.40',
    url='https://weather-tools.readthedocs.io/en/latest/weather_mv/',
    description='A tool to load weather data into BigQuery.',
    install_requires=beam_gcp_requirements + base_requirements,
)
