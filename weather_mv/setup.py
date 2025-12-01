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
    "google-cloud-bigquery==2.34.4",
    "google-cloud-bigquery-storage==2.14.1",
    "google-cloud-bigtable==1.7.2",
    "google-cloud-core==1.7.3",
    "google-cloud-datastore==1.15.5",
    "google-cloud-dlp==3.8.0",
    "google-cloud-language==1.3.2",
    "google-cloud-pubsub==2.13.4",
    "google-cloud-pubsublite==1.4.2",
    "google-cloud-recommendations-ai==0.2.0",
    "google-cloud-spanner==1.19.3",
    "google-cloud-videointelligence==1.16.3",
    "google-cloud-vision==1.0.2",
    "apache-beam[gcp]==2.40.0",
]

base_requirements = [
    "dataclasses",
    "numpy==1.22.4",
    "pandas==1.5.1",
    "xarray==2023.1.0",
    "xarray-beam==0.6.2",
    "cfgrib==0.9.10.2",
    "netcdf4==1.6.1",
    "geojson==2.5.0",
    "simplejson==3.17.6",
    "rioxarray==0.13.4",
    "metview==1.13.1",
    "rasterio==1.3.1",
    "earthengine-api>=0.1.263",
    "pyproj==3.4.0",  # requires separate binary installation!
    "gdal==3.5.1",  # requires separate binary installation!
    "gcsfs==2022.11.0",
    "zarr==2.15.0",
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
