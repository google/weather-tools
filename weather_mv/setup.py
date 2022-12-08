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
    "numpy",
    "pandas",
    "xarray",
    "cfgrib",
    "netcdf4",
    "geojson",
    "simplejson",
    "rioxarray",
    "metview",
    "rasterio",
    "earthengine-api>=0.1.263",
    "pyproj",  # requires separate binary installation!
    "gdal",  # requires separate binary installation!
]

setup(
    name='loader_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    version='0.2.9',
    url='https://weather-tools.readthedocs.io/en/latest/weather_mv/',
    description='A tool to load weather data into BigQuery.',
    install_requires=beam_gcp_requirements + base_requirements,
)
