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
    "google-cloud-bigquery==3.47.0",
    "google-cloud-bigquery-storage==2.43.0",
    "google-cloud-bigtable==2.50.2",
    "google-cloud-core==2.8.0",
    "google-cloud-datastore==2.27.1",
    "google-cloud-dlp==3.40.0",
    "google-cloud-language==2.22.0",
    "google-cloud-pubsub==2.44.0",
    "google-cloud-pubsublite==1.13.0",
    "google-cloud-recommendations-ai==0.10.18",  # Capped by apache-beam[gcp] (<0.11.0).
    "google-cloud-spanner==3.72.0",
    "google-cloud-videointelligence==2.21.0",
    "google-cloud-vision==3.16.0",
    "apache-beam[gcp]==2.77.0",
]

base_requirements = [
    "dataclasses",
    "numpy==2.4.6",  # Capped by apache-beam==2.77.0 (<2.5.0).
    "pandas==2.3.3",  # Latest pandas 2.x compatible with xarray==2025.9.0.
    "xarray==2025.9.0",  # Capped by cfgrib==0.9.15.1.
    "xarray-beam==0.11.5",
    "cfgrib==0.9.15.1",
    "netcdf4==1.7.5",
    "geojson==3.3.0",
    "simplejson==4.3.0",
    "rioxarray==0.21.0",  # Capped by xarray==2025.9.0 (rioxarray>=0.22.0 requires xarray>=2026.2).
    "metview==1.16.3",
    "rasterio==1.5.2",
    "earthengine-api>=0.1.263,<1.7.36",  # Capped by apache-beam[gcp] (google-auth-httplib2<0.3.0).
    "pyproj==3.8.0",  # requires separate binary installation!
    "gdal==3.13.3",  # requires separate binary installation!
    "gcsfs==2026.10.1",
    "zarr==3.4.2",
]

setup(
    name='loader_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    version='0.3.0',
    url='https://weather-tools.readthedocs.io/en/latest/weather_mv/',
    description='A tool to load weather data into BigQuery.',
    install_requires=beam_gcp_requirements + base_requirements,
)
