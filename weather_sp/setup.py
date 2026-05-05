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
    "pygrib==2.1.8",
    "eccodes",
    "numpy>=1.20.3",
    "xarray==2025.12.0",
    "scipy==1.16.3",
]

setup(
    name='splitter_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    version='0.4.0',
    url='https://weather-tools.readthedocs.io/en/latest/weather_sp/',
    description='A tool to split weather data files into per-variable files.',
    install_requires=beam_gcp_requirements + base_requirements,
)
