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
    "pygrib==2.1.8",
    "eccodes",
    "numpy>=1.20.3",
    "xarray==2025.9.0",  # Capped by cfgrib==0.9.15.1.
    "scipy==1.18.1",
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
