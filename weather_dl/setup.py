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
    "cdsapi=0.5.1",
    "ecmwf-api-client=1.6.3",
    "numpy>=1.19.1",
    "pandas=1.5.1",
    "xarray=2022.11.0",
    "requests>=2.24.0",
    "urllib3==1.26.5",
    "google-cloud-firestore==2.6.0",
    # "gcloud" should already be installed in the host image.
    # If we install it here, we'll hit auth issues.
]

setup(
    name='download_pipeline',
    packages=find_packages(),
    version='0.1.11',
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://weather-tools.readthedocs.io/en/latest/weather_dl/',
    description='A tool to download weather data.',
    install_requires=beam_gcp_requirements + base_requirements,
)
