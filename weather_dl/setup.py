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
    "ecmwf-api-client==1.6.5",
    "numpy>=1.19.1",
    "pandas==2.3.3",
    "xarray==2025.12.0",
    "requests>=2.24.0",
    "urllib3==2.6.2",
    "google-cloud-firestore==2.22.0",
    "firebase-admin==6.9.0",
    # "gcloud" should already be installed in the host image.
    # If we install it here, we'll hit auth issues.
]

setup(
    name='download_pipeline',
    packages=find_packages(),
    version='0.2.0',
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://weather-tools.readthedocs.io/en/latest/weather_dl/',
    description='A tool to download weather data.',
    install_requires=beam_gcp_requirements + base_requirements,
    dependency_links=['https://github.com/dabhicusp/cdsapi-beta-google-weather-tools.git@master#egg=cdsapi'],  # TODO([
        #474](https://github.com/google/weather-tools/issues/474)): Compatible cdsapi with weather-dl.
)
