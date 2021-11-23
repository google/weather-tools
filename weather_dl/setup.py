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

base_requirements = [
    "cdsapi",
    "ecmwf-api-client",
    "apache-beam[gcp]",
    "numpy>=1.19.1",
    "pandas",
    "xarray",
    "requests>=2.24.0",
    "firebase-admin>=5.0.0",
    "google-cloud-datastore>=1.15.0,<2",  # For compatability with apache-beam[gcp]
    "google-cloud-firestore",
    "urllib3==1.25.11",
    "pyparsing==2.4.2",  # Fix http2lib auth breakage
]

setup(
    name='download_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf',
    description='A tool to download weather data.',
    install_requires=base_requirements,
)
