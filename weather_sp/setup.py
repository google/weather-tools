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
    "apache-beam[gcp]",
    "pygrib",
    "numpy>=1.20.3",
    "xarray",
    "scipy",
]

setup(
    name='splitter_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    version='0.3.0',
    url='https://weather-tools.readthedocs.io/en/latest/weather_sp/',
    description='A tool to split weather data files into per-variable files.',
    install_requires=base_requirements,
)
