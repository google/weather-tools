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

from setuptools import find_packages, setup

weather_dl_requirements = [
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

weather_mv_requirements = [
    "apache-beam[gcp]",
    "dataclasses",
    "numpy",
    "pandas",
    "xarray",
    "google-cloud-bigquery",
    "google-cloud-storage==2.2.1",
    "pyparsing==2.4.2",  # Fix http2lib auth breakage
    "cfgrib",
    "netcdf4",
    "geojson",
    "rioxarray"
]

weather_sp_requirements = [
    "apache-beam[gcp]",
    "numpy>=1.20.3",
    "pygrib",
    "xarray",
    "scipy",
]

test_requirements = [
    "pytype==2021.11.29",
    "flake8",
    "pytest",
    "pytest-subtests",
    "netcdf4",
    "numpy",
    "xarray",
    "xarray-beam",
    "absl-py",
]

all_test_requirements = weather_dl_requirements + weather_mv_requirements + weather_sp_requirements + test_requirements

setup(
    name='google-weather-tools',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    url='https://weather-tools.readthedocs.io/',
    description='Apache Beam pipelines to make weather data accessible and useful.',
    long_description=open('README.md', 'r', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    platforms=['darwin', 'linux'],
    license='License :: OSI Approved :: Apache Software License',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: MacOS :: MacOS X',
        # 'Operating System :: Microsoft :: Windows',  # TODO(#64): Fully support Windows.
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Scientific/Engineering :: Atmospheric Science',

    ],
    # Apache Beam's Python SDK only supports up to 3.8
    python_requires='>=3.7, <3.9',
    install_requires=['apache-beam[gcp]'],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    scripts=['weather_dl/weather-dl', 'weather_dl/download-status',
             'weather_mv/weather-mv', 'weather_sp/weather-sp'],
    tests_require=test_requirements,
    extras_require={
        'dev': ['tox', 'sphinx>=2.1', 'myst-parser'] + all_test_requirements,
        'test': all_test_requirements,
    },
    project_urls={
        'Issue Tracking': 'http://github.com/google/weather-tools/issues',
    },
)
