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

weather_dl_requirements = [
    "cdsapi",
    "ecmwf-api-client",
    "numpy>=1.19.1",
    "pandas",
    "xarray",
    "requests>=2.24.0",
    "firebase-admin>=5.0.0",
    "google-cloud-firestore",
    "urllib3==1.26.5",
]

weather_mv_requirements = [
    "dataclasses",
    "numpy",
    "pandas",
    "xarray",
    "cfgrib",
    "netcdf4",
    "geojson",
    "more-itertools",
    "simplejson",
    "rioxarray",
    "rasterio",
    "earthengine-api>=0.1.263",
    "pyproj",  # requires separate binary installation!
    "gdal",  # requires separate binary installation!
]

weather_sp_requirements = [
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
    "metview",
]

all_test_requirements = beam_gcp_requirements + weather_dl_requirements + \
                        weather_mv_requirements + weather_sp_requirements + \
                        test_requirements

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
        'Programming Language :: Python :: 3.9',
        'Topic :: Scientific/Engineering :: Atmospheric Science',

    ],
    python_requires='>=3.7, <3.10',
    install_requires=['apache-beam[gcp]==2.40.0'],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    scripts=['weather_dl/weather-dl', 'weather_dl/download-status',
             'weather_mv/weather-mv', 'weather_sp/weather-sp'],
    tests_require=test_requirements,
    extras_require={
        'dev': ['tox', 'sphinx>=2.1', 'myst-parser'] + all_test_requirements,
        'test': all_test_requirements,
        'regrid': ['metview']
    },
    project_urls={
        'Issue Tracking': 'http://github.com/google/weather-tools/issues',
    },
)
