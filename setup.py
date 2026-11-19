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

weather_dl_requirements = [
    "cdsapi==0.7.7",
    "ecmwf-api-client",
    "numpy>=1.19.1",
    "pandas",
    "xarray",
    "requests>=2.24.0",
    "google-cloud-firestore",
    "firebase-admin==7.7.0",
    "urllib3==2.8.0",
]

weather_mv_requirements = [
    "dataclasses",
    "numpy",
    "pandas",
    "xarray",
    "cfgrib",
    "netcdf4",
    "geojson",
    "simplejson",
    "rioxarray",
    "rasterio",
    "earthengine-api>=0.1.263,<1.7.36",  # Capped by apache-beam[gcp] (google-auth-httplib2<0.3.0).
    "pyproj",  # requires separate binary installation!
    "gdal",  # requires separate binary installation!
    "xarray-beam==0.11.5",
    "gcsfs==2026.10.1",
    "zarr==3.4.2",
]

weather_sp_requirements = [
    "numpy>=1.20.3",
    "pygrib",
    "xarray",
    "scipy",
]

test_requirements = [
    "pytype==2024.10.11",
    "ruff==0.17.4",
    "pytest",
    "pytest-subtests",
    "netcdf4",
    "numpy",
    "xarray",
    "xarray-beam",
    "absl-py",
    "metview",
    "memray",
    "pytest-memray",
    "h5py",
    "pooch",
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
        'Programming Language :: Python :: 3.12',
        'Topic :: Scientific/Engineering :: Atmospheric Science',

    ],
    python_requires='>=3.12',
    install_requires=['apache-beam[gcp]==2.77.0', 'apache-beam[dill]==2.77.0', 'gcsfs==2026.10.1'],
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    scripts=['weather_dl/weather-dl', 'weather_mv/weather-mv', 'weather_sp/weather-sp'],
    tests_require=test_requirements,
    extras_require={
        'docs': ['tox', 'sphinx>=2.1', 'myst-parser', 'Jinja2<3.2'],
        'test': all_test_requirements,
        'dev': ['google-weather-tools[docs,test]'],
        'regrid': ['metview']
    },
    project_urls={
        'Issue Tracking': 'http://github.com/google/weather-tools/issues',
    },
)
