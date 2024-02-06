# Copyright 2024 Google LLC
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

requirements = ["fsspec", "gcsfs", "numpy", "sqlglot", "xarray", "xee", "zarr"]

setup(
    name="xql",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    install_requires=requirements,
    version="0.0.1",
    author='anthromet',
    author_email='anthromets-ecmwf@google.com',
    description=("Running SQL queries on Xarray Datasets. Consider dataset as a table and data variable as a column."),
    long_description=open('README.md', 'r', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    python_requires='>=3.8, <3.11',
)
