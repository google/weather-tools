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
"""Setup weather-mv.

This setup.py script makes use of Apache Beam's recommended way to install non-python dependencies to worker images.
This is employed to enable a portable installation of cfgrib, which requires ecCodes.

Please see this documentation and example code:
- https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/#nonpython
- https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/complete/juliaset/setup.py
"""

from setuptools import setup, find_packages, Command
import subprocess
from distutils.command.build import build as _build  # type: ignore

base_requirements = [
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


# This class handles the pip install mechanism.
class build(_build):  # pylint: disable=invalid-name
    """A build command class that will be invoked during package install.
    The package built using the current setup.py will be staged and later
    installed in the worker using `pip install package'. This class will be
    instantiated during install for this specific scenario and will trigger
    running the custom commands specified.
    """
    sub_commands = _build.sub_commands + [('CustomCommands', None)]


# Some custom command to run during setup. The command is not essential for this
# workflow. It is used here as an example. Each command will spawn a child
# process. Typically, these commands will include steps to install non-Python
# packages. For instance, to install a C++-based library libjpeg62 the following
# two commands will have to be added:
#
#     ['apt-get', 'update'],
#     ['apt-get', '--assume-yes', 'install', 'libjpeg62'],
#
# First, note that there is no need to use the sudo command because the setup
# script runs with appropriate access.
# Second, if apt-get tool is used then the first command needs to be 'apt-get
# update' so the tool refreshes itself and initializes links to download
# repositories.  Without this initial step the other apt-get install commands
# will fail with package not found errors. Note also --assume-yes option which
# shortcuts the interactive confirmation.
#
# Note that in this example custom commands will run after installing required
# packages. If you have a PyPI package that depends on one of the custom
# commands, move installation of the dependent package to the list of custom
# commands, e.g.:
#
#     ['pip', 'install', 'my_package'],
#
# TODO(BEAM-3237): Output from the custom commands are missing from the logs.
# The output of custom commands (including failures) will be logged in the
# worker-startup log.
"""Install the ecCodes binary from ECMWF."""
CUSTOM_COMMANDS = [
    cmd.split() for cmd in [
        'apt-get update',
        'apt-get --assume-yes install libeccodes-dev'
    ]
]


class CustomCommands(Command):
    """A setuptools Command class able to run arbitrary commands."""

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def RunCustomCommand(self, command_list):
        print('Running command: %s' % command_list)
        p = subprocess.Popen(
            command_list,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        # Can use communicate(input='y\n'.encode()) if the command run requires
        # some confirmation.
        stdout_data, _ = p.communicate()
        print('Command output: %s' % stdout_data)
        if p.returncode != 0:
            raise RuntimeError(
                'Command %s failed: exit code: %s' % (command_list, p.returncode))

    def run(self):
        for command in CUSTOM_COMMANDS:
            self.RunCustomCommand(command)


setup(
    name='loader_pipeline',
    packages=find_packages(),
    author='Anthromets',
    author_email='anthromets-ecmwf@google.com',
    version='0.1.8',
    url='https://weather-tools.readthedocs.io/en/latest/weather_mv/',
    description='A tool to load weather data into BigQuery.',
    install_requires=base_requirements,
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    }
)
