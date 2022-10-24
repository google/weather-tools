# Copyright 2022 Google LLC
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
"""Setup Regrid

This setup script installs extra dependencies needed by regridding (namely, MetView)
on the remove Dataflow environment via setuptools Commands. We assume that the
remove runtime environment has miniconda installed (if not a Docker image with MetView).
"""
import shutil
import subprocess
from distutils.command.build import build as _build  # type: ignore

from setuptools import setup, Command

regrid_requirements = [
    "metview",
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


"""Install MetView on an Anaconda-equipped machine."""
CONDA_COMMANDS = [
    cmd.split() for cmd in [
        'conda install metview-batch -c conda-forge -y',
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
            raise print(
                'Command %s failed: exit code: %s' % (command_list, p.returncode))

    def run(self):
        if shutil.which('conda') is not None:
            for command in CONDA_COMMANDS:
                self.RunCustomCommand(command)


setup(
    name='regrid_dependencies',
    packages=[],
    install_requires=regrid_requirements,
    description='Installs extra dependencies needed for regridding at package build time.',
    cmdclass={
        # Command class instantiated and run during pip install scenarios.
        'build': build,
        'CustomCommands': CustomCommands,
    }
)
