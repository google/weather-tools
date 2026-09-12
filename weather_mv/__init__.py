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

import logging
import sys
import argparse
import typing as t

from .loader_pipeline.pipeline import run as wmv_run
from .loader_pipeline.pipeline import pipeline as wmv_pipeline


class Pipeline():
    """A class for custom pipelines to extend."""
    def __init__(self, setup_file_path: t.Optional[str] = None):
        self.extra_args = []
        if setup_file_path:
            self.extra_args.extend(f'--setup_file {setup_file_path}'.split())

    def add_args(self):
        """Adds extra arguments to self.extra_args."""
        pass

    def pipeline(self, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        """Runs the pipeline."""
        wmv_pipeline(known_args, pipeline_args)

    def extract_args(self) -> t.Tuple[argparse.Namespace, t.List[str]]:
        """Extracts arguments."""
        all_args = sys.argv + self.extra_args
        return wmv_run(all_args)

    def run(self):
        """Extracts kw arguments."""
        logging.getLogger().setLevel(logging.INFO)
        self.pipeline(*self.extract_args())
