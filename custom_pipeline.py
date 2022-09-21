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

import os

import weather_mv


class MyCustomPipeline(weather_mv.Pipeline):
    """A custom pipeline extending weather mover pipeline."""
    def add_args(self):
        """Adds extra arguments to the pipeline."""
        self.extra_args.extend('--runner DataflowRunner \
                                --project grid-intelligence-sandbox \
                                --region us-central1 \
                                --job_name test-wt-as-library-2 \
                                --temp_location gs://deep-tmp/ \
                                --sdk_container_image gcr.io/grid-intelligence-sandbox/miniconda3-beam:testfinal-fix1'
                               .split())


if __name__ == '__main__':
    p = MyCustomPipeline(f'{os.getcwd()}/weather_mv/setup.py')
    p.add_args()
    p.run()
