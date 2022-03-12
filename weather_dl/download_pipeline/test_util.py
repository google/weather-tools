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
import pickle
import tempfile

import apache_beam as beam


def _write_pickle(value, path):
    with open(path, 'wb') as f:
        pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)


# Source: https://github.com/google/xarray-beam/blob/main/xarray_beam/_src/test_util.py
class EagerPipeline:
    """A mock Beam pipeline for testing that returns lists of Python objects.
    Example usage:
      >>> EagerPipeline() | beam.Create([1, 2, 3]) | beam.Map(lambda x: x**2)
      [1, 4, 9]
    """

    def __or__(self, ptransform):
        with tempfile.NamedTemporaryFile() as f:
            with beam.Pipeline('DirectRunner') as pipeline:
                (
                        pipeline
                        | ptransform
                        | beam.combiners.ToList()
                        | beam.Map(_write_pickle, f.name)
                )
            pipeline.run()
            return pickle.load(f)
