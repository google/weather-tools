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

import argparse

from src.weather_lm import nl_to_weather_data
from src.xql import main
from src.xql.utils import connect_dask_cluster
from typing import List, Tuple

def parse_args() -> Tuple[argparse.Namespace, List[str]]:
    parser = argparse.ArgumentParser()

    parser.add_argument('mode', type=str, help='Select one from [xql, lm]')

    return parser.parse_known_args()


if __name__ == '__main__':
    known_args, _ = parse_args()
    if known_args.mode not in ["xql", "lm"]:
        raise RuntimeError("Invalid mode type. Select one from [xql, lm]")
    prefix = "xql" if known_args.mode == "xql" else "lm"
    try:
        # Connect Dask Cluster
        connect_dask_cluster()
        while True:
            query = input(f"{prefix}> ")
            if query == ".exit":
                break
            if known_args.mode == "xql":
                main(query)
            else:
                nl_to_weather_data(query)
    except ImportError as e:
        raise ImportError('main function is not imported please try again.') from e

