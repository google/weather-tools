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
from src.xql import main
from src.xql.utils import connect_dask_cluster

if __name__ == '__main__':
    try:
        # Connect Dask Cluster
        connect_dask_cluster()
        while True:
            query = input("xql> ")
            if query == ".exit":
                break
            main(query)
    except ImportError as e:
        raise ImportError('main function is not imported please try again.') from e

