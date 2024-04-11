#!/usr/bin/env python3
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

from langchain.prompts import PromptTemplate

from .constant import GENERATE_SQL_TEMPLATE, SELECT_DATASET_TEMPLATE

DEFINED_PROMPTS = {
    'select_dataset': PromptTemplate(
        input_variables = ["table_map", "question"],
        template = SELECT_DATASET_TEMPLATE,
    ),
    'generate_sql': PromptTemplate(
        input_variables = [
            "question",
            "few_shot_examples",
            "table",
            "columns",
            "dims",
            "latitude_dim",
            "latitude_range",
            "longitude_dim",
            "longitude_range"
        ],
        template = GENERATE_SQL_TEMPLATE,
    )
}
