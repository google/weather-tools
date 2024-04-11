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

import json
import typing as t

from gcsfs import GCSFileSystem
from langchain.prompts import PromptTemplate
from langchain.schema.output_parser import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI

from .constant import METADATA_URI

def get_table_map() -> t.Dict:
    """
    Load and return the table map from dataset-meta.json file.

    Returns:
        dict: Dictionary containing table names as keys and their metadata as values.
    """
    fs = GCSFileSystem()
    table_map = {}
    with fs.open(METADATA_URI) as f:
        table_map = json.load(f)
    return table_map

def get_table_map_prompt() -> t.Tuple:
    """
    Generate a prompt containing information about each table in the dataset.

    Returns:
        tuple: A tuple containing the prompt string and the table map dictionary.
    """
    table_prompts = []
    table_map = get_table_map()
    for k, v in table_map.items():
        data_str = f"""Table name is {k}.
        It's located at {v['uri']} and containing following columns: {', '.join(v['columns'])}"""

        table_prompts.append(data_str)
    return "\n".join(table_prompts), table_map


def get_invocation_steps(prompt: PromptTemplate, model: ChatGoogleGenerativeAI):
    """
    Get the invocation steps for a given prompt and model.

    Parameters:
    - prompt (PromptTemplate): The prompt template to use.
    - model (ChatGoogleGenerativeAI): The generative model to use.

    Returns:
    - Pipeline: The invocation steps for the given prompt and model.
    """
    chat = (
        prompt
        | model.bind()
        | StrOutputParser()
    )
    return chat
