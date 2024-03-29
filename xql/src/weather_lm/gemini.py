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

import os

from langchain_google_genai import ChatGoogleGenerativeAI

from .constant import few_shots
from .template import DEFINED_PROMPTS
from .utils import get_invocation_steps, get_table_map_prompt
from xql import run_query

def nl_to_sql_query(input_statement: str) -> str:
    """
    Convert a natural language query to SQL.

    Parameters:
    - input_statement (str): The natural language query.

    Returns:
    - str: The generated SQL query.
    """
    # Check if API key is provided either directly or through environment variable
    api_key = os.getenv("GOOGLE_API_KEY")

    if api_key is None:
        raise RuntimeError("Environment variable GOOGLE_API_KEY is not set.")

    # Get table map
    table_map_prompt, table_map = get_table_map_prompt()

    # Initialize model for natural language processing
    model = ChatGoogleGenerativeAI(model="gemini-pro")

    # Get invocation steps for selecting dataset
    select_dataset_model = get_invocation_steps(DEFINED_PROMPTS['select_dataset'], model)

    # Get invocation steps for generating SQL
    generate_sql_model = get_invocation_steps(DEFINED_PROMPTS['generate_sql'], model)

    # Invoke pipeline to select dataset based on input statement
    select_dataset_res = select_dataset_model.invoke({ "question": input_statement, "table_map": table_map_prompt })

    # Extract dataset key from result
    dataset_key = select_dataset_res.split(":")[-1].strip()

    # Retrieve dataset metadata using dataset key
    dataset_metadata = table_map[dataset_key]

    # Invoke pipeline to generate SQL query
    generate_sql_res = generate_sql_model.invoke({
        "question": input_statement,
        "table": dataset_metadata['uri'],
        "columns": dataset_metadata['columns'],
        "few_shot_examples": few_shots
    })

    # Extract SQL query from result.
    # The response will look like [SQLQuery: SELECT * FROM {table} WHERE ...].
    # So slice the sql query from string.
    sql_query = generate_sql_res[11:-1]

    # Print generated SQL statement for debugging
    print("Generated SQL Statement:", sql_query)

    return sql_query


def nl_to_weather_data(input_statement: str):
    """
    Convert a natural language query to SQL and fetch weather data.

    Parameters:
    - input_statement (str): The natural language query.
    """
    # Generate SQL query
    sql_query = nl_to_sql_query(input_statement)

    # Execute SQL query to fetch weather data
    print(run_query(sql_query))
