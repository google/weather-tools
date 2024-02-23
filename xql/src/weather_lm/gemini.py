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

from langchain.prompts import PromptTemplate
from langchain.schema.output_parser import StrOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI

from .template import few_shots, TEMPLATE
from xql import run_query

def nl_to_weather_data(input_statement: str, api_key: str = None):
    """
    Convert a natural language query to SQL and fetch weather data.

    Parameters:
    - input_statement (str): The natural language query.

    Returns:
    - str: The weather data fetched from the SQL query.
    """

    api_key_env = os.getenv("GOOGLE_API_KEY")

    if api_key is None and api_key_env is None:
        raise RuntimeError("Environment variable GOOGLE_API_KEY is not set.")

    prompt = PromptTemplate(
        input_variables=["input", "few_shot_examples"],
        template=TEMPLATE,
    )

    model = ChatGoogleGenerativeAI(model="gemini-pro")

    inputs = {
        "input": lambda x: x["question"],
        "few_shot_examples": lambda x: few_shots,
    }

    chat = (
        inputs
        | prompt
        | model.bind()
        | StrOutputParser()
    )

    result = chat.invoke({"question": input_statement})
    sql_query = result[11:-1]
    print("SQL Statement : ", sql_query)
    run_query(sql_query)
