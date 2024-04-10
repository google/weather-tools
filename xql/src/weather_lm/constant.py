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

# ruff: noqa: E501

METADATA_URI = "gs://darshan-store/xql/metadata.json"

GENERATE_SQL_TEMPLATE = """You are a SQL expert. Given an input question, first create a syntactically correct SQL query to execute.
Never query for all columns from a table. You must query only the columns that are needed to answer the question.
Pay attention to use only the column names you can see in the tables below.
Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Everytime wrap table name in single quotes ('').
Specify the time range, latitude, longitude as follows: (time >= '2012-12-01' AND time < '2013-04-01').

While accessing the variable name from the table don't use "\" this.
Ex. ( SELECT MAX("vertical_velocity") FROM 'table' ) => True syntax
    ( SELECT MAX(\"vertical_velocity\") FROM 'table' ) => False syntax
Avoid using the 'time BETWEEN', 'latitude BETWEEN' syntax, opt for the former style instead.

Note: At present, only data variables are supported in the SELECT Clause.  Coordinates (latitude, longitude, time) are not supported.  Therefore,
coordinates should not be used in the SELECT Clause.

Example:

Some important data details to consider:
- Use latitude and longitude ranges for cities and countries.
- Standard aggregations are applied to the data. A unique convention for aggregation for daily, monthly and yearly are time_date, time_month and time_year.
- The WHERE clause and GROUP BY is specifically applies to coordinates variables. e.g. timestamp, latitude, longitude, and level coordinates.
    For "timestamp," use time_date for grouping by date and time_month for grouping by month. Standard SQL GROUP BY operations apply only to "latitude",
    "longitude", and "level" column.
- Write time always into 'YYYY-MM-DD' format. i.e. '2021-12-01'.

Please use the following format:

Question: "Question here"
SQLQuery: "SQL Query to run"

Use the following information for the database:
- Use {table} as table name.
- The dataset includes columns like {columns}. Select appropriate columns from these which are most relevant to the Question.
- Latitude range is {latitude_range}, and longitude range is {longitude_range}. Generate query accordingly.
- {latitude_dim} and {longitude_dim} are my columns for latitude and longitude so use them everywhere in query.
    Ex. If lat and lon are in the {dims} then instead of latitude > x AND longitude > y use lat > x AND lon > y.
- The interpretation of the "organic" soil type is value of soil type is equal to 6.
- "Over all locations", "globally" entails iterating through "latitude" & "longitude."

Some examples of SQL queries that correspond to questions are:

{few_shot_examples}

Question: {question}"""

few_shots = {
    "Aggregate precipitation over months over all locations?" : "SELECT SUM(precipitation) FROM 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3' GROUP BY latitude, longitude, time_month",
    "Daily average temperature.":"""SELECT AVG(temperature) FROM "gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3" GROUP BY time_date""",
    "Average temperature of the Antarctic Area during last monsoon over months.":"SELECT AVG(temperature) FROM 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3' WHERE time >= '2022-06-01' AND time < '2022-11-01' AND latitude < 66.5 GROUP BY time_month",
    "Average temperature over years.":"SELECT AVG(temperature) FROM 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3' GROUP BY time_year",
    "Aggregate precipitation globally?" : "SELECT SUM(precipitation) FROM 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3' GROUP BY latitude, longitude",
    "For January 2000" : "SELECT * from TABLE where time >= '2000-01-01 00:00:00' AND time < '2000-02-01 00:00:00' ",
    "Daily average temperature of city x for January 2015?": "SELECT AVG(temperature) FROM 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3' WHERE time >= '2015-01-01' AND time < '2015-02-01' AND latitude > 40 AND latitude < 41 AND longitude > 286 AND longitude < 287 GROUP BY time_date",
    "Daily min reflectivity of city x for January 2015?": "SELECT AVG(reflectivity) FROM 'ee://projects/anthromet-prod/assets/opera/instantaneous_maximum_reflectivity' WHERE time >= '2015-01-01' AND time < '2015-02-01' AND lat > 40.48 AND lat < 41.87 AND lon > -74.25 AND lon < -71.98 GROUP BY time_date"
}

SELECT_DATASET_TEMPLATE = """
I have some description of tables that stores weather related data.
Analyze and give me an table that i need to query for provided question.
Sometimes the exact column not be there in the table so select table that contains most relevant columns.
    Ex. Daily average of precipitation rate asked but exact precipitation column is not there then select the table that contains relevant column like total_precipitation, precipitation_rate, total_precipitation_rate, etc.
Below is the description and input qustion
{table_map}

Question: {question}

Please use the following format:

{question}:appropriate table
"""
