# weather-tools

Apache Beam pipelines to make weather data accessible and useful.

[![CI](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml)

## Introduction

This project contributes a series of command-line tools to make common data engineering tasks easier for researchers in
climate and weather. These solutions were born out of the need to improve repeated work performed by research teams
across Alphabet.

The first tool created was the weather downloader (`weather-dl`). This makes it easier to ingest data from the European
Center for Medium Range Forecasts (ECMWF). `weather-dl` enables users to describe very specifically what data they'd
like to ingest from ECMWF's catalogs. It also offers them control over how to parallelize requests, empowering users to
[retrieve data efficiently](Efficient-Requests.md). Downloads are driven from a
[configuration file](Configuration.md), which can be reviewed (and version-controlled) independently of pipeline or
analysis code.

We also provide two additional tools to aid climate and weather researchers: the weather mover (`weather-mv`) and the
weather splitter (`weather-sp`). These CLIs are still in their alpha stages of development. Yet, they have been used for
production workflows for several partner teams.

We created the weather mover (`weather-mv`) to load geospatial data from cloud buckets
into [Google BigQuery](https://cloud.google.com/bigquery). This enables rapid exploratory analysis and visualization of
weather data: From BigQuery, scientists can load arbitrary climate data fields into a Pandas or XArray dataframe via a
simple SQL query.

The weather splitter (`weather-sp`) helps normalize how archival weather data is stored in cloud buckets:
Whether you're trying to merge two datasets with overlapping variables — or, you simply need
to [open Grib data from XArray](https://github.com/ecmwf/cfgrib/issues/2), it's really useful to split datasets into
their component variables.

## Installing

It's recommended that you create a local python environment (with
[Anaconda](https://www.anaconda.com/products/individual)). Otherwise, these tools can be installed with pip:

  ```shell
  pip install google-weather-tools
  ```

From here, you can use the `weather-*` tools from your python environment. Currently, the following tools are available:

- [⛈ `weather-dl`](weather_dl/README.md) (_beta_) – Download weather data (namely, from ECMWF's API).
- [⛅️ `weather-mv`](weather_mv/README.md) (_alpha_) – Load weather data into BigQuery.
- [🌪 `weather-sp`](weather_sp/README.md) (_alpha_) – Split weather data by variable.

## Quickstart

Together, let's
download [Era 5 pressure level data](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=overview)
and ingest it into Google BigQuery.

_Pre-requisites_:

1. Acquire and install a license from
   ECMWF's [Copernicus (CDS) API](https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key).
2. Create an empty BigQuery Dataset. This can be done in
   the [console](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-cloud-console#create_a_dataset)
   or via the [`bq` CLI](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-command-line). For example:
   ```shell
   bq mk --project_id=$PROJECT $DATASET_ID
   ```

_Steps_:

1. Use `weather-dl` to acquire the Era 5 pressure level data.

   For simplicity, let's run everything on your local machine. For the downloader, this means we'll use
   the `--local-run` option:

   ```shell
   weather-dl configs/era5_example_config_local_run.cfg --local-run
   ```

   > Recommendation: Pass the `-d, --dry-run` flag to any of these commands to preview effects.

   Generally, `weather-dl` is designed to ingest weather data to cloud storage. To learn how to configure downloads,
   please see [this documentation](Configuration.md). See [detailed usage of `weather-dl` here](weather_dl/README.md).

2. (optional) Split your downloaded dataset up by variable with `weather-sp`:

   ```shell
    weather-sp --input-pattern "./local_run/era5-*.nc" --output-dir "split_data" 
   ```

   Consult the [`weather-sp` docs](weather_sp/README.md) for more.

3. Use `weather-mv` to upload this data to Google BigQuery.

   ```shell
   weather-mv --uris "./local_run/**.nc" \ # or  --uris "./split_data/**.nc" \
      --output_table "$PROJECT.$DATASET_ID.$TABLE_ID" \
      --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
      --direct_num_workers 2
   ```

   See [these docs](weather_mv/README.md) for more about this tool.

   > Warning: Dry-runs are currently not supported. See [#22](https://github.com/googlestaging/weather-tools/issues/22).

That's it! Soon, you'll have your weather data ready for analysis in BigQuery.

> Note: The exact interfaces for these CLIs are subject to change. For example, we plan to make the CLIs have more
> uniform arguments ([#21](https://github.com/googlestaging/weather-tools/issues/21)).

## Contributing

The weather tools are under active development, and contributions are welcome! Please check out
our [guide](CONTRIBUTING.md) to get started.

## License

This is not an official Google product.

```
Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
