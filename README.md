# weather-tools

Apache Beam pipelines to make weather data accessible and useful.

[![CI](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/weather-tools/badge/?version=latest)](https://weather-tools.readthedocs.io/en/latest/?badge=latest)

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

It is currently recommended that you create a local python environment (with
[Anaconda](https://www.anaconda.com/products/individual)) and install the
sources as follows:

  ```shell
conda env create --name weather-tools --file=environment.yml
conda activate weather-tools
  ```

> Note: Due to its use of 3rd-party binary dependencies such as GDAL and MetView, `weather-tools`
> is transitioning from PyPi to Conda for its main release channel. The instructions above
> are a temporary workaround before our Conda-forge release.

From here, you can use the `weather-*` tools from your python environment. Currently, the following tools are available:

- [⛈ `weather-dl`](weather_dl/README.md) (_beta_) – Download weather data (namely, from ECMWF's API).
- [⛅️ `weather-mv`](weather_mv/README.md) (_alpha_) – Load weather data into analytics engines, like BigQuery.
- [🌪 `weather-sp`](weather_sp/README.md) (_alpha_) – Split weather data by arbitrary dimensions.

## Quickstart

In this tutorial, we will
download the [Era 5 pressure level dataset](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=overview)
and ingest it into Google BigQuery using `weather-dl` and `weather-mv`, respectively.

### Prerequisites

1. [Register](https://cds.climate.copernicus.eu/user/register) for a license from
   ECMWF's [Copernicus (CDS) API](https://cds.climate.copernicus.eu/api-how-to).
2. Install your license by copying your API url & key from [this page](https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key) to a new file `$HOME/.cdsapirc`.[^1] The file should look like this:
   ```
   url: https://cds.climate.copernicus.eu/api/v2
   key: <YOUR_USER_ID>:<YOUR_API_KEY>
   ```
3. If you do not already have a Google Cloud project, create one by following
   [these steps](https://cloud.google.com/docs/get-started). If you are working on
   an existing project, make sure your user has the [BigQuery Admin role](https://cloud.google.com/bigquery/docs/access-control#bigquery.admin).
   To learn more about granting IAM roles to users in Google Cloud, visit the
   [official docs](https://cloud.google.com/iam/docs/granting-changing-revoking-access#grant-single-role).
4. Create an empty BigQuery Dataset. This can be done using
   the [Google Cloud Console](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-cloud-console#create_a_dataset)
   or via the [`bq` CLI tool](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-command-line). 
   For example:
   ```shell
   bq mk --project_id=$PROJECT_ID $DATASET_ID
   ```
6. Follow [these steps](https://cloud.google.com/storage/docs/creating-buckets) 
   to create a bucket for staging temporary files in [Google Cloud Storage](https://cloud.google.com/storage).

### Steps

For the purpose of this tutorial, we will use your local machine to run the
data pipelines. Note that all `weather-tools` can also be run in [Cloud Dataflow](https://cloud.google.com/dataflow)
which is easier to scale and fully managed.

1. Use `weather-dl` to download the *Era 5 pressure level* dataset.
   ```bash
   weather-dl configs/era5_example_config_local_run.cfg \
      --local-run # Use the local machine
   ```

   > Recommendation: Pass the `-d, --dry-run` flag to any of these commands to preview the effects.

   **NOTE:** By default, local downloads are saved to the `./local_run` directory unless another file system is specified.
   The recommended output location for `weather-dl` is [Cloud Storage](https://cloud.google.com/storage).
   The source and destination of the download are configured using the `.cfg` configuration file which is passed to the command.
   To learn more about this configuration file's format and features,
   see [this reference](Configuration.md). To learn more about the `weather-dl` command, visit [here](weather_dl/README.md).

2. *(optional)* Split your downloaded dataset up with `weather-sp`:

   ```shell
    weather-sp --input-pattern "./local_run/era5-*.nc" \
       --output-dir "split_data" 
   ```

   Visit the `weather-sp` [docs](weather_sp/README.md) for more information.

3. Use `weather-mv` to ingest the downloaded data into BigQuery, in a structured format.

   ```bash
   weather-mv bigquery --uris "./local_run/**.nc" \ # or "./split_data/**.nc" if weather-sp is used
      --output_table "$PROJECT.$DATASET_ID.$TABLE_ID" \ # The path to the destination BigQuery table
      --temp_location "gs://$BUCKET/tmp" \  # Needed for stage temporary files before writing to BigQuery
      --direct_num_workers 2
   ```

   See [these docs](weather_mv/README.md) for more about the `weather-mv` command.

That's it! After the pipeline is completed, you should be able to query the ingested 
dataset in [BigQuery SQL workspace](https://cloud.google.com/bigquery/docs/bigquery-web-ui)
and analyze it using [BigQuery ML](https://cloud.google.com/bigquery-ml/docs/introduction).

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

[^1]: Note that you need to be logged in for the [CDS API page](https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key) to actually show your user ID and API key. Otherwise, it will display a placeholder, which is confusing to some users.
