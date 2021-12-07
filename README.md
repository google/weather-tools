<h1 style="text-align: center">weather-tools</h1>

<p style="text-align: center">Easy-to-use Apache Beam pipelines to make weather data accessible and useful.</p>

[![CI](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml)

## Introduction

This project contributes a series of command-line tools to make common data engineering tasks easier for researchers in
climate and weather. These solutions were born out of the need to improve repeated work performed by research teams
across Alphabet.

The first tool created was the weather downloader (`weather-dl`). This makes it easier to ingest data from the European
Center for Medium Range Forecasts (ECMWF). `weather-dl` enables users to describe very specifically what data they'd
like to ingest from ECMWF's catalogs. It also offers them control over how to parallelize requests, empowering users to
[retrieve data efficiently](https://confluence.ecmwf.int/display/WEBAPI/Retrieval+efficiency). Downloads are driven from
a configuration file, which can be reviewed (and version-controlled) independently of pipeline or analysis code.

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
  pip install git+http://github.com/google/weather-tools.git#egg=weather-tools
  ```

> Note: Publishing on PyPi will come soon, [#1](https://github.com/googlestaging/weather-tools/issues/1).

From here, you can use the `weather-*` tools from your python environment. Currently, the following tools are available:

- [`weather-dl`](weather_dl/README.md) (_beta_) – Download weather data (namely, from ECMWF's API).
- [`weather-mv`](weather_mv/README.md) (_alpha_) – Load weather data into BigQuery.
- [`weather-sp`](weather_sp/README.md) (_alpha_) – Split weather data by variable.

## Quickstart

Together, let's
download [Era 5 pressure level data](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=overview)
and ingest it into Google BigQuery.

_Pre-requisites_:

1. Acquire and install a license from
   ECMWF's [Copernicus (CDS) API](https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key).
2. Create an empty BigQuery Table. This can be done in
   the [console](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-cloud-console#create_a_dataset)
   or via the [`bq` CLI](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-command-line). For example:
   ```shell
   bq mk --project_id=$PROJECT $DATASET_ID.$TABLE_ID
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
   please see [this documentation](Configuration.md).

2. (optional) Split your downloaded dataset up by variable with `weather-sp`:

   ```shell
    weather-sp --input-pattern "./local_run/era5-*.nc" --output-dir "split_data" 
   ```

3. Use `weather-mv` to upload this data to Google BigQuery.

   ```shell
   weather-mv --uris "./local_run/**.nc" \ # or  --uris "./split_data/**.nc" \
      --output_table "$PROJECT.$DATASET_ID.$TABLE_ID" \
      --temp_location gs://$BUCKET/tmp/
   ```

   > Warning: Dry-runs are currently not supported. See [#22](https://github.com/googlestaging/weather-tools/issues/22).

That's it! Soon, you'll have your weather data ready for analysis in BigQuery.

> Note: The exact interfaces for these CLIs are subject to change. For example, we plan to make the CLIs have more
> uniform arguments ([#21](https://github.com/googlestaging/weather-tools/issues/21)).

## Choosing a Beam Runner

All tools use Apache Beam pipelines. By default, pipelines run locally using the `DirectRunner`. You can optionally
choose to run the pipelines on [Google Cloud Dataflow](https://cloud.google.com/dataflow) by selection
the `DataflowRunner`.

When working with GCP, it's recommended you set the project ID up front with the command:

```shell
gcloud config set project <your-id>
```

### _Dataflow options_:

* `--runner`: The `PipelineRunner` to use. This field can be either `DirectRunner` or `DataflowRunner`.
  Default: `DirectRunner` (local mode)
* `--project`: The project ID for your Google Cloud Project. This is required if you want to run your pipeline using the
  Dataflow managed service (i.e. `DataflowRunner`).
* `--temp_location`: Cloud Storage path for temporary files. Must be a valid Cloud Storage URL, beginning with `gs://`.
* `--region`: Specifies a regional endpoint for deploying your Dataflow jobs. Default: `us-central1`.
* `--job_name`: The name of the Dataflow job being executed as it appears in Dataflow's jobs list and job details.

Example run:

```shell
weather-dl configs/seasonal_forecast_example_config.cfg \
  --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --temp_location gs://$BUCKET/tmp/
```

For a full list of how to configure the dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params).

### _Direct Runner options_:

* `--direct_num_workers`: The number of workers to use. We recommend 2 for local development.

Example run:

```shell
weather-mv -i gs://netcdf_file.nc \
  -o project.dataset_id.table_id \
  -t gs://temp_location  \
  --direct_num_workers 2
```

For a full list of how to configure the direct runner, please review
[this page](https://beam.apache.org/documentation/runners/direct/).

## Monitoring

When running Dataflow, you can track metrics through UI, or beta CLI commands:

```shell
JOBID=<enter job id here>
gcloud beta dataflow metrics list $JOBID --source=user
```

## Contributing

The weather tools are under active development, and contributions are welcome! Please check out
our [guide](CONTRIBUTING.md) to get started.
