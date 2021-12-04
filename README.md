# weather-tools

[![CI](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml/badge.svg)](https://github.com/googlestaging/weather-tools/actions/workflows/ci.yml)

Apache Beam pipelines to make weather data accessible and useful.

<details>
<summary>
<em>Milestone 1</em>: Load a subset of <a href="https://www.ecmwf.int/en/forecasts/datasets/archive-datasets">historical ECMWF data</a> needed for <a href="https://deepmind.com/blog/article/machine-learning-can-boost-value-wind-energy">DeepMind's wind energy-related forecasting</a>.
</summary>

- ✅ Use MARs API to download ECMWF's HRES forecasts
- ✅ Download ECMWF's ENS forecasts
- ✅ Pipe downloaded data into BigQuery for general use

</details>

## Developer Setup

Please follow the [contributing guidelines](CONTRIBUTING.md) rather than the Installation steps below.

## Installing

It's recommended that you create a local python environment (with
[Anaconda](https://www.anaconda.com/products/individual)). Otherwise, these tools can be installed with pip:

  ```shell
  pip install git+http://github.com/google/weather-tools.git#egg=weather-tools
  ```

> Note: Publishing on PyPi will come soon, [#1](https://github.com/googlestaging/weather-tools/issues/1).

From here, you can use the `weather-*` tools from your python environment. Currently, the following tools are available:

- [`weather-dl`](weather_dl/README.md) – Download weather data (namely, from ECMWF's API).
- [`weather-mv`](weather_mv/README.md) – Load weather data into BigQuery.
- [`weather-sp`](weather_sp/README.md) – Split weather data by variable.

## Choosing a Beam Runner

All tools use Apache Beam pipelines. By default, pipelines run locally using the `DirectRunner`. You can optionally
choose to run the pipelines on Google Cloud Dataflow by selection the `DataflowRunner`.

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

