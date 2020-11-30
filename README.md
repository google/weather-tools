
![Build Status](https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf/badges/master/pipeline.svg)

**Goal**: Create a pipeline to make [ECMWF](https://www.ecmwf.int/) data available to all of Alphabet.

_Milestone 1_: Load a subset of [historical ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/archive-datasets) needed for [DeepMind's wind energy-related forecasting](https://deepmind.com/blog/article/machine-learning-can-boost-value-wind-energy).
- [ ] Use MARs API to download ECMWF's HRES forecasts
- [ ] Download ECMWF's ENS forecasts
- [ ] Pipe downloaded data into BigQuery for general use

## Developer Setup

Please follow the [contributing guidelines](CONTRIBUTING.md) rather than the Installation steps below.

## Installing

1). Create a personal_access_token with `read_api` scope ([docs](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html)).

2). Run the following command (substituting your <personal_access_token>):

```
pip install ecmwf-pipeline --no-deps --index-url https://__token__:<personal_access_token>@gitlab.com/api/v4/projects/20919443/packages/pypi/simple
```

## Weather Downloader (`weather-dl`)

Weather Downloader downloads netcdf files from ECMWF to Google Cloud Storage.

```
usage: weather-dl [-h] [-c {cds,mars}] config

positional arguments:
  config                path/to/config.cfg, specific to the <client>. Accepts *.cfg and *.json files.
```
_Common options_: 
* `-c, --client`: Select the weather API client. The default is `cds` or Copernicus. `mars` or MARS is also supported.

Invoke with `-h` or `--help` to see the full range of options.

For further information on how to write config files, please consult [this documentation](Configuration.md).

## Weather Mover (`weather-mv`)

Weather Mover creates Google Cloud BigQuery tables from netcdf files in Google Cloud Storage.

```
usage: weather-mv [-h] -i URIS -o OUTPUT_TABLE [--import_time IMPORT_TIME]

```

_Required Options_:
* `-i, --uris`: URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-""
* `-o, --output-table`: Full name of destination BigQuery table. Ex: my_project.my_dataset.my_table

Invoke with `-h` or `--help` to see the full range of options.

## Using Dataflow

All tools use Apache Beam pipelines. By default, pipelines run locally using the `DirectRunner`. You can optionally choose to run the pipelines on Google Cloud Dataflow.

_Dataflow options_: 
* `--runner`: The `PipelineRunner` to use. This field can be either `DirectRunner` or `DataflowRunner`. Default: `DirectRunner` (local mode)
* `--project`: The project ID for your Google Cloud Project. This is required if you want to run your pipeline using the Dataflow managed service (i.e. `DataflowRunner`).
* `--temp_location`: Cloud Storage path for temporary files. Must be a valid Cloud Storage URL, beginning with `gs://`.
* `--region`: Specifies a regional endpoint for deploying your Dataflow jobs. Default: `us-central1`.
* `--job_name`: The name of the Dataflow job being executed as it appears in Dataflow's jobs list and job details.

Example run: 
```shell script
weather-dl seasonal_forecast_example_config.cfg \
  --runner DataflowRunner \
  --project $PROJECT \
  --region $REGION \
  --temp_location gs://$BUCKET/tmp/
```

For a full list of how to configure the dataflow pipeline, please review 
[this table](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params).

## Monitoring

When running Dataflow, you can track metrics through UI, or beta CLI commands:
```shell script
JOBID=<enter job id here>
gcloud beta dataflow metrics list $JOBID --source=user
```

You can also view how your ECMWF MARS API jobs are listed active or queued by logging in [here](https://apps.ecmwf.int/mars-activity/).

## FAQ

### Q: Where does PubSub Fit in? 
We plan to use GCP's PubSub service to process [live ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/catalogue-ecmwf-real-time-products). Ideally, we'd like to plumb near-realtime data to BigQuery
via PubSub, if possible. We may need to add a Dataflow / Beam intermediary for this -- investigating is needed.

