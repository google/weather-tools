[![Build Status](https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf/badges/master/pipeline.svg)](https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf/-/pipelines)
[Documentation](https://google-pso.gitlab.io/ais/grid_intelligence_ai/ecmwf/)

# ECMWF-pipeline

**Goal**: Create pipelines to make [ECMWF](https://www.ecmwf.int/) data available to all of Alphabet.

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

Create a local python environment. In that environment, clone the repo (you'll need an SSH key that has
access to our GitLab repo; this can be set up by [following these instructions](https://docs.gitlab.com/ee/ssh/)).
Then, enter the repo, and install python packages with `pip`:

```shell script
git clone git@gitlab.com:google-pso/ais/grid_intelligence_ai/ecmwf.git
cd ecmwf
pip install . 
```

Alternatively, you can clone the repo over HTTPS: 
```shell script
git clone https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf.git
cd ecmwf
pip install . 
```

From here, you can use the `weather-*` scripts in the `bin/` folder.

## Weather Downloader (`bin/weather-dl`)

Weather Downloader downloads weather data to Google Cloud Storage.

```
usage: weather-dl [-h] [-f] [-d] [-m MANIFEST_LOCATION] config

Weather Downloader downloads weather data to Google Cloud Storage.

positional arguments:
  config                path/to/config.cfg, containing client and data information. Accepts *.cfg and *.json files.
```

_Common options_: 
* `-f, --force-download`: Force redownload of partitions that were previously downloaded.
* `-d, --dry-run`: Run pipeline steps without _actually_ downloading or writing to cloud storage.
* `-m, --manifest-location MANIFEST_LOCATION`:  Location of the manifest. Either a Firestore collection URI 
    ('fs://<my-collection>?projectId=<my-project-id>'), a GCS bucket URI, or 'noop://<name>' for an in-memory location.

Invoke with `-h` or `--help` to see the full range of options.

For further information on how to write config files, please consult [this documentation](Configuration.md).

## Weather Mover (`bin/weather-mv`)

Weather Mover creates Google Cloud BigQuery tables from netcdf files in Google Cloud Storage.

```
usage: usage: weather-mv [-h] -i URIS -o OUTPUT_TABLE -t TEMP_LOCATION [--import_time IMPORT_TIME]

```

_Required Options_:
* `-i, --uris`: URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-""
* `-o, --output_table`: Full name of destination BigQuery table. Ex: my_project.my_dataset.my_table
* `-t, --temp_location`: Temp Location for staging files to import to BigQuery

Invoke with `-h` or `--help` to see the full range of options.

## Choosing a Beam Runner

All tools use Apache Beam pipelines. By default, pipelines run locally using the `DirectRunner`. You can optionally choose to run the pipelines on Google Cloud Dataflow by selection the `DataflowRunner`.

When working with GCP, it's recommended you set the project ID up front with the command:
```shell script
gcloud config set project <your-id>
```

### _Direct Runner options_:
* `--direct_num_workers`: The number of workers to use. We recommend 2 for local development.

Example run:
```shell script
weather-mv -i gs://netcdf_file.nc \
  -o project.dataset_id.table_id \
  -t gs://temp_location  \
  --direct_num_workers 2
```

For a full list of how to configure the direct runner, please review
[this page](https://beam.apache.org/documentation/runners/direct/).

### _Dataflow options_:
* `--runner`: The `PipelineRunner` to use. This field can be either `DirectRunner` or `DataflowRunner`. Default: `DirectRunner` (local mode)
* `--project`: The project ID for your Google Cloud Project. This is required if you want to run your pipeline using the Dataflow managed service (i.e. `DataflowRunner`).
* `--temp_location`: Cloud Storage path for temporary files. Must be a valid Cloud Storage URL, beginning with `gs://`.
* `--region`: Specifies a regional endpoint for deploying your Dataflow jobs. Default: `us-central1`.
* `--job_name`: The name of the Dataflow job being executed as it appears in Dataflow's jobs list and job details.


Example run: 
```shell script
weather-dl configs/seasonal_forecast_example_config.cfg \
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


