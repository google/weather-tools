# ⛅️ `weather-mv` – Weather Mover

Weather Mover loads weather data from cloud storage into analytics engines,
like [Google BigQuery](https://cloud.google.com/bigquery) (_alpha_).

## Features

* **Rapid Querability**: After geospatial data is in BigQuery, data wranging becomes as simple as writing SQL. This
  allows for rapid data exploration, visualization, and model pipeline prototyping.
* **Simple Versioning**: All rows in the table come with a `data_import_time` column. This provides some notion of how
  the data is versioned. Downstream analysis can adapt to data ingested at differen times by updating a `WHERE` clause.
* **Parallel Upload**: Each file will be processed in parallel. With Dataflow autoscaling, even large datasets can be
  processed in a reasonable amount of time.
* **Streaming support**: When running the mover in streaming mode, it will automatically process files as they appear in
  cloud buckets via PubSub.
* _(new)_ **Grib Regridding**: `weather-mv regrid` uses [MetView](https://metview.readthedocs.io/en/latest/) to
  interpolate Grib files to a
  [range of grids.](https://metview.readthedocs.io/en/latest/metview/using_metview/regrid_intro.html?highlight=grid#grid)

## Usage

```
usage: weather-mv [-h] {bigquery,bq,regrid,rg} ...

Weather Mover loads weather data from cloud storage into analytics engines.

positional arguments:
  {bigquery,bq,regrid,rg}
                        help for subcommand
    bigquery (bq)       Move data into Google BigQuery
    regrid (rg)         Copy and regrid grib data with MetView.

optional arguments:
  -h, --help            show this help message and exit
```

The weather mover makes use of subcommands to distinguish between tasks. The above tasks are currently supported.

_Common options_

* `-i, --uris`: (required) URI glob pattern matching input weather data, e.g. 'gs://ecmwf/era5/era5-2015-*.gb'.
* `--topic`: A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. E.g.
  'projects/<PROJECT_ID>/topics/<TOPIC_ID>'.
* `--window_size`: Output file's window size in minutes. Only used with the `topic` flag. Default: 1.0 minute.
* `--num_shards`: Number of shards to use when writing windowed elements to cloud storage. Only used with the `topic`
  flag. Default: 5 shards.
* `-d, --dry-run`: Preview the load into BigQuery. Default: off.

Invoke with `-h` or `--help` to see the full range of options.

### `weather-mv bigquery`

```
usage: weather-mv bigquery [-h] -i URIS [--topic TOPIC] [--window_size WINDOW_SIZE] [--num_shards NUM_SHARDS] [-d]
                           -o OUTPUT_TABLE [-v variables [variables ...]] [-a area [area ...]]
                           [--import_time IMPORT_TIME] [--infer_schema]
                           [--xarray_open_dataset_kwargs XARRAY_OPEN_DATASET_KWARGS] [--disable_in_memory_copy]
                           [--tif_metadata_for_datetime TIF_METADATA_FOR_DATETIME] [-s]
                           [--coordinate_chunk_size COORDINATE_CHUNK_SIZE]
```

The `bigquery` subcommand loads weather data into BigQuery. In addition to the common options above, users may specify
command-specific options:

_Command options_:

* `-o, --output_table`: (required) Full name of destination BigQuery table. Ex: my_project.my_dataset.my_table
* `-v, --variables`:  Target variables (or coordinates) for the BigQuery schema. Default: will import all data variables
  as columns.
* `-a, --area`:  Target area in [N, W, S, E]. Default: Will include all available area.
* `--import_time`: When writing data to BigQuery, record that data import occurred at this time
  (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC.
* `--infer_schema`: Download one file in the URI pattern and infer a schema from that file. Default: off
* `--xarray_open_dataset_kwargs`: Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.
* `--disable_in_memory_copy`: Restrict in-memory copying of dataset. Default: False.
* `--coordinate_chunk_size`: The size of the chunk of coordinates used for extracting vector data into BigQuery. Used to
  tune parallel uploads.
* `--tif_metadata_for_datetime` : Metadata that contains tif file's timestamp. Applicable only for tif files.
* `-s, --skip-region-validation` : Skip validation of regions for data migration. Default: off.
* `--disable_grib_schema_normalization` : To disable grib's schema normalization. Default: off.

Invoke with `bq -h` or `bigquery --help` to see the full range of options.

> Note: In case of grib files, by default its schema will be normalized and the name of the data variables will look 
> like `<level>_<height>_<attrs['GRIB_stepType']>_<key>`.
> 
> This solves the issue of skipping over some of the data due to: https://github.com/ecmwf/cfgrib#filter-heterogeneous-grib-files.

_Usage examples_:

```bash
weather-mv bigquery --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2
```

Using the subcommand alias `bq`:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2
```

Preview load with a dry run:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --dry-run
```

Restrict in-memory copying of dataset:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --disable_in_memory_copy
```

Load COG's (.tif) files:

```bash
weather-mv bq --uris "gs://your-bucket/*.tif" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --tif_metadata_for_datetime start_time
```

Upload only a subset of variables:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --variables u10 v10 t
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Upload all variables, but for a specific geographic region (for example, the continental US):

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --area 49.34 -124.68 24.74 -66.95 \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Control how weather data is opened with XArray:

```bash
weather-mv bq --uris "gs://your-bucket/*.grib" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --xarray_open_dataset_kwargs '{"engine": "cfgrib", "indexpath": "", "backend_kwargs": {"filter_by_keys": {"typeOfLevel": "surface", "edition": 1}}}' \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Using DataflowRunner:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --job_name $JOB_NAME 
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

### `weather-mv regrid`

```
usage: weather-mv regrid [-h] -i URIS [--topic TOPIC] [--window_size WINDOW_SIZE] [--num_shards NUM_SHARDS] [-d]
                         --output_path OUTPUT_PATH [--regrid_kwargs REGRID_KWARGS] [--to_netcdf]
```

The `regrid` subcommand makes a regridded copy of the input data with MetView.

To use this capability of the weather mover, please use the `[regrid]` extra when installing:

```shell
pip install google-weather-tools[regrid]
```

In addition to the common options above, users may specify command-specific options:

_Command options_:

* `-o, --output_path`: (required) The destination path for the regridded files.
* `-k, --regrid_kwargs`: Keyword-args to pass into `metview.regrid()` in the form of a JSON string. Will default to
  '{"grid": [0.25, 0.25]}'.
* `--to_netcdf`: Write output file in NetCDF via XArray. Default: off

For a full range of grid options, please
consult [this documentation.](https://metview.readthedocs.io/en/latest/metview/using_metview/regrid_intro.html?highlight=grid#grid)

Invoke with `rg -h` or `regrid --help` to see the full range of options.

> Note: Currently, `regrid` doesn't work out-of-the-box! Until [#172](https://github.com/google/weather-tools/issues/172)
> is fixed, users will have to use a workaround in order to ensure MetView is installed in their runner environment
> (instructions are below).

_Usage examples_:

```bash
weather-mv regrid --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" 
 
```

Using the subcomand alias 'rg':

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" 
 
```

Preview regrid with a dry run:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --dry-run
 
```

Interpolate to a finer grid resolution:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --regrid_kwargs '{"grid": [0.1, 0.1]}'.
 
```

Interpolate to a high-resolution octahedral gaussian grid:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --regrid_kwargs '{"grid": "O1280}'.
 
```

Convert gribs to NetCDF on copy:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --to_netcdf
```

Using DataflowRunner:

```bash
weather-mv rg --uris "gs://your-bucket/*.nc" \
           --output_path "gs://regrid-bucket/" \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --experiment=use_runner_v2 \
           --sdk_container_image="gcr.io/$PROJECT/$REPO:latest"  \
           --job_name $JOB_NAME 
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Streaming ingestion

`weather-mv` optionally provides the ability to react
to [Pub/Sub events for objects added to GCS](https://cloud.google.com/storage/docs/pubsub-notifications). This can be
used to automate ingestion into BigQuery as soon as weather data is disseminated. Another common use case it to
automatically create a down-sampled version of a dataset with `regrid`. To set up the Weather Mover with streaming
ingestion, use the `--topic` flag (see "Common options" above).

Objects that don't match the `--uris` glob pattern will be filtered out of ingestion. This way, a bucket can contain
multiple types of data yet only have subsets processed with `weather-mv`.

> It's worth noting: when setting up PubSub, **make sure to create a topic for GCS `OBJECT_FINALIZE` events only.**

_Usage examples_:

```shell
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --topic "projects/$PROJECT/topics/$TOPIC_ID" \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp \
           --job_name $JOB_NAME 
```

Window incoming data every five minutes instead of every minute.

```shell
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --topic "projects/$PROJECT/topics/$TOPIC_ID" \
           --window_size 5 \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp \
           --job_name $JOB_NAME 
```

Increase the number of shards per window.

```shell
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --topic "projects/$PROJECT/topics/$TOPIC_ID" \
           --num_shards 10 \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp \
           --job_name $JOB_NAME 
```

### BigQuery

Data is written into BigQuery using streaming inserts. It may
take [up to 90 minutes](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataavailability)
for buffers to persist into storage. However, weather data will be available for querying immediately.

> Note: It's recommended that you specify variables to ingest (`-v, --variables`) instead of inferring the schema for
> streaming pipelines. Not all variables will be distributed with every file, especially when they are in Grib format.

## Private Network Configuration

While running `weather-mv` pipeline in GCP, there is a possibility that you may receive following error -
"Quotas were exceeded: IN_USE_ADDRESSES"

This error occurs when GCP is trying to add new worker-instances and finds that, “Public IP” quota (assigned to your
project) is exhausted.

To solve this, we recommend using private IP while running your dataflow pipelines.

```shell
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location gs://$BUCKET/tmp \
           --runner DataflowRunner \
           --project $PROJECT \
           --region $REGION \
           --no_use_public_ips \
           --network=$NETWORK \
           --subnetwork=regions/$REGION/subnetworks/$SUBNETWORK
```

_Common options_:

* `--no_use_public_ips`: To make Dataflow workers use private IP addresses for all communication, specify the
  command-line flag: --no_use_public_ips. Make sure that the specified network or subnetwork has Private Google Access
  enabled.
* `--network`: The Compute Engine network for launching Compute Engine instances to run your pipeline.
* `--subnetwork`:  The Compute Engine subnetwork for launching Compute Engine instances to run your pipeline.

For more information regarding how to configure Private IP, please refer
to [Private IP Configuration Guide for Dataflow Pipeline Execution](https://docs.google.com/document/d/1MHzDSsV2EwsyvPSsW1rsxVzi9z91JmT_JOFQ7HZPBQ8/edit?usp=sharing)
.

For more information regarding Pipeline options, please refer
to [pipeline-options](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Custom Dataflow Container for ECMWF dependencies (like MetView)

It's difficult to install all necessary system dependencies on a Dataflow worker with a pure python solution. For
example, MetView requires binaries to be installed on the system machine, which are broken in the standard debian
install channels (they are only maintained via `conda-forge`).

Thus, to include such dependencies, we've provided steps for you to build
a [Beam container environment](https://beam.apache.org/documentation/runtime/environments/). In the near future, we'll
arrange things so you don't have to worry about any of these extra
steps ([#172](https://github.com/google/weather-tools/issues/172)).

Currently, this image is necessary for the `weather-mv regrid` command, but no other commands. To deploy this tool,
please do the following:

1. Host a container image of the included Dockerfile in your repository of choice (instructions for building images in
   GCS are in the next section).
2. Add the following two flags to your regrid pipeline.
   ```
   --experiment=use_runner_v2 \
   --sdk_container_image=$CONTAINER_URL
   ```
   For example, the full Dataflow command, assuming you follow the next section's instructions, should look like:

   ```bash
   weather-mv rg --uris "gs://your-bucket/*.nc" \
              --output_path "gs://regrid-bucket/" \
              --runner DataflowRunner \
              --project $PROJECT \
              --region  $REGION \
              --temp_location "gs://$BUCKET/tmp" \
              --experiment=use_runner_v2 \
              --sdk_container_image="gcr.io/$PROJECT/$REPO:latest"
              --job_name $JOB_NAME 
   ```

### Building & Publishing the container in GCS.

*Pre-requisites*: Install the gcloud CLI ([instructions are here](https://cloud.google.com/sdk/docs/install)).

Then, log in to your cloud account:

```shell
gcloud auth login
```

> Please follow all the instructions from the CLI. This will involve running an
> auth script on your local machine, which will open a browser window to log you
> in.

Last, make sure you have adequate permissions to use Google Cloud Build (see
[IAM options here](https://cloud.google.com/build/docs/iam-roles-permissions)).
[This documentation](https://g3doc.corp.google.com/company/gfw/support/cloud/products/cloud-build/index.md#permissions)
lists the specific permissions that you'll need to have in your project.

*Updating the image*: Please modify the `Dockerfile` in the tool directory. Then, build and upload the image with Google
Cloud Build:

```shell
export PROJECT=<your-project-here>
export REPO=miniconda3-beam
export IMAGE_URI=gcr.io/$PROJECT/$REPO
export TAG="0.0.2" # Please increment on every update.
# dev release
gcloud builds submit weather_mv/ --tag "$IMAGE_URI:dev"

# release:
gcloud builds submit weather_mv/ --tag "$IMAGE_URI:$TAG"  && gcloud builds submit weather_mv/ --tag "$IMAGE_URI:latest"
```