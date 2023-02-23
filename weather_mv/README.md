# ⛅️ `weather-mv` – Weather Mover <sub><sup>_(alpha)_</sup></sub>

Weather Mover provides an easy and scalable way for:

1. Loading weather datasets 
   from Cloud Storage into [BigQuery](https://cloud.google.com/bigquery) and
   [Google Earth Engine](https://earthengine.google.com/)
2. Fast [interpolation & grid conversions](https://metview.readthedocs.io/en/latest/gen_files/icon_functions/regrid.html)
on large weather datasets

It supports popular multi-dimensional data formats including NetCDF, GRIB, 
GeoTIFF, and all other formats [supported by Xarray](https://docs.xarray.dev/en/stable/user-guide/io.html).

## Why do we need this?

* **Upload Speed**: Files in a weather dataset will be processed in parallel.
  With [Dataflow autoscaling](https://cloud.google.com/dataflow/docs/horizontal-autoscaling), 
  even large datasets can be transferred to analytical engines in a
  reasonable amount of time.
* **Rapid Querying**: After your geospatial data is in BigQuery, data wrangling
  becomes as simple as writing SQL. This
  allows for rapid data exploration, visualization, and model pipeline
  prototyping.
* **Simple Versioning**: All rows in the destination BigQuery table come with
  a `data_import_time`
  column. This provides some notion of how
  the data is versioned. Downstream analysis can adapt to data ingested at
  different times by updating a `WHERE` clause.
* **Streaming Support**: Weather Mover supports a streaming mode where it
  automatically processes files as they appear in cloud buckets via PubSub.
* **Scalable Regridding**: The regrid option
  uses [MetView](https://metview.readthedocs.io/en/latest/) to
  interpolate Grib files to a
  [range of grids.](https://metview.readthedocs.io/en/latest/metview/using_metview/regrid_intro.html?highlight=grid#grid)
  To learn more about its use cases,
  visit [MetView docs](https://metview.readthedocs.io/en/latest/metview/using_metview/regrid_intro.html#regrid-explained).
* **Earth Engine**: By ingesting weather data into [Google Earth Engine](https://earthengine.google.com/),
  research teams can use its powerful [APIs and App Development interface](https://developers.google.com/earth-engine)
  for visualization and analysis of geospatial data.

## Usage

```
usage: weather-mv [-h] {bigquery,bq,regrid,rg,earthengine,ee} ...

Weather Mover loads weather data from cloud storage into analytics engines.

positional arguments:
  {bigquery,bq,regrid,rg,earthengine,ee}
                        help for subcommand
    bigquery (bq)       Move data into Google BigQuery
    regrid (rg)         Copy and regrid grib data with MetView.
    earthengine (ee)    Move data into Google Earth Engine

optional arguments:
  -h, --help            show this help message and exit
```

The three main subcommands are:

* `bigquery` or `bq`: Ingest data from Cloud Storage into BigQuery
* `earthengine` or `ee`: Ingest data from Cloud Storage into Earth Engine
* `regrid` or `rg`: Regrid data in Cloud Storage using MetView

### General Flags

* `-i, --uris`: (required) URI glob pattern matching input weather data, e.g. `
  gs://ecmwf/era5/era5-2015-*.gb`.
* `--topic`: A Pub/Sub topic for GCS `OBJECT_FINALIZE` [events](https://cloud.google.com/storage/docs/pubsub-notifications#events), 
   e.g. `projects/<PROJECT_ID>/topics/<TOPIC_ID>`.
* `--window_size`: Output file's window size in minutes, only used with
  the `topic` flag. (default: _1 min_)
* `--num_shards`: Number of shards to use when writing windowed elements to
  Cloud Storage. Only used with the `topic` flag. (default: _5_)
* `-d, --dry-run`: Preview the load into BigQuery. (default: _disabled_)

Invoke with `-h` or `--help` to see the full range of options.

### BigQuery

```
usage: weather-mv bigquery [-h] -i URIS [--topic TOPIC] [--window_size WINDOW_SIZE] [--num_shards NUM_SHARDS] [-d]
                           -o OUTPUT_TABLE [-v variables [variables ...]] [-a area [area ...]]
                           [--import_time IMPORT_TIME] [--infer_schema]
                           [--xarray_open_dataset_kwargs XARRAY_OPEN_DATASET_KWARGS]
                           [--tif_metadata_for_datetime TIF_METADATA_FOR_DATETIME] [-s]
                           [--coordinate_chunk_size COORDINATE_CHUNK_SIZE]
```

The `bigquery` subcommand loads weather data into BigQuery. In addition to the
common options above, users may specify
command-specific options:

_Command options_:

* `-o, --output_table`: (required) Full name of destination BigQuery table. Ex:
  my_project.my_dataset.my_table
* `-v, --variables`:  Target variables (or coordinates) for the BigQuery schema.
  Default: will import all data variables
  as columns.
* `-a, --area`:  Target area in [N, W, S, E]. Default: Will include all
  available area.
* `--import_time`: When writing data to BigQuery, record that data import
  occurred at this time
  (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC.
* `--infer_schema`: Download one file in the URI pattern and infer a schema from
  that file. Default: off
* `--xarray_open_dataset_kwargs`: Keyword-args to pass
  into `xarray.open_dataset()` in the form of a JSON string.
* `--coordinate_chunk_size`: The size of the chunk of coordinates used for
  extracting vector data into BigQuery. Used to
  tune parallel uploads.
* `--tif_metadata_for_datetime` : Metadata that contains tif file's timestamp.
  Applicable only for tif files.
* `-s, --skip-region-validation` : Skip validation of regions for data
  migration. Default: off.
* `--disable_grib_schema_normalization` : To disable grib's schema
  normalization. Default: off.

Invoke with `bq -h` or `bigquery --help` to see the full range of options.

> Note: In case of grib files, by default its schema will be normalized and the
> name of the data variables will look
> like `<level>_<height>_<attrs['GRIB_stepType']>_<key>`.
>
> This solves the issue of skipping over some of the data due
> to: https://github.com/ecmwf/cfgrib#filter-heterogeneous-grib-files.

_Example:_

```bash
weather-mv bigquery --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2
```

For an extensive set of examples, visit [here](Examples.md#bigquery).


### Regrid

```
usage: weather-mv regrid [-h] -i URIS [--topic TOPIC] [--window_size WINDOW_SIZE] [--num_shards NUM_SHARDS] [-d]
                         --output_path OUTPUT_PATH [--regrid_kwargs REGRID_KWARGS] [--to_netcdf]
```

The `regrid` subcommand makes a regridded copy of the input data with MetView.


_Command options_:

* `-o, --output_path`: (required) The destination path for the regridded files.
* `-k, --regrid_kwargs`: Keyword-args to pass into `metview.regrid()` in the
  form of a JSON string. Will default to
  '{"grid": [0.25, 0.25]}'.
* `--to_netcdf`: Write output file in NetCDF via XArray. Default: off

For a full range of grid options, please
consult [this documentation.](https://metview.readthedocs.io/en/latest/metview/using_metview/regrid_intro.html?highlight=grid#grid)

Invoke with `rg -h` or `regrid --help` to see the full range of options.

> **Warning**: MetView requires a decent amount of disk space in order to
> perform any regrid operation! Intermediary
> regridding steps will write temporary grib data to disk. Thus, please make use
> of the `--disk_size_gb` Dataflow
> option. A good rule of thumb would be to consume `30 + 2.5x` GBs of disk,
> where `x` is the size of each source data
> file.
>
> TODO([#191](https://github.com/google/weather-tools/issues/191)): Find smaller
> disk space bound.

_Example_:

```bash
weather-mv regrid --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" 
 
```

For an extensive set of examples, visit [here](Examples.md#regrid).

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

### Earth Engine

```
usage: weather-mv earthengine [-h] -i URIS --asset_location ASSET_LOCATION --ee_asset EE_ASSET
                           [--ee_asset_type ASSET_TYPE] [--disable_grib_schema_normalization] [--use_personal_account] [-s]
                           [--xarray_open_dataset_kwargs XARRAY_OPEN_DATASET_KWARGS]
                           [--service_account my-service-account@...gserviceaccount.com --private_key PRIVATE_KEY_LOCATION]
                           [--ee_qps EE_QPS] [--ee_latency EE_LATENCY] [--ee_max_concurrent EE_MAX_CONCURRENT]
```

The `earthengine` subcommand ingests weather data into Earth Engine. In addition
to the general flags above, users may specify command-specific options:

_Command options_:

* `--asset_location`: (required) Bucket location at which asset files will be
  pushed.
* `--ee_asset`: (required) The asset folder path in earth engine project where
  the asset files will be pushed.
  It should be in format: `projects/<project-id>/assets/<asset-folder>`. Make
  sure that <asset-folder> is there
  under <project-id> in earth engine assets. i.e.
  projects/my-gcp-project/assets/my/foo/bar.
* `--ee_asset_type`: The type of asset to ingest in the earth engine. Default:
  IMAGE.
  Supported types are `IMAGE` and `TABLE`.\
  `IMAGE`: Uploads georeferenced raster datasets in GeoTIFF format.\
  `TABLE`: Uploads the datsets in the CSV format. Useful in case of point data (
  sparse data).
* `--disable_grib_schema_normalization`:  Restricts merging of grib datasets.
  Default: False
* `-u, --use_personal_account`: To use personal account for earth engine
  authentication.
* `--service_account`: Service account address when using a private key for
  earth engine authentication.
* `--private_key`: To use a private key for earth engine authentication. Only
  used with the `service_account` flag.
* `--xarray_open_dataset_kwargs`: Keyword-args to pass
  into `xarray.open_dataset()` in the form of a JSON string.
* `-s, --skip-region-validation` : Skip validation of regions for data
  migration. Default: off.
* `--ee_qps`: Maximum queries per second allowed by EE for your project.
  Default: 10.
* `--ee_latency`: The expected latency per requests, in seconds. Default: 0.5.
* `--ee_max_concurrent`: Maximum concurrent api requests to EE allowed for your
  project. Default: 10.
* `--band_names_mapping`: A JSON file which contains the band names for the TIFF
  file.
* `--initialization_time_regex`: A Regex string to get the initialization time
  from the filename.
* `--forecast_time_regex`: A Regex string to get the forecast/end time from the
  filename.

Invoke with `ee -h` or `earthengine --help` to see the full range of options.

_Example_:

```bash
weather-mv earthengine --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
```

For an extensive set of examples, visit [here](Examples.md#earth-engine).

## Configuring Dataflow

One of the most powerful features of Apache Beam is supporting a wide range of
execution backends, called [_runners_](https://beam.apache.org/documentation/runners/direct/).
The default [_Direct Runner_](https://beam.apache.org/documentation/runners/direct/)
 is a multi-threaded single-node backend which executes the pipeline in the local 
machine. In contrast, the [_Dataflow Runner_](https://beam.apache.org/documentation/runners/dataflow/)
deploys the pipeline on [Cloud Dataflow](https://cloud.google.com/dataflow), 
a fully-managed Google Cloud service which is highly optimized for Apache Beam
pipelines. 

Using Dataflow has many benefits including convenient high-performance
integration with Cloud Storage and BigQuery as well as easy scaling.
The runner used for Weather Tools pipelines can be configured using
the `--runner` option (e.g. `--runner DataflowRunner`).
Additional configurations may also be passed to customize the Dataflow
pipeline including resources and autoscaling settings.

For a full reference of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Streaming ingestion

`weather-mv` optionally provides the ability to react
to [Pub/Sub events for objects added to GCS](https://cloud.google.com/storage/docs/pubsub-notifications).
This can be used to automate ingestion into BigQuery as soon as weather data is
disseminated. Another common use case it to automatically create a down-sampled
version of a dataset with `regrid`. To set up the Weather Mover with streaming
ingestion, use the `--topic` flag (see [General Flags](#general-flags)).

Objects that don't match the `--uris` glob pattern will be filtered out of
ingestion. This way, a bucket can contain
multiple types of data yet only have subsets processed with `weather-mv`.

> **NOTE:** When setting up PubSub, make sure to create a topic for
**GCS `OBJECT_FINALIZE` events only**.

> **NOTE:** Data is written into BigQuery using streaming inserts. It may
> take [up to 90 minutes](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataavailability)
> for buffers to persist into storage. However, weather data will be available for
> querying immediately.

> **NOTE:** It's recommended that you specify variables to
> ingest (`-v, --variables`) instead of inferring the schema for
> streaming pipelines. Not all variables will be distributed with every file,
> especially when they are in Grib format.

_Examples_:

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

## Known Issues

### "Quotas were exceeded: IN_USE_ADDRESSES"

This error occurs when GCP is trying to add new worker-instances and finds that,
“Public IP” quota (assigned to your project) is exhausted.

To solve this, we recommend using private IP while running your dataflow
pipelines.

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

_Network configuration options_:

* `--no_use_public_ips`: To make Dataflow workers use private IP addresses for
  all communication, specify the
  command-line flag: --no_use_public_ips. Make sure that the specified network
  or subnetwork has Private Google Access
  enabled.
* `--network`: The Compute Engine network for launching Compute Engine instances
  to run your pipeline.
* `--subnetwork`:  The Compute Engine subnetwork for launching Compute Engine
  instances to run your pipeline.

To learn more, visit [Private IP Configuration Guide for Dataflow Pipeline Execution](../Private-IP-Configuration.md). 

For more information regarding Dataflow pipeline options, see the [Configuring Dataflow](#configuring-dataflow)
section above.

## Dataflow Container Image

Weather Mover uses 3rd-party binary dependencies including MetView which can
not be reliably set up using pure python solutions. To overcome this, we use
the [Conda-forge](https://conda-forge.org/) distribution of these binaries.
While our [Conda environment](https://github.com/google/weather-tools/blob/main/environment.yml)
is sufficient for local runs, in remote executions like Dataflow, a Docker 
container image with the dependencies pre-installed is required.

By default, our public Docker image 
`gcr.io/weather-tools-prod/weather-tools:x.y.z` is used when the pipeline is 
executed using _DataflowRunner_. The image is built from [this Dockerfile](https://github.com/google/weather-tools/blob/main/Dockerfile)
and produces the same Conda environment.

For development purposes, you may use your own customized Docker image. See
[these instructions](../Runtime-Container.md) to learn how to build a custom
image for this project using [Cloud Build](https://cloud.google.com/build).
After building and pushing the custom container image to a container registry of
your choice (like [Cloud Container Registry](https://cloud.google.com/container-registry)),
you can then specify the image used by Dataflow workers for running your 
pipeline using these flags:

 ```
 --experiment=use_runner_v2 \
 --sdk_container_image=$CONTAINER_URL
 ```

_Example:_

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

> **NOTE:** Currently, this image is only necessary for the `weather-mv regrid`
> command as the other commands use pure python dependencies.
