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
* _(new)_ **Earth Engine Ingestion**: `weather-mv earthengine` ingests weather data into [Google Earth Engine](https://earthengine.google.com/).

## Usage

```
usage: weather-mv [-h] {bigquery,bq,regrid,rg} ...

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

The weather mover makes use of subcommands to distinguish between tasks. The above tasks are currently supported.

_Common options_

* `-i, --uris`: (required) URI glob pattern matching input weather data, e.g. 'gs://ecmwf/era5/era5-2015-*.gb'.
* `--topic`: A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. E.g.
  'projects/<PROJECT_ID>/topics/<TOPIC_ID>'. Cannot be used with `--subscription`.
* `--subscription`: A Pub/Sub subscription for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. Cannot be 
  used with `--topic`.
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
                           [--xarray_open_dataset_kwargs XARRAY_OPEN_DATASET_KWARGS]
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
           --area 49 -124 24 -66 \
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

> **Warning**: MetView requires a decent amount of disk space in order to perform any regrid operation! Intermediary 
> regridding steps will write temporary grib data to disk. Thus, please make use of the `--disk_size_gb` Dataflow 
> option. A good rule of thumb would be to consume `30 + 2.5x` GBs of disk, where `x` is the size of each source data
> file. 
> 
> TODO([#191](https://github.com/google/weather-tools/issues/191)): Find smaller disk space bound.

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

Using DataflowRunner, with added disk per VM:

```bash
weather-mv rg --uris "gs://your-bucket/*.nc" \
           --output_path "gs://regrid-bucket/" \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --disk_size_gb 250 \ 
           --temp_location "gs://$BUCKET/tmp" \
           --experiment=use_runner_v2 \
           --sdk_container_image="gcr.io/$PROJECT/$REPO:latest"  \
           --job_name $JOB_NAME 
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

### `weather-mv earthengine`

```
usage: weather-mv earthengine [-h] -i URIS --asset_location ASSET_LOCATION --ee_asset EE_ASSET
                           [--ee_asset_type ASSET_TYPE] [--disable_grib_schema_normalization] [--use_personal_account] [-s]
                           [--xarray_open_dataset_kwargs XARRAY_OPEN_DATASET_KWARGS]
                           [--service_account my-service-account@...gserviceaccount.com --private_key PRIVATE_KEY_LOCATION]
                           [--ee_qps EE_QPS] [--ee_latency EE_LATENCY] [--ee_max_concurrent EE_MAX_CONCURRENT]
```

The `earthengine` subcommand ingests weather data into Earth Engine. It includes a caching function that allows it to
skip ingestion for assets that have already been created in Earth Engine or for which the asset file already exists in
the GCS bucket. In addition to the common options above, users may specify command-specific options:

_Command options_:

* `--asset_location`: (required) Bucket location at which asset files will be pushed.
* `--ee_asset`: (required) The asset folder path in earth engine project where the asset files will be pushed.
  It should be in format: `projects/<project-id>/assets/<asset-folder>`. Make sure that <asset-folder> is there
  under <project-id> in earth engine assets. i.e. projects/my-gcp-project/assets/my/foo/bar.
* `--ee_asset_type`: The type of asset to ingest in the earth engine. Default: IMAGE.
  Supported types are `IMAGE` and `TABLE`.\
  `IMAGE`: Uploads georeferenced raster datasets in GeoTIFF format.\
  `TABLE`: Uploads the datsets in the CSV format. Useful in case of point data (sparse data). 
* `--disable_grib_schema_normalization`:  Restricts merging of grib datasets. Default: False
* `-u, --use_personal_account`: To use personal account for earth engine authentication.
* `--service_account`: Service account address when using a private key for earth engine authentication.
* `--private_key`: To use a private key for earth engine authentication. Only used with the `service_account` flag.
* `--xarray_open_dataset_kwargs`: Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.
* `-s, --skip-region-validation` : Skip validation of regions for data migration. Default: off.
* `-f, --force`: A flag that allows overwriting of existing asset files in the GCS bucket. Default: off, which means
  that the ingestion of URIs for which assets files (GeoTiff/CSV) already exist in the GCS bucket will be skipped.
* `--ee_qps`: Maximum queries per second allowed by EE for your project. Default: 10.
* `--ee_latency`: The expected latency per requests, in seconds. Default: 0.5.
* `--ee_max_concurrent`: Maximum concurrent api requests to EE allowed for your project. Default: 10.
* `--band_names_mapping`: A JSON file which contains the band names for the TIFF file.
* `--initialization_time_regex`: A Regex string to get the initialization time from the filename.
* `--forecast_time_regex`: A Regex string to get the forecast/end time from the filename.

Invoke with `ee -h` or `earthengine --help` to see the full range of options.

_Usage examples_:

```bash
weather-mv earthengine --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
```

Using the subcommand alias `ee`:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
```

Preview ingestion with a dry run:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --dry-run
```

Authenticate earth engine using personal account:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --use_personal_account
```

Authenticate earth engine using a private key:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --service_account "my-service-account@...gserviceaccount.com" \
           --private_key "path/to/private_key.json"
```

Ingest asset as table in earth engine:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --ee_asset_type "TABLE"
```

Restrict merging all bands or grib normalization:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --disable_grib_schema_normalization
```

Control how weather data is opened with XArray:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
           --xarray_open_dataset_kwargs '{"engine": "cfgrib", "indexpath": "", "backend_kwargs": {"filter_by_keys": {"typeOfLevel": "surface", "edition": 1}}}' \
           --temp_location "gs://$BUCKET/tmp"
```

Limit EE requests:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --ee_qps 10 \
           --ee_latency 0.5 \
           --ee_max_concurrent 10
```

Custom Band names:

```bash
weather-mv ee --uris "gs://your-bucket/*.tif" \
           --asset_location "gs://$BUCKET/assets" \ # Needed to store assets generated from *.tif
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --band_names_mapping "filename.json"
```

Getting initialization and forecast/end date-time from the filename:

```bash
weather-mv ee --uris "gs://your-bucket/*.tif" \
           --asset_location "gs://$BUCKET/assets" \ # Needed to store assets generated from *.tif
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --initialization_time_regex "$REGEX" \
           --forecast_time_regex "$REGEX"
```

Example:

```bash
weather-mv ee --uris "gs://tmp-gs-bucket/3B-HHR-E_MS_MRG_3IMERG_20220901-S000000-E002959_0000_V06C_30min.tiff" \
           --asset_location "gs://$BUCKET/assets" \ # Needed to store assets generated from *.tif
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --initialization_time_regex "3B-HHR-E_MS_MRG_3IMERG_%Y%m%d-S%H%M%S-*tiff" \
           --forecast_time_regex "3B-HHR-E_MS_MRG_3IMERG_%Y%m%d-S*-E%H%M%S*tiff"
```

Using DataflowRunner:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --job_name $JOB_NAME
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Streaming ingestion

`weather-mv` optionally provides the ability to react
to [Pub/Sub events for objects added to GCS](https://cloud.google.com/storage/docs/pubsub-notifications). This can be
used to automate ingestion into BigQuery as soon as weather data is disseminated. Another common use case it to
automatically create a down-sampled version of a dataset with `regrid`. To set up the Weather Mover with streaming
ingestion, use the `--topic` or `--subscription` flag  (see "Common options" above).

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
to [Private IP Configuration Guide for Dataflow Pipeline Execution](../Private-IP-Configuration.md)
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
steps ([#172](https://github.com/google/weather-tools/issues/172)). See [these instructions](../Runtime-Container.md) 
to learn how to build a custom image for this project.

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
