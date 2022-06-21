# ⛅️ `weather-mv` – Weather Mover

Weather Mover loads weather data from cloud storage into [Google BigQuery](https://cloud.google.com/bigquery) (_alpha_).

## Features

* **Rapid Querability**: After geospatial data is in BigQuery, data wranging becomes as simple as writing SQL. This
  allows for rapid data exploration, visualization, and model pipeline prototyping.
* **Simple Versioning**: All rows in the table come with a `data_import_time` column. This provides some notion of how
  the data is versioned. Downstream analysis can adapt to data ingested at differen times by updating a `WHERE` clause.
* **Parallel Upload**: Each file will be processed in parallel. With Dataflow autoscaling, even large datasets can be
  processed in a reasonable amount of time.

## Usage

```
usage: weather-mv [-h] -i URIS -o OUTPUT_TABLE [-v variables [variables ...]] [-a area [area ...]]
                  [--topic TOPIC] [--window_size WINDOW_SIZE] [--num_shards NUM_SHARDS]
                  [--import_time IMPORT_TIME] [--infer_schema] [-d]

Weather Mover loads weather data from cloud storage into Google BigQuery.
```

_Common options_:

* `-i, --uris`: (required) URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-""
* `-o, --output_table`: (required) Full name of destination BigQuery table. Ex: my_project.my_dataset.my_table
* `-v, --variables`:  Target variables (or coordinates) for the BigQuery schema. Default: will import all data variables
  as columns.
* `-a, --area`:  Target area in [N, W, S, E]. Default: Will include all available area.
* `--topic`: A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. E.g. 'projects/<
  PROJECT_ID>/topics/<TOPIC_ID>'.
* `--window_size`: Output file's window size in minutes. Only used with the `topic` flag. Default: 1.0 minute.
* `--num_shards`: Number of shards to use when writing windowed elements to cloud storage. Only used with the `topic`
  flag. Default: 5 shards.
* `--import_time`: When writing data to BigQuery, record that data import occurred at this time
  (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC.
* `--infer_schema`: Download one file in the URI pattern and infer a schema from that file. Default: off
* `--xarray_open_dataset_kwargs`: Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.
* `--coordinate_chunk_size`: The size of the chunk of coordinates used for extracting vector data into BigQuery. 
  Used to tune parallel uploads.
* `--tif_metadata_for_datetime` : Metadata that contains tif file's timestamp. Applicable only for tif files.
* `-d, --dry-run`: Preview the load into BigQuery. Default: off.
* `--disable_in_memory_copy`: Restrict in-memory copying of dataset. Default: False.
* `-s, --skip-region-validation` : Skip validation of regions for data migration. Default: off.

Invoke with `-h` or `--help` to see the full range of options.

_Usage Examples_:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2
```

Preview load with a dry run:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --dry-run
```

Restrict in-memory copying of dataset:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --disable_in_memory_copy
```

Load COG's (.tif) files:

```bash
weather-mv --uris "gs://your-bucket/*.tif" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --tif_metadata_for_datetime start_time
```

Upload only a subset of variables:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --variables u10 v10 t
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Upload all variables, but for a specific geographic region (for example, the continental US):

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --area 49.34 -124.68 24.74 -66.95 \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Control how weather data is opened with XArray.

```bash
weather-mv --uris "gs://your-bucket/*.grib" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --xarray_open_dataset_kwargs '{"engine": "cfgrib", "indexpath": "", "backend_kwargs": {"filter_by_keys": {"typeOfLevel": "surface", "edition": 1}}}' \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Using DataflowRunner

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --job_name $JOB_NAME 
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Streaming ingestion into BigQuery

`weather-mv` optionally provides the ability to react
to [Pub/Sub events for objects added to GCS](https://cloud.google.com/storage/docs/pubsub-notifications). This can be
used to automate ingestion into BigQuery as soon as weather data is disseminated. To set up the Weather Mover with
streaming ingestion, use the `--topic` flag (see above). Objects that don't match the `--uris` will be filtered out of
ingestion. It's worth noting: when setting up PubSub, **make sure to create a topic for GCS `OBJECT_FINALIZE` events
only.**

Data is written into BigQuery using streaming inserts. It may
take [up to 90 minutes](https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataavailability)
for buffers to persist into storage. However, weather data will be available for querying immediately.

> Note: It's recommended that you specify variables to ingest (`-v, --variables`) instead of inferring the schema for
> streaming pipelines. Not all variables will be distributed with every file, especially when they are in Grib format.

_Usage Examples_:

```shell
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --topic "projects/$PROJECT/topics/$TOPIC_ID" \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp \
           --job_name $JOB_NAME 
```

Window incoming data every five minutes instead of every minute.

```shell
weather-mv --uris "gs://your-bucket/*.nc" \
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
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --topic "projects/$PROJECT/topics/$TOPIC_ID" \
           --num_shards 10 \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp \
           --job_name $JOB_NAME 
```

## Private Network Configuration

While running `weather-mv` pipeline in GCP, there is a possibility that you may receive following error -
"Quotas were exceeded: IN_USE_ADDRESSES"

This error occurs when GCP is trying to add new worker-instances and finds that, “Public IP” quota (assigned to your project) is exhausted.

To solve this, we recommend using private IP while running your dataflow pipelines.

```shell
weather-mv --uris "gs://your-bucket/*.nc" \
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

* `--no_use_public_ips`: To make Dataflow workers use private IP addresses for all communication, specify the command-line flag: --no_use_public_ips. Make sure that the specified network or subnetwork has Private Google Access enabled.
* `--network`: The Compute Engine network for launching Compute Engine instances to run your pipeline.
* `--subnetwork`:  The Compute Engine subnetwork for launching Compute Engine instances to run your pipeline.

For more information regarding how to configure Private IP, please refer to [Private IP Configuration Guide for Dataflow Pipeline Execution](https://docs.google.com/document/d/1MHzDSsV2EwsyvPSsW1rsxVzi9z91JmT_JOFQ7HZPBQ8/edit?usp=sharing).

For more information regarding Pipeline options, please refer to [pipeline-options](https://cloud.google.com/dataflow/docs/reference/pipeline-options).
