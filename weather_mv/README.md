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

Invoke with `-h` or `--help` to see the full range of options.

> Warning: Dry-runs are currently not supported. See [#22](https://github.com/googlestaging/weather-tools/issues/22).

_Usage Examples_:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2
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
           --temp_location gs://$BUCKET/tmp \ 
           --area 49.34 -124.68 24.74 -66.95 \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Control how weather data is opened with XArray.

```bash
weather-mv --uris "gs://your-bucket/*.grib" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location gs://$BUCKET/tmp \ 
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
