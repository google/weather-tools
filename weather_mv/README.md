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
usage: weather-mv [-h] [-v variables [variables ...]] [-a area [area ...]] -i URIS -o OUTPUT_TABLE [--import_time IMPORT_TIME] [--infer_schema] [-d]

Weather Mover loads weather data from cloud storage into Google BigQuery.
```

_Common options_:

* `-i, --uris`: (required) URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-""
* `-o, --output_table`: (required) Full name of destination BigQuery table. Ex: my_project.my_dataset.my_table
* `--import_time`: When writing data to BigQuery, record that data import occurred at this time
  (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC.
* `-v, --variables`:  Target variables for the BigQuery schema. Default: will import all data variables as columns.
* `-a, --area`:  Target area in [N, W, S, E]. Default: Will include all available area.
* `--infer_schema`: Download one file in the URI pattern and infer a schema from that file. Default: off

Invoke with `-h` or `--help` to see the full range of options.

> Warning: Dry-runs are currently not supported. See [#22](https://github.com/googlestaging/weather-tools/issues/22).

_Usage Examples_:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --direct_num_workers 2
```

Upload only a subset of variables:

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --variables u10 v10 t
           --direct_num_workers 2
```

Upload all variables, but for a specific geographic region (for example, the contitental US):

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --area 49.34 -124.68 24.74 -66.95 \
           --direct_num_workers 2
```

Using DataflowRunner

```bash
weather-mv --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp \
           --job_name $JOB_NAME 
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).
