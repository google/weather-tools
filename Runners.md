# Choosing a Beam Runner

All tools use Apache Beam pipelines. By default, pipelines run locally using the `DirectRunner`. You can optionally
choose to run the pipelines on [Google Cloud Dataflow](https://cloud.google.com/dataflow) by selection
the `DataflowRunner`.

When working with GCP, it's recommended you set the project ID up front with the command:

```shell
gcloud config set project <your-id>
```

## _Direct Runner options_:

* `--direct_num_workers`: The number of workers to use. We recommend 2 for local development.

Example run:

```shell
weather-mv -i gs://netcdf_file.nc \
  -o $PROJECT.$DATASET_ID.$TABLE_ID \
  -t gs://$BUCKET/tmp  \
  --direct_num_workers 2
```

For a full list of how to configure the direct runner, please review
[this page](https://beam.apache.org/documentation/runners/direct/).

## _Dataflow options_:

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

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Monitoring

When running Dataflow, you
can [monitor jobs through UI](https://cloud.google.com/dataflow/docs/guides/using-monitoring-intf),
or [via Dataflow's CLI commands](https://cloud.google.com/dataflow/docs/guides/using-command-line-intf):

For example, to see all outstanding Dataflow jobs, simply run:

```shell
gcloud dataflow jobs list
```

To describe stats about a particular Dataflow job, run:

```shell
gcloud dataflow jobs describe $JOBID
```

In addition, Dataflow provides a series
of [Beta CLI commands](https://cloud.google.com/sdk/gcloud/reference/beta/dataflow).

These can be used to keep track of job metrics, like so:

```shell
JOBID=<enter job id here>
gcloud beta dataflow metrics list $JOBID --source=user
```

You can even [view logs via the beta commands](https://cloud.google.com/sdk/gcloud/reference/beta/dataflow/logs/list):

```shell
gcloud beta dataflow logs list $JOBID
```

