# ⛈ `weather-dl` – Weather Downloader

Weather Downloader ingests weather data to cloud buckets, such
as [Google Cloud Storage](https://cloud.google.com/storage) (_beta_).

## Features

* **Flexible Pipelines**: `weather-dl` offers a high degree of control over what is downloaded via configuration files.
  Separate scripts need not be written to get new data or add parameters. For more, see the
  [configuration docs](../Configuration.md).
* **Efficient Parallelization**: The tool gives you full control over how downloads are sharded and parallelized (with
  good defaults). This lets you focus on the data and not the plumbing.
* **Hassle-Free Dev-Ops**. `weather-dl` and Dataflow make it easy to spin up VMs on your behalf with one command. No
  need to keep your local machine online all night to acquire data.
* **Robust Downloads**. If an error occurs when fetching a shard, Dataflow will automatically retry the download for
  you. Previously downloaded shards will be skipped by default, so you can re-run the tool without having to worry about
  duplication of work.

> Note: Currently, only ECMWF's MARS and CDS clients are supported. If you'd like to use `weather-dl` to work with other
> data sources, please [file an issue](https://github.com/googlestaging/weather-tools/issues) (or consider
> [making a contribution](../CONTRIBUTING.md)).

## Usage

```
usage: weather-dl [-h] [-f] [-d] [-l] [-m MANIFEST_LOCATION] [-n NUM_REQUESTS_PER_KEY] [-p PARTITION_CHUNKS] config

Weather Downloader ingests weather data to cloud storage.

positional arguments:
  config                path/to/config.cfg, containing client and data information. Accepts *.cfg and *.json files.
```

_Common options_:

* `-f, --force-download`: Force redownload of partitions that were previously downloaded.
* `-d, --dry-run`: Run pipeline steps without _actually_ downloading or writing to cloud storage.
* `-l, --local-run`: Run locally and download to local hard drive. The data and manifest directory is set by default
  to '<$CWD>/local_run'. The runner will be set to `DirectRunner`. The only other relevant option is the config
  and `--direct_num_workers`
* `-m, --manifest-location MANIFEST_LOCATION`: Location of the manifest. By default, it will use Cloud Logging 
  (stdout for direct runner). You can set the name of the manifest as the hostname of a URL with the 'cli' protocol. 
  For example, 'cli://manifest' will prefix all the manifest logs as '[manifest]'. In addition, users can specify a GCS 
  bucket URI, or 'noop://<name>' for an in-memory location.
* `-n, --num-requests-per-key`: Number of concurrent requests to make per API key. Default: make an educated guess per
  client & config. Please see the client documentation for more details.
* `-p, --partition-chunks`: Group shards into chunks of this size when computing the partitions. Specifically, this 
  affects how we chunk elements in a cartesian product, which affects parallelization of that step. Default: chunks of
  1000 elements.

Invoke with `-h` or `--help` to see the full range of options.

For further information on how to write config files, please consult [this documentation](../Configuration.md).

_Usage Examples_:

```bash
weather-dl configs/era5_example_config_local_run.cfg --local-run
```

Preview download with a dry run:

```bash
weather-dl configs/mars_example_config.cfg --dry-run
```

Using DataflowRunner

```bash
weather-dl configs/mars_example_config.cfg \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp  \
           --job_name $JOB_NAME
```

Using the DataflowRunner and specifying 3 requests per license

```bash
weather-dl configs/mars_example_config.cfg \
           -n 3 \
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp  \
           --job_name $JOB_NAME
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Monitoring

You can view how your ECMWF API jobs are by visitng the client-specific job queue:

* [MARS](https://apps.ecmwf.int/mars-activity/)
* [Copernicus](https://cds.climate.copernicus.eu/live/queue)

If you use Google Cloud Storage, we recommend using [`gsutil` (link)](https://cloud.google.com/storage/docs/gsutil) to
inspect the progress of your downloads. For example:

```shell
# Check that the file-sizes of your downloads look alright
gsutil du -h gs://your-cloud-bucket/mars-data/*T00z.nc 
# See how many downloads have finished
gsutil du -h gs://your-cloud-bucket/mars-data/*T00z.nc | wc -l
```
