# `weather-dl` – Weather Downloader

Weather Downloader ingests weather data to cloud buckets, such as Google Cloud Storage.

```
usage: weather-dl [-h] [-f] [-d] [-l] [-m MANIFEST_LOCATION] config

Weather Downloader ingests weather data to cloud storage.

positional arguments:
  config                path/to/config.cfg, containing client and data information. Accepts *.cfg and *.json files.
```

_Common options_:

* `-f, --force-download`: Force redownload of partitions that were previously downloaded.
* `-d, --dry-run`: Run pipeline steps without _actually_ downloading or writing to cloud storage.
* `-m, --manifest-location MANIFEST_LOCATION`:  Location of the manifest. Either a Firestore collection URI
  ('fs://<my-collection>?projectId=<my-project-id>'), a GCS bucket URI, or 'noop://<name>' for an in-memory location.
* `-l, --local-run`: Run locally and download to local hard drive. The data and manifest directory is set by default
  to '<$CWD>/local_run'. The runner will be set to `DirectRunner`. The only other relevant option is the config
  and `--direct_num_workers`

Invoke with `-h` or `--help` to see the full range of options.

For further information on how to write config files, please consult [this documentation](../Configuration.md).

## Monitoring

You can view how your ECMWF API jobs are by visitng the client-specific job queue:

* [MARS](https://apps.ecmwf.int/mars-activity/)
* [Copernicus](https://cds.climate.copernicus.eu/live/queue)

### `download-status`

In addition, we've provided a simple tool for getting a rough measure of download state. Provided a bucket prefix, it
will output the counts of the statuses in that prefix.

```shell
usage: download-status [-h] [-m MANIFEST_LOCATION] prefix

Check statuses of `weather-dl` downloads.

positional arguments:
  prefix                Prefix of the location string (e.g. a cloud bucket); used to filter which statuses to check.
```

_Options_

* `-m`, `--manifest-location`: Specify the location to a manifest; this is the same as `weather-dl`. Only supports
  Firebase Manifests.

Example usage:

```shell
download-status "gs://ecmwf-downloads/hres/world/
...
The current download statuses for 'gs://ecmwf-downloads/hres/world/' are: Counter({'scheduled': 245, 'success': 116, 'in-progress': 4, 'failure': 1}).
```
