# ECMWF-pipeline

[Build Status](https://pantheon.corp.google.com/cloud-build/dashboard?project=grid-intelligence-sandbox)

TODO(b/167705057): Upgrade CI.

## Goals

Goal: Create a pipeline to make [ECMWF](https://www.ecmwf.int/) data available to all teams at Alphabet.

Milestone 1: Load a subset of [historical ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/archive-datasets) needed for [DeepMind's wind energy-related forecasting](https://deepmind.com/blog/article/machine-learning-can-boost-value-wind-energy).
- [ ] Use MARs API to download ECMWF's HRES forecasts
- [ ] Download ECMWF's ENS forecasts
- [ ] Pipe downloaded data into BigQuery for general use

## Installing

ECMWF-pipeline can be installed via source code:
```
gcloud init
gcloud source repos clone ecmwf-pipeline --project=grid-intelligence-sandbox
cd ecmwf-pipeline
pip install .
```

TODO(b/167705057) Update installation instructions for GitLab.

For additional developer setup, please follow the [contributing guidelines](CONTRIBUTING.md).

## Usage
```
usage: ecmwf_download [-h] [-c {cdn}] config

positional arguments:
  config                path/to/config.cfg, specific to the <client>. Accepts *.cfg and *.json files.
```
Common options: 
* `-c, --client`: Select the weather API client. The default is 'cnd' or Copernicus. More options
will be supported later.

Invoke with `-h` or `--help` to see the full range of options.

For further information on how to write config files, please consult [this documentation](docs/Configuration.md).

## FAQ

### Q: Where does PubSub Fit in? 
We plan to use GCP's PubSub service to process [live ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/catalogue-ecmwf-real-time-products). Ideally, we'd like to plumb near-realtime data to BigQuery
via PubSub, if possible. We may need to add a Dataflow / Beam intermediary for this -- investigating is needed.

