
![Build Status](https://gitlab.com/google-pso/ais/grid_intelligence_ai/ecmwf/badges/master/pipeline.svg)

**Goal**: Create a pipeline to make [ECMWF](https://www.ecmwf.int/) data available to all of Alphabet.

_Milestone 1_: Load a subset of [historical ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/archive-datasets) needed for [DeepMind's wind energy-related forecasting](https://deepmind.com/blog/article/machine-learning-can-boost-value-wind-energy).
- [ ] Use MARs API to download ECMWF's HRES forecasts
- [ ] Download ECMWF's ENS forecasts
- [ ] Pipe downloaded data into BigQuery for general use

## Installing

1). Create a personal_access_token with `read_api` scope ([docs](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html)).

2). Run the following command (substituting your <personal_access_token>):

```
pip install ecmwf-pipeline --no-deps --index-url https://__token__:<personal_access_token>@gitlab.com/api/v4/projects/20919443/packages/pypi/simple
```

For additional developer setup, please follow the [contributing guidelines](CONTRIBUTING.md).

## Usage
```
usage: ecmwf_download [-h] [-c {cds}] config

positional arguments:
  config                path/to/config.cfg, specific to the <client>. Accepts *.cfg and *.json files.
```
_Common options_: 
* `-c, --client`: Select the weather API client. The default is `cds` or Copernicus. More options
will be supported later.

Invoke with `-h` or `--help` to see the full range of options.

For further information on how to write config files, please consult [this documentation](Configuration.md).

## FAQ

### Q: Where does PubSub Fit in? 
We plan to use GCP's PubSub service to process [live ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/catalogue-ecmwf-real-time-products). Ideally, we'd like to plumb near-realtime data to BigQuery
via PubSub, if possible. We may need to add a Dataflow / Beam intermediary for this -- investigating is needed.

