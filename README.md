# ECMWF-pipeline

[Build Status](https://pantheon.corp.google.com/cloud-build/dashboard?project=grid-intelligence-sandbox)

## Goals

Goal: Create a pipeline to make [ECMWF](https://www.ecmwf.int/) data available to all teams at Alphabet.

Milestone 1: Load a subset of [historical ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/archive-datasets) needed for [DeepMind's wind energy-related forecasting](https://deepmind.com/blog/article/machine-learning-can-boost-value-wind-energy).
- [ ] Use MARs API to download ECMWF's HRES forecasts
- [ ] Download ECMWF's ENS forecasts
- [ ] Pipe downloaded data into BigQuery for general use


## Project Structure 
    
```
docs/  # equivalent of g3docs folder

ecmwf_pipeline/  # also could be `src/`, but we prefer the python convetion of a library name.
    __init__.py
    ...  # Other needed files in the library
    <module>.py
    <module>_test.py  # We'll follow this naming pattern for tests
    
setup.py  # Project should be pip-installable, requirements managed here.

notebooks/  # Explorations / investigations
```


## Installing

ECMWF-pipeline can be installed via source code:
```
gcloud init
gcloud source repos clone ecmwf-pipeline --project=grid-intelligence-sandbox
cd ecmwf-pipeline
pip install .
```

For additional developer setup, please follow the [contributing guidelines](/CONTRIBUTING.md).

## Usage

TODO(AWG)

## FAQ

### Q: Where does PubSub Fit in? 
We plan to use GCP's PubSub service to process [live ECMWF data](https://www.ecmwf.int/en/forecasts/datasets/catalogue-ecmwf-real-time-products). Ideally, we'd like to plumb near-realtime data to BigQuery
via PubSub, if possible. We may need to add a Dataflow / Beam intermediary for this -- investigating is needed.

