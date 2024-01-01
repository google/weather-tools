# `xql` - Querying Xarray Datasets with SQL

Running SQL like queries on Xarray Datasets.
> Note: For now, we support only zarr datasets.

# Supported Features

* **`Select` Variables** - From a large dataset having hundreds of variables select only needed variables.
* **Apply `where` clause** - A general where condition like SQL. Applicable for queries which includes data for specific time range or only for specific regions. 
* > Note: For now, we support conditions on coordinates.
* **`group by` and `aggregate` Functions** - Aggregate functions `AVG()`, `MIN()`, `MAX()` supported after applying `group-by` on any coordinate like time.

# Quickstart

## Prerequisites

Get an access to the dataset you want to query. As an example we're using the analysis ready era5 public dataset. [full_37-1h-0p25deg-chunk-1.zarr-v3](https://pantheon.corp.google.com/storage/browser/gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3?project=gcp-public-data-signals).

For this gcloud must be configured in the environment. [Initializing the gcloud CLI](https://cloud.google.com/sdk/docs/initializing).

## Usage

Install required packages
```
pip install -r xql/requirements.txt
```

Jump into xql
```
python xql/main.py
```
---

Running a simple query on dataset. Comparing with SQL a data variable is like a column and table is like a dataset.
```
SELECT evaporation, geopotential_at_surface, temperature FROM '{TABLE}'
```
Replace `{TABLE}` with dataset uri. Eg. `gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3`.

---
Apply a conditions. Query to get temperature of arctic region in last winter:
```
SELECT temperature FROM '{TABLE}' WHERE time >= '2022-12-01' AND time < '2023-03-01' AND latitude >= 66.5
```
---
Aggregating results using Group By and Aggregate function. Daily average of temperature of last winter in arctic region.
```
SELECT AVG(temperature) FROM '{TABLE}' WHERE time >= '2022-12-01' AND time < '2023-03-01' AND latitude >= 66.5
GROUP BY time_day
```
Replace `time_day` to `time_month` or `time_year` if monthly or yearly average is needed. Also use MIN() and MAX() functions same way as AVG().