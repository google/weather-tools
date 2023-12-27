# `xql` - Querying Xarray Datasets with SQL

Running SQL like quieries on Xarray Datasets. (Only works on zarr datasets for now.)

# Supported Features

* **Select Variables** - From a large dataset having hundreds of variables select only needed variables.
* **Apply Where Clouse** - A general where condition like SQL. Applicable for queries which includes data for specific time range or only for specific regions. (Conditions on coordinates suppoted for now.)
* **Group By And Aggregate Functions** - Aggregate functions `AVG()`, `MIN()`, `MAX()` suppoted after applying groupby on any coordinate like time.

# Usage

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
Replace `{TABLE}` with dataset uri. Ex. `gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3`:

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