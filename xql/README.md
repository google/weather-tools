# `xql` - Querying Xarray Datasets with SQL

Running SQL like queries on Xarray Datasets. Consider dataset as a table and data variable as a column.
> Note: For now, we support only zarr datasets.

# Supported Features

* **`Select` Variables** - From a large dataset having hundreds of variables select only needed variables.
* **Apply `where` clause** - A general where condition like SQL. Applicable for queries which includes data for specific time range or only for specific regions. 
* **`group by` Functions** - This is supported on the coordinates only. e.g. time, latitude, longitude, etc.
* **`aggregate` Functions** - Aggregate functions `AVG()`, `MIN()`, `MAX()`, etc. Only supported on data variables.
* For more checkout the [road-map](https://github.com/google/weather-tools/tree/xql-init/xql#roadmap).
> Note: For now, we support `where` conditions on coordinates only. 

> Note: For now, variable with starting from the numbers(0-9) in aggregation function is not supported.

# Quickstart

## Prerequisites

Get an access to the dataset you want to query. Here as an example we're going to use the analysis ready era5 public dataset. [full_37-1h-0p25deg-chunk-1.zarr-v3](https://pantheon.corp.google.com/storage/browser/gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3?project=gcp-public-data-signals).

For this `gcloud` must be configured in your local environment. Refer [Initializing the gcloud CLI](https://cloud.google.com/sdk/docs/initializing) for configuring the `gcloud` locally.

## Usage

```
# Install required packages
pip install -r xql/requirements.txt

# Jump into xql
python xql/main.py
```
---
### Supported meta commands
`.help`: For usage info.

`.exit`: To exit from the xql interpreter.

`.set`: To set the dataset uri as a shortened key.
```
.set era5 gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3
```

`.show`: To list down dataset shortened key. Eg. `.show` or `.show [key]`

```
.show era5
```

`[query]`  =>  Any valid sql like query.

---
### Example Queries

1. Apply a conditions. Query to get temperature of arctic region in January 2022:
    ```
    SELECT 
        temperature 
    FROM 'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3' 
    WHERE
        time >= '2022-01-01' AND 
        time < '2022-02-01' AND 
        latitude >= 66.5
    ```
    > Note: Multiline queries are not yet supported. Convert copied queries into single line before execution.

2. Aggregating results using Group By and Aggregate function. Daily average of temperature of arctic region in January 2022.
    Setting the table name as shortened key.

    ```
    .set era5 gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3
    ```
    ```
    SELECT 
        AVG(temperature), SUM(charnock), MIN(temperature) 
    FROM era5
    WHERE 
        time >= '2022-01-01' AND 
        time < '2022-02-01' AND 
        latitude >= 66.5
    GROUP BY time_day
    ```
    Replace `time_day` to `time_month` or `time_year` if monthly or yearly average is needed. Also use `MIN()` and `MAX()` functions same way as `AVG()`.

3. `caveat`: Above queries run on the client's local machine and it generates a large two dimensional array so querying for very large amount of data will fall into out of memory erros.

    e.g. Query like below will give OOM errors if the client machine don't have the enough RAM.

    ```
    SELECT 
        evaporation,
        geopotential_at_surface,
        temperature 
    FROM era5
    ```

# Roadmap

_Updated on 2024-01-08_

1. [x] **Select Variables**
    1. [ ] On Coordinates
    2. [x] On Variables 
2. [x] **Where Clause**: `=`, `>`, `>=`, `<`, `<=`, etc.
    1. [x] On Coordinates
    2. [ ] On Variables 
3. [x] **Aggregate Functions**: Only `AVG()`, `MIN()`, `MAX()`, `SUM()` are supported.
   1. [x] With Group By
   2. [x] Without Group By
   3. [x] Multiple Aggregate function in a single query
4. [ ] **Order By**: Only suppoted for coordinates.
5. [ ] **Limit**: Limiting the result to display.
6. [ ] **Mathematical Operators** `(+, - , *, / )`: Add support to use mathematical operators in the query.
7. [ ] **Aliases**: Add support to alias while querying.
8. [ ] **Join Operations**: Support joining tables and apply query.
9. [ ] **Nested Queries**: Add support to write nested queries.
10. [ ] **Custom Aggregate Functions**: Support custom aggregate functions