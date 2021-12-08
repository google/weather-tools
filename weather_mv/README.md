# ⛅️ `weather-mv` – Weather Mover

Weather Mover loads weather data from cloud storage into Google BigQuery.

```
usage: weather-mv [-h] [-v variables [variables ...]] [-a area [area ...]] -i URIS -o OUTPUT_TABLE -t TEMP_LOCATION [--import_time IMPORT_TIME] [--infer_schema]

Weather Mover loads weather data from cloud storage into Google BigQuery.
```

_Common options_:

* `-i, --uris`: (required) URI prefix matching input netcdf objects. Ex: gs://ecmwf/era5/era5-2015-""
* `-o, --output_table`: (required) Full name of destination BigQuery table. Ex: my_project.my_dataset.my_table
* `-t, --temp_location`: Temp Location for staging files to import to BigQuery
* `--import_time`: When writing data to BigQuery, record that data import occurred at this time
  (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC.
* `-v, --variables`:  Target variables for the BigQuery schema. Default: will import all data variables as columns.
* `-a, --area`:  Target area in [N, W, S, E]. Default: Will include all available area.
* `--infer_schema`: Download one file in the URI pattern and infer a schema from that file. Default: off

Invoke with `-h` or `--help` to see the full range of options.
