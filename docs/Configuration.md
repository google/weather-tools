# Configuration Files

## `parameters` Section

Parameters for the pipeline and / or to pass to each worker. 

* `dataset`: (required) Name of the target dataset. Allowed options are dictated by the client.
* `target_template`: (required) Download artifact filename template. Can use Python string format symbols.
* `partition_keys`: (required) This determines how download jobs will be divided. 
  * Value can be a single item or a list.
  * Each value must appear as a key in the `selection` section.
  * Each downloader will receive a config file with every parameter listed in the `selection`, _except_ for the fields specified by the `partition_keys`.
  * The downloader config will contain one instance of the cross-product of every key in `partition_keys`. 
    * E.g. `['year', 'month']` will lead to a config set like `[(2015, 01), (2015, 02), (2015, 03), ...]`.
  * The list of keys will be used to format the `target_template`.
  
TODO(alxr): Revise

## `selection` Section

Parameters used to select desired data. These will be passed as request parameters to the specified API client.

### Copernicus / CDN
For CDN parameter options, check out the [Copenicus documentation](https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset).
See [this example](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=form)
for what kind of requests one can make.

## Example

`my_config.cfg`
```
[parameters]
dataset=reanalysis-era5-pressure-levels  ; required
target_template=download-{}.nc           ; required. Will be download artifact template. 
                                         ; Can use Python string format symbols.
partition_keys=year                      ; required. Can be a list
[selection]
product_type=ensemble_mean
format=netcdf
variable=                                ; Multi-line values in *.cfg files are treated as lists.
    u_component_of_wind
    v_component_of_wind
    temperatore
pressure_level=
    500
    1000
month=
    01
    04
    07
    10
year=
    2015
    2016
    2017
    2018
    2019
time=
    00:00
    06:00
    12:00
    18:00
```



