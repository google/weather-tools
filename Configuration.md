# Configuration Files

Configuration can manifest as `*.cfg` or `*.json` files. Each config file has two sections: A `parameters` section, which
configures the pipeline, and a `selection` section, which selects a subset of the data via the API client.

## `parameters` Section

Parameters for the pipeline.

* `dataset`: (optional) Name of the target dataset. Allowed options are dictated by the client.
* `client`: (required) Select the weather API client. Supported values are `cds` for Copernicus, and `mars` for MARS.
* `target_template`: (required) Download artifact filename template. Can use Python string format symbols. Must have the same number of format symbols as the number of partition keys.
* `partition_keys`: (optional) This determines how download jobs will be divided. 
  * Value can be a single item or a list.
  * Each value must appear as a key in the `selection` section.
  * Each downloader will receive a config file with every parameter listed in the `selection`, _except_ for the fields specified by the `partition_keys`.
  * The downloader config will contain one instance of the cross-product of every key in `partition_keys`. 
    * E.g. `['year', 'month']` will lead to a config set like `[(2015, 01), (2015, 02), (2015, 03), ...]`.
  * The list of keys will be used to format the `target_template`.
  
### Subsections

Sometimes, we'd like to alternate passing certain parameters to each client. For example, certain data sources have 
limits on the number of API requests that can be made, enforcing a maximum per license. In these cases, the user can 
specify a parameters subsection. The downloader will overwrite the base parameters with the key-value pairs in each 
subsection, evenly alternating between each parameter set across the partitions.

To specify a subsection, create a new section with the following naming pattern: `[parameters.<subsection-name>]`.
The `<subsectiona-name>` can be any string, but it's recommended to chose a name that describes the grouping of values 
in the section.

Here's an example of this type of configuration: 
```
[parameters]
dataset=ecmwf-mars-output
target_template=gs://ecmwf-downloads/hres-single-level/{}.nc
partition_keys=
    date
[parameters.deepmind]
api_key=KKKKK1
api_url=UUUUU1
[parameters.research]
api_key=KKKKK2
api_url=UUUUU2
[parameters.cloud]
api_key=KKKKK3
api_url=UUUUU3
```

## `selection` Section

Parameters used to select desired data. These will be passed as request parameters to the specified API client.

### Copernicus / CDS
Visit the follow to register / acquire API credentials: 
_[Install the CDS API key](https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key)_.
After, please set the `api_url` and `api_key` arguments in the `parameters` section of your configuration.
Alternatively, one can set these values as environment variables: 
```shell script
export CDSAPI_URL=$api_url
export CDSAPI_KEY=$api_key
```

For CDS parameter options, check out the [Copernicus documentation](https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset).
See [this example](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=form)
for what kind of requests one can make.

### MARS

Visit the following to register / acquire API credentials: 
_[Install ECWMF Key](https://confluence.ecmwf.int/display/WEBAPI/Access+MARS#AccessMARS-key)_. After, please set
the `api_url`, `api_key`, and `api_email` arguments in the `parameters` section of your configuration.
Alternatively, one can set these values as environment variables: 
```shell script
export ECMWF_API_URL=$api_url
export ECMWF_API_EMAIL=$api_email
export ECMWF_API_KEY=$api_key
```
 
For MARS parameter options, first read up on 
[request syntax](https://confluence.ecmwf.int/display/WEBAPI/Brief+MARS+request+syntax). For a full range of what data 
can be requested, please consult the [MARS catalog](https://apps.ecmwf.int/archive-catalogue/).
See [these examples](https://confluence.ecmwf.int/display/UDOC/MARS+example+requests) 
to discover the kinds of requests that can be made.

> **NOTE**: MARS data is stored on tape drives. It takes longer for multiple workers to request data than a single 
> worker. Thus, it's recommended _not_ to set a partition key when writing MARS data configurations.

## Example

See [a CDS example config](configs/era5_example_config.cfg) or [a MARS example config](configs/yesterdays_surface_example.cfg).

