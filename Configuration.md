# Configuration Files

Config files describe both _what_ to download and _how_ it should be downloaded. To this end, configs have two sections:
`selection` that describes the data desired from data-sources and `parameters` that define the details of the download.
By convention, the `parameters` section comes first.

Configuration files can be written in `*.cfg` or `*.json`, but typically, they're written in the former format (i.e.
Python's native config language, which is similar to INI format).

Before jumping into the details of each section, let's look at a few example configs.

## Examples

The following demonstrate how to download weather data from ECMWF's Copernicus (CDS) and Meteorological Archival and
Retrieval System (MARS) catalogues.

### Download Era5 Pressure Level Reanalysis from Copernicus

```
[parameters]
client=cds  ; choose a data source client
dataset=reanalysis-era5-pressure-levels  ; specify a dataset to download from the data source (CDS-specific)
target_path=gs://ecmwf-output-test/era5/{}/{}/{}-pressure-{}.nc  ; create a template for the output file path
partition_keys=  ; define how we should partition the download by the "keys" in the `selection` section.
    year         ; See docs below for more explanation
    month
    day
    pressure_level
[selection]
product_type=ensemble_mean
format=netcdf
variable=
    divergence
    fraction_of_cloud_cover
    geopotential
pressure_level=
    500
year=    ; we can specify a list of values using multiple lines
    2015
    2016
    2017
month=
    01
day=
    01
    15
time=
    00:00
    06:00
    12:00
    18:00
```

### Download Yesterday's Surface Temperatures from MARS.

```
[parameters]
client=mars  ; download from MARS (this is the default data source).
target_path=all.an  ; Download all the data from the selection section into one file.
[selection]
class   = od
type    = analysis
levtype = surface
date    = -1  ; Yesterday -- see link to MARS request syntax in docs linked below.
time    = 00/06/12/18  ; We can specify multiple values using `/` delimiters -- see MARS request syntax docs
param   = z/sp
```

## `parameters` Section

_Parameters for the pipeline_

These describe which data source to download, where the data should live, and how the download should be partitioned.

* `client`: (required) Select the weather API client. Supported values are `cds` for Copernicus, and `mars` for MARS.
* `dataset`: (optional) Name of the target dataset. Allowed options are dictated by the client.
* `target_path`: (required) Download artifact filename template. Can use Python string format symbols. Must have the
  same number of format symbols as the number of partition keys.
* `target_filename`: (optional) This file name will be appended to `target_path`.
    * Like `target_path`, `target_filename` can contain format symbols to be replaced by partition keys; if this is
      used, the total number of format symbols in both fields must match the number of partition keys.
    * This field is required when generating a date-based directory hierarchy (see below).
* `append_date_dirs`: (optional) A boolean indicating whether a date-based directory hierarchy should be created (see
  below); defaults to false if not used.
* `partition_keys`: (optional) This determines how download jobs will be divided.
    * Value can be a single item or a list.
    * Each value must appear as a key in the `selection` section.
    * Each downloader will receive a config file with every parameter listed in the `selection`, _except_ for the fields
      specified by the `partition_keys`.
    * The downloader config will contain one instance of the cross-product of every key in `partition_keys`.
        * E.g. `['year', 'month']` will lead to a config set like `[(2015, 01), (2015, 02), (2015, 03), ...]`.
    * The list of keys will be used to format the `target_path`.

### Creating a date-based directory hierarchy

The configuration can be set up to automatically generate a date-based directory hierarchy for the output files.

To enable this feature, the `append_date_dirs` field has to be set to `true`. In addition, the `target_filename` needs
to be specified, and `date` has to be a `partition_key`;
`date` will not be used as a replacement in `target_template` but will instead be used to create a directory structure.

The resulting target path will be `<target_path>/{year}/{month}/{day}<target_filename>`. The number of format symbols in
this path has to match the number of partition keys excluding `date`.

<details>
<summary><strong>Examples</strong></summary>

Below are more examples of how to use `target_path`, `target_filename`, and `append_date_dirs`.

Note that any parameters that are not relevant to the target path have been omitted.

```
[parameters]
target_filename=.nc
target_path=gs://ecmwf-output-test/era5/
append_date_dirs=true
partition_keys=
     date
[selection]
date=2017-01-01/to/2017-01-02
```

will create  
`gs://ecmwf-output-test/era5/2017/01/01.nc` and  
`gs://ecmwf-output-test/era5/2017/01/02.nc`.

```
[parameters]
target_filename=-pressure-{}.nc
target_path=gs://ecmwf-output-test/era5/
append_date_dirs=true
partition_keys=
     date
     pressure_level
[selection]
pressure_level=
    500
date=2017-01-01/to/2017-01-02
```

will create  
`gs://ecmwf-output-test/era5/2017/01/01-pressure-500.nc` and   
`gs://ecmwf-output-test/era5/2017/01/02-pressure-500.nc`.

```
[parameters]
target_filename=.nc
target_path=gs://ecmwf-output-test/pressure-{}/era5/
append_date_dirs=true
partition_keys=
     date
     pressure_level
[selection]
pressure_level=
    500
date=2017-01-01/to/2017-01-02
```

will create  
`gs://ecmwf-output-test/pressure-500/era5/2017/01/01.nc` and  
`gs://ecmwf-output-test/pressure-500/era5/2017/01/02.nc`.

The above example also illustrates how to create a directory structure based on partition keys, even without using the
date-based creation:

```
[parameters]
target_path=gs://ecmwf-output-test/era5/{}/{}/{}-pressure-{}.nc
partition_keys=
    year
    month
    day
    pressure_level
[selection]
pressure_level=
    500
year=
    2017
month=
    01
day=
    01
    02
```

will create  
`gs://ecmwf-output-test/era5/2017/01/01-pressure-500.nc` and  
`gs://ecmwf-output-test/era5/2017/01/02-pressure-500.nc`.

</details>

### Subsections

Sometimes, we'd like to alternate passing certain parameters to each client. For example, certain data sources have
limits on the number of API requests that can be made, enforcing a maximum per license. In these cases, the user can
specify a parameters subsection. The downloader will overwrite the base parameters with the key-value pairs in each
subsection, evenly alternating between each parameter set across the partitions.

To specify a subsection, create a new section with the following naming pattern: `[parameters.<subsection-name>]`.
The `<subsection-name>` can be any string, but it's recommended to chose a name that describes the grouping of values in
the section.

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

_Parameters used to select desired data_

These will be passed as request parameters to the specified API client. Selections are dependent on how each data
source's catalog is structured.

### Copernicus / CDS

**Catalog**: [https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset](https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset)

Visit the follow to register / acquire API credentials:
_[Install the CDS API key](https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key)_. After, please set
the `api_url` and `api_key` arguments in the `parameters` section of your configuration. Alternatively, one can set
these values as environment variables:

```shell
export CDSAPI_URL=$api_url
export CDSAPI_KEY=$api_key
```

For CDS parameter options, check out
the [Copernicus documentation](https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset).
See [this example](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=form)
for what kind of requests one can make.

### MARS

**Catalog**: [https://apps.ecmwf.int/archive-catalogue/](https://apps.ecmwf.int/archive-catalogue/)

Visit the following to register / acquire API credentials:
_[Install ECWMF Key](https://confluence.ecmwf.int/display/WEBAPI/Access+MARS#AccessMARS-key)_. After, please set
the `api_url`, `api_key`, and `api_email` arguments in the `parameters` section of your configuration. Alternatively,
one can set these values as environment variables:

```shell
export MARSAPI_URL=$api_url
export MARSAPI_EMAIL=$api_email
export MARSAPI_KEY=$api_key
```

For MARS parameter options, first read up on
[MARS request syntax](https://confluence.ecmwf.int/display/WEBAPI/Brief+MARS+request+syntax). For a full range of what
data can be requested, please consult the [MARS catalog](https://apps.ecmwf.int/archive-catalogue/).
See [these examples](https://confluence.ecmwf.int/display/UDOC/MARS+example+requests)
to discover the kinds of requests that can be made.

> **NOTE**: MARS data is stored on tape drives. It takes longer for multiple workers to request data than a single
> worker. Thus, it's recommended _not_ to set a partition key when writing MARS data configurations.
