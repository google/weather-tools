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
* `partition_keys`: (optional) This determines how download jobs will be divided.
    * Value can be a single item or a list.
    * Each value must appear as a key in the `selection` section.
    * Each downloader will receive a config file with every parameter listed in the `selection`, _except_ for the fields
      specified by the `partition_keys`.
    * The downloader config will contain one instance of the cross-product of every key in `partition_keys`.
        * E.g. `['year', 'month']` will lead to a config set like `[(2015, 01), (2015, 02), (2015, 03), ...]`.
    * The list of keys will be used to format the `target_path`.

> **NOTE**: `target_path` template is totally compatible with Python's standard string formatting.
> This includes being able to use named arguments (e.g. 'gs://bucket/{year}/{month}/{day}.nc') as well as specifying formats for strings 
> (e.g. 'gs://bucket/{year:04d}/{month:02d}/{day:02d}.nc').

### Creating a date-based directory hierarchy

The date-based directory hierarchy can be created using Python's standard string formatting.
Below are some examples of how to use `target_path` with Python's standard string formatting.

<details>
<summary><strong>Examples</strong></summary>

Note that any parameters that are not relevant to the target path have been omitted.

```
[parameters]
target_path=gs://ecmwf-output-test/era5/{date:%%Y/%%m/%%d}.nc
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
target_path=gs://ecmwf-output-test/era5/{date:%%Y/%%m/%%d}-pressure-{pressure_level}.nc
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
target_path=gs://ecmwf-output-test/pressure-{pressure_level}/era5/{date:%%Y/%%m/%%d}.nc
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

```
[parameters]
target_path=gs://ecmwf-output-test/era5/{year:04d}/{month:02d}/{day:02d}-pressure-{pressure_level}.nc
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

> **Note**: Replacing the `target_path` of the above example with this `target_path=gs://ecmwf-output-test/era5/{year}/{month}/{day}-pressure-
>{pressure_level}.nc`
>
> will create
>
> `gs://ecmwf-output-test/era5/2017/1/1-pressure-500.nc` and  
> `gs://ecmwf-output-test/era5/2017/1/2-pressure-500.nc`.

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

**License**: By using Copernicus / CDS Dataset, users agree to the terms and conditions specified in dataset. i.e. [License](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=download#manage-licences).

**Catalog**: [https://cds.climate.copernicus.eu/datasets](https://cds.climate.copernicus.eu/datasets)

Visit the follow to register / acquire API credentials:
_[Install the CDS API key](https://cds.climate.copernicus.eu/how-to-api)_. After, please set
the `api_url` and `api_key` arguments in the `parameters` section of your configuration. Alternatively, one can set
these values as environment variables:

```shell
export CDSAPI_URL=$api_url
export CDSAPI_KEY=$api_key
```

For CDS parameter options, check out
the [Copernicus documentation](https://cds.climate.copernicus.eu/datasets).
See [this example](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels?tab=overview)
for what kind of requests one can make.

### MARS

**License**: By using MARS Dataset, users agree to the terms and conditions specified in [License](https://www.ecmwf.int/en/forecasts/accessing-forecasts/licences-available) document.

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

**Supported Date Formats**: Weather-DL supports multiple date input formats, ranging from a single date/time to an entire month or year. The table below summarizes the various supported input formats.

<table>
  <colgroup>
    <col style="width:15%">
    <col style="width:35">
    <col style="width:25%">
    <col style="width:25%">
  </colgroup>
  <thead>
    <tr>
      <td><h5>Keyword</h5></td>
      <td><h5>Description</h5></td>
      <td><h5>Sample Config</h5></td>
      <td><h5>Output Partitions</h5></td>
    </tr>
    <tr>
      <!-- https://github.com/google/weather-tools/pull/528 -->
      <td>(no partition keys specified)</td>
      <td>Introduced support for creating a single output-file when no partition_keys are specified, ensuring that all data is written into one consolidated file.</td>
      <td>[parameters]<br/>target_path=data.nc<br/>[selection]<br/>format=netcdf<br/>date=2017-01-01<br/>time=00/06/12/18</td>
      <td>data.nc</td>
    </tr>
    <tr>
      <!-- https://github.com/google/weather-tools/pull/525 -->
      <td>date_range</td>
      <td>Added support for specifying one or more date-range values, enabling users to download data across multiple date intervals in a single run.<br/><br/>Note: partition_key must be specified.</td>
      <td>[parameters]<br/>target_path={date_range}.nc<br/>[selection]<br/>format=netcdf<br/>date_range=<br/>2017-01-01/to/2017-01-10<br/>2017-01-21/to/2017-01-31<br/>time=00/06/12/18</td>
      <td>2017-01-01_to_2017-01-10.nc<br/>2017-01-21_to_2017-01-31.nc</td>
    </tr>
    <tr>
      <!-- https://github.com/google/weather-tools/pull/529 -->
      <td>year</td>
      <td>Added ability to accept and process multiple month & multiple year inputs in configuration, enabling users to specify broad time-spans without listing each period separately.</td>
      <td>
        <h5>Case-1</h5>target_path={year}.nc<br/>partition_keys=year<br/>[selection]<br/>format=netcdf<br/>year=2001/to/2003<br/>month=1/to/12<br/>day=all<br/>
        <h5>Case-2</h5>target_path={year}.nc<br/>partition_keys=year<br/>[selection]<br/>format=netcdf<br/>year=2001/to/2003<br/>month=2/to/8/by/2<br/>day=all<br/>
        <h5>Case-3</h5>target_path={year}.nc<br/>partition_keys=year<br/>[selection]<br/>format=netcdf<br/>year=2001/to/2003<br/>month=1/4/11<br/>day=all<br/>
      </td>
      <td>
        <h5>Case-1</h5>2001.nc<br/>2002.nc<br/>2003.nc<br/>
        <h5>Case-2</h5>2001.nc<br/>2002.nc<br/>2003.nc<br/>
        <h5>Case-3</h5>2001.nc<br/>2002.nc<br/>2003.nc<br/>
      </td>
    </tr>
    <tr>
      <!-- https://github.com/google/weather-tools/issues/262 -->
      <!-- https://github.com/google/weather-tools/issues/334 -->
      <!-- https://github.com/google/weather-tools/blob/main/configs/s2s_operational_forecast_example.cfg -->
      <!-- https://critique.corp.google.com/cl/748174885/depot/google3/research/weather/anthromet/download_configs/enstrophy/ifs-ext-reforecast-pressure-levels-inst-all-levs.cfg -->
      <td>hdate</td>
      <td>This parameter allows weather‑dl to explicitly specify historical target dates for downloads, giving users precise control over which past dates are retrieved.</td>
      <td>
        <h5>Case-1</h5>target_path={date}.gb<br/>[selection]<br/>date=2020-01-02<br/>hdate=1/to/3<br/>
        <h5>Case-2</h5>target_path={date}.gb<br/>[selection]<br/>date=2016-01-04/2016-01-07<br/>hdate=1/to/2<br/>
      </td>
      <td>
        <h5>Case-1</h5>2019-01-02<br/>2018-01-02<br/>2017-01-02<br/>
        <h5>Case-2</h5><br/>
      </td>
    </tr>
    <tr>
      <!-- https://github.com/google/weather-tools/pull/503 -->
      <td>year-month</td>
      <td>Added support for a year‑month key, allowing users to specify downloads using month granularity (e.g., “2025‑09”) instead of only daily or ranged formats.</td>
      <td>target_path={year-month}.gb<br/>year-month=2024-10/to/2024-12</td>
      <td>2024-10.gb<br/>2024-11.gb<br/>2024-12.gb</td>
    </tr>
  </thead>
</table>