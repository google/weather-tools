# 🌪 `weather-sp` – Weather Splitter

Splits NetCDF and Grib files into several files by variable (_alpha_).

## Features

* **Format-Aware Processing**: Care is taken to ensure that each input file format is handled appropriately. For
  example, for Grib files, overlapping variables with different levels are kept separate. In addition, buckets with
  mixtures of NetCDF and Grib files can be processed on the same run.

## Usage

```
usage: weather-sp [-h] -i INPUT_PATTERN (--output-template OUTPUT_TEMPLATE | --output-dir OUTPUT_DIR) [--formatting FORMATTING] [-d] [-f]
```

_Common options_:

* `-i, --input-pattern`: Pattern for input weather data.
* `--output-template`: Template of output file path using Python formatting, see [Output section](#output) below. Mutually exclusive
  with `--output-dir`.
* `--output-dir`: Path to base folder for output files, see [Output section](#output) below. Mutually exclusive
  with `--output-template`
* `--formatting`: Used only with `--output-dir` to specify splitting and output format
   using Python formatting, see [Output section](#output) below.
* `-f, --force`: Force re-splitting of the pipeline. Turns of skipping of already split data.
* `-d, --dry-run`: Test the input file matching and the output file scheme without splitting.

Invoke with `-h` or `--help` to see the full range of options.

_Usage examples_:

```bash
weather-sp --input-pattern 'gs://test-tmp/era5/2017/**' \
           --output-dir 'gs://test-tmp/era5/splits'
           --formatting '.{typeOfLevel}'
```

Preview splits with a dry run:

```bash
weather-sp --input-pattern 'gs://test-tmp/era5/2017/**' \
           --output-dir 'gs://test-tmp/era5/splits' \
           --formatting '.{typeOfLevel}'
           --dry-run
```

Using DataflowRunner

```bash
weather-sp --input-pattern 'gs://test-tmp/era5/2015/**' \
           --output-dir 'gs://test-tmp/era5/splits'
           --formatting '.{typeOfLevel}'
           --runner DataflowRunner \
           --project $PROJECT \
           --temp_location gs://$BUCKET/tmp  \
           --job_name $JOB_NAME
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Specifying input files

The input file pattern matching is done by the Apache Beam filesystems module,
see [this documentation](https://beam.apache.org/releases/pydoc/2.12.0/apache_beam.io.filesystems.html#apache_beam.io.filesystems.FileSystems.match)
under 'pattern syntax'

Notably, `**` Is equivalent to `.*`, so to match all files in a directory, use

```bash
--input-pattern 'gs://test-tmp/era5/2017/**'
```

On the other hand, to specify a specific pattern use

```bash
--input-pattern 'gs://test-tmp/era5/2017/*/*.nc'
```

## Output & Split Dimensions

The base output file names are specified using the `--output-template` or `--output-dir` flags. These flags are mutually
exclusive, and one of them is required. \
The output formatting also specifies which dimensions to split by using Python formatting.
For example, adding `{time}` in the output template or formatting will cause the file to be split along the time dimension.

### Available dimensions to split

How a file can be split depends on the file type.

#### GRIB
GRIB files can be split along any dimensions that is available in the file's metadata. \
Examples: 'typeOfLevel', 'level', 'step', 'shortName', 'gridType', 'time', 'forecastTime' \
Any available dimensions can be combined when splitting.

#### NetCDF
NetCDF files are already in a hypercube format and can only be split by one of the dimensions and by data variable.
Since splitting by latitude or longitude would lead to a large number of small files, this is not supported,
and it is recommended to use the `weather-mv` tool instead. \
Supported splits for NetCDF files are thus 'variable' to split by data variable, and any dimension 
other than latitude and longitude.


### Output directory

Based on the output directory path, the directory structure of the input pattern is replicated.

To create the directory structure, the common path of the input pattern is removed from the input file path and replaced
with the output path. \
The formatting specified by the `--formatting` flag is added between file name and ending and is
used to determine along which dimensions to split.

Example:

```bash
--input-pattern 'gs://test-input/era5/2020/**' \
--output-dir 'gs://test-output/splits'
--formatting '.{variable}'
```

For a file `gs://test-input/era5/2020/02/01.nc` the output file pattern is
`gs://test-output/splits/2020/02/01.{variable}.nc` and if the temperature is a variable in that data, the output file
for that split will be `gs://test-output/splits/2020/02/01.t.nc`.

Example:

```bash
--input-pattern 'gs://test-input/era5/2020/**' \
--output-dir 'gs://test-output/splits'
--formatting '_{date}:{time}_{level}hPa'
```
For a file `gs://test-input/era5/2020/02/01.grib` the output file pattern is
`gs://test-output/splits/2020/02/01_{date}:{time}_{level}hPa.grib` and for date = 20200101, time = 1200, level = 400,
 the output file for that split will be `gs://test-output/splits/2020/02/01_20200101:1200_400hPa.grib`.


### Output template with Python-style formatting

Using Python-style substitution (e.g. `{1}`) allows for more flexibility when creating the output files. The
substitutions are based on the directory structure of the input file, where each `{<x>}` stands for one directory name,
counting backwards from the end, i.e. the file name is `{0}`, the immediate directory in which it is located is `{1}`,
and so on. In addition, you need to supply the split dimensions in the output template. These will be filled by values
found in each file.

Example:

```bash
--input-pattern 'gs://test-input/era5/2020/**' \
--output-template 'gs://test-output/splits/{2}.{0}.{1}T00.{variable}.nc'
```

For a file `gs://test-input/era5/2020/02/01.nc` the output file pattern is
`gs://test-output/splits/2020.01.02T00.{variable}.nc` and if the temperature is a variable in that data, the output
file for that split will be `gs://test-output/splits/2020.01.02T00.t.nc`

## Dry run

To verify the input file matching and the output naming scheme, `weather-sp` can be run with the `--dry-run` option.
This does not read the files, so it will not check whether the files are readable and in the correct format. It will
only list the input files with the corresponding output file schemes.
