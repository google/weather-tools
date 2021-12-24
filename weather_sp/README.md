# 🌪 `weather-sp` – Weather Splitter

Splits NetCDF and Grib files into several files by variable (_alpha_).

## Features

* **Format-Aware Processing**: Care is taken to ensure that each input file format is handled appropriately. For
  example, for Grib files, overlapping variables with different levels are kept separate. In addition, buckets with
  mixtures of NetCDF and Grib files can be processed on the same run.

## Usage

```
usage: weather-sp [-h] -i INPUT_PATTERN -o OUTPUT_DIR [-d]

Split weather data file into files by variable.
```

_Common options_:

* `-i, --input-pattern`: Pattern for input weather data.
* `-o, --output-dir`: Path to base folder for output files. This directory will replace the common path of the
  input-pattern. For `input-pattern a/b/c/**` and
  `output-dir /x/y/z` a file `a/b/c/file.nc` will createoutput files like `/x/y/z/c/file.nc_shortname.nc`
* `-d, --dry-run`: Test the input file matching and the output file scheme without splitting.

Invoke with `-h` or `--help` to see the full range of options.

_Usage examples_:

```bash
weather-sp --input-pattern 'gs://test-tmp/era5/2017/**' \
           --output-dir 'gs://test-tmp/era5/splits'
```

Preview splits with a dry run:

```bash
weather-sp --input-pattern 'gs://test-tmp/era5/2017/**' \
           --output-dir 'gs://test-tmp/era5/splits' \
           --dry-run
```

Using DataflowRunner

```bash
weather-sp --input-pattern 'gs://test-tmp/era5/2015/**' \
           --output-dir 'gs://test-tmp/era5/splits'
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

## Output

The base output path is specified using the `--output-path` flag. Based on that path, the directory structure of the
input pattern is replicated.

To create the directory structure, the common path of the input pattern is removed from the input file path and replaced
with the output path.

Example:

```bash
--input-pattern 'gs://test-input/era5/2020/**' \
--output-dir 'gs://test-output/splits'
```

For a file `gs://test-input/era5/2020/01/01.nc` the output file pattern is
`gs://test-output/splits/2020/01/01.nc` and if the temperature is a variable in that data, the output file for that
split will be `gs://test-output/splits/2020/01/01.nc_t.nc`

## Dry run

To verify the input file matching and the output naming scheme, `weather-sp` can be run with the `--dry-run` option.
This does not read the files, so it will not check whether the files are readable and in the correct format. It will
only list the input files with the corresponding output file schemes.
