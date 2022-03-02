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
* `--output-template`: Path to template using Python formatting, see [Output section](#output) below. Mutually exclusive
  with `--output-dir`.
* `--output-dir`: Path to base folder for output files, see [Output section](#output) below. Mutually exclusive
  with `--output-template`
* `-f, --force`: Force re-splitting of the pipeline. Turns of skipping of already split data.
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

The base output file names are specified using the `--output-template` or `--output-dir` flags. These flags are mutually
exclusive, and one of them is required.

### Output directory

Based on the output directory path, the directory structure of the input pattern is replicated.

To create the directory structure, the common path of the input pattern is removed from the input file path and replaced
with the output path. A '_' is added to separate the split level / variable.

Example:

```bash
--input-pattern 'gs://test-input/era5/2020/**' \
--output-dir 'gs://test-output/splits'
```

For a file `gs://test-input/era5/2020/02/01.nc` the output file pattern is
`gs://test-output/splits/2020/02/01.{shortname}.nc` and if the temperature is a variable in that data, the output file
for that split will be `gs://test-output/splits/2020/02/01.t.nc`

### Output template with Python-style formatting

Using Python-style substitution (e.g. `{1}`) allows for more flexibility when creating the output files. The
substitutions are based on the directory structure of the input file, where each `{<x>}` stands for one directory name,
counting backwards from the end, i.e. the file name is `{0}`, the immediate directory in which it is located is `{1}`,
and so on. In addition, you may supply `{shortname}` or `{levelType}` in the output template. These will be filled by values
found in each file (NetCDF files typically only have one type of level, so this variable is not needed).

Example:

```bash
--input-pattern 'gs://test-input/era5/2020/**' \
--output-template 'gs://test-output/splits/{2}.{0}.{1}T00.{shortname}.nc'
```

For a file `gs://test-input/era5/2020/02/01.nc` the output file pattern is
`gs://test-output/splits/2020.01.02T00.{shortname}.nc` and if the temperature is a variable in that data, the output
file for that split will be `gs://test-output/splits/2020.01.02T00.t.nc`

## Dry run

To verify the input file matching and the output naming scheme, `weather-sp` can be run with the `--dry-run` option.
This does not read the files, so it will not check whether the files are readable and in the correct format. It will
only list the input files with the corresponding output file schemes.
