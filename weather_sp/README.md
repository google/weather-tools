# Weather data splitter (`weather-sp`)

Splits netcdf and grib files into several files by variable.

Usage example:

```bash
weather-splitter --input_pattern 'gs://test-tmp/era5/2017/**' \
                 --output_dir 'gs://test-tmp/era5/splits'
```

Using DataflowRunner

```bash
weather-splitter --input_pattern 'gs://test-tmp/era5/2015/**' \
                 --output_dir 'gs://test-tmp/era5/splits'
                 --runner DataflowRunner \
                 --project grid-intelligence-sandbox \
                 --temp_location gs://<temp folder>  \
                 --job_name $JOB_NAME
```

## Specifying input files

The input file pattern matching is done by the Apache beam filesystems module,
see [this documentation](https://beam.apache.org/releases/pydoc/2.12.0/apache_beam.io.filesystems.html#apache_beam.io.filesystems.FileSystems.match)
under 'pattern syntax'

Notably, `**` Is equivalent to `.*`, so to match all files in a directory, use

```bash
--input_pattern 'gs://test-tmp/era5/2017/**'
```

On the other hand, to specify a specific pattern use

```bash
--input_pattern 'gs://test-tmp/era5/2017/*/*.nc'
```

## Output

The base output path is specified using the `--output_path` flag. Based on that path, the directory structure of the
input pattern is replicated.

To create the directory structure, the common path of the input pattern is removed from the input file path and replaced
with the output path.

Example:

```bash
--input_pattern 'gs://test-input/era5/2020/**' \
--output_dir 'gs://test-output/splits'
```

For a file `gs://test-input/era5/2020/01/01.nc` the output file pattern is
`gs://test-output/splits/2020/01/01.nc` and if the temperature is a variable in that data, the output file for that
split will be `gs://test-output/splits/2020/01/01.nc_t.nc`

## Dry run

To verify the input file matching and the output naming scheme, weather-splitter can be run with the `--dry-run` option.
This does not read the files, so it will not check whether the files are readable and in the correct format. It will
only list the input files with the corresponding output file schemes.
