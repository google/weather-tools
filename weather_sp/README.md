# Weather data splitter

Splits netcdf and grib files into several files by variable.

Usage example:

```bash
weather-splitter --input_pattern 'gs://test-tmp/era5/2017/**'
```
Using DirectflowRunner
```bash
weather-splitter --input_pattern 'gs://test-tmp/era5/2015/**' \
                 --runner DataflowRunner \
                 --project grid-intelligence-sandbox \
                 --temp_location gs://<temp folder>  \
                 --job_name $JOB_NAME
```

## Specifying input files
The input file pattern matching is done by the Apache beam filesystems module,
see https://beam.apache.org/releases/pydoc/2.12.0/apache_beam.io.filesystems.html#apache_beam.io.filesystems.FileSystems.match
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
Split files will be placed in a sub-folder called `/split_files/`; files in that
folder will be ignored if the script is run again.

... to be continued...