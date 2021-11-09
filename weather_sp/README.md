# Weather data splitter

Splits netcdf and grib files into several files by variable.

Usage example:

```bash
weather-splitter --uris 'gs://test-tmp/era5/2017'
```

All files below that path will be split.

Split files will be placed in a sub-folder called `/split_files/`; files in that
folder will be ignored if the script is run again.

... to be continued...