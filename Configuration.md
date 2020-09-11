# Configuration Files

Configuration can manifest as `*.cfg` or `*.json` files. Each config file has two sections: A `parameters` section, which
configures the pipeline, and a `selection` section, which selects a subset of the data via the API client.

## `parameters` Section

Parameters for the pipeline.

* `dataset`: (required) Name of the target dataset. Allowed options are dictated by the client.
* `target_template`: (required) Download artifact filename template. Can use Python string format symbols. Must have the same number of format symbols as the number of partition keys.
* `partition_keys`: (required) This determines how download jobs will be divided. 
  * Value can be a single item or a list.
  * Each value must appear as a key in the `selection` section.
  * Each downloader will receive a config file with every parameter listed in the `selection`, _except_ for the fields specified by the `partition_keys`.
  * The downloader config will contain one instance of the cross-product of every key in `partition_keys`. 
    * E.g. `['year', 'month']` will lead to a config set like `[(2015, 01), (2015, 02), (2015, 03), ...]`.
  * The list of keys will be used to format the `target_template`.

## `selection` Section

Parameters used to select desired data. These will be passed as request parameters to the specified API client.

### Copernicus / CDN
For CDN parameter options, check out the [Copenicus documentation](https://cds.climate.copernicus.eu/cdsapp#!/search?type=dataset).
See [this example](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-pressure-levels?tab=form)
for what kind of requests one can make.

## Example

See [example config](example_config.cfg).

