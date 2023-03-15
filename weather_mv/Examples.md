# Weather Mover Examples

## BigQuery


Using the subcommand alias `bq`:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2
```

Preview load with a dry run:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --dry-run
```

Load COG's (.tif) files:

```bash
weather-mv bq --uris "gs://your-bucket/*.tif" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --temp_location "gs://$BUCKET/tmp" \  # Needed for batch writes to BigQuery
           --direct_num_workers 2 \
           --tif_metadata_for_datetime start_time
```

Upload only a subset of variables:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --variables u10 v10 t
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Upload all variables, but for a specific geographic region (for example, the
continental US):

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --area 49 -124 24 -66 \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Control how weather data is opened with XArray:

```bash
weather-mv bq --uris "gs://your-bucket/*.grib" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --xarray_open_dataset_kwargs '{"engine": "cfgrib", "indexpath": "", "backend_kwargs": {"filter_by_keys": {"typeOfLevel": "surface", "edition": 1}}}' \
           --temp_location "gs://$BUCKET/tmp" \
           --direct_num_workers 2
```

Using DataflowRunner:

```bash
weather-mv bq --uris "gs://your-bucket/*.nc" \
           --output_table $PROJECT.$DATASET_ID.$TABLE_ID \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --job_name $JOB_NAME 
```

For a full list of how to configure the Dataflow pipeline, please review
[this table](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

## Regrid

Using the subcomand alias 'rg':

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" 
 
```

Preview regrid with a dry run:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --dry-run
 
```

Interpolate to a finer grid resolution:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --regrid_kwargs '{"grid": [0.1, 0.1]}'.
 
```

Interpolate to a high-resolution octahedral gaussian grid:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --regrid_kwargs '{"grid": "O1280}'.
 
```

Convert gribs to NetCDF on copy:

```bash
weather-mv rg --uris "gs://your-bucket/*.gb" \
           --output_path "gs://regrid-bucket/" \
           --to_netcdf
```

Using DataflowRunner:

```bash
weather-mv rg --uris "gs://your-bucket/*.nc" \
           --output_path "gs://regrid-bucket/" \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --experiment=use_runner_v2 \
           --sdk_container_image="gcr.io/$PROJECT/$REPO:latest"  \
           --job_name $JOB_NAME 
```

Using DataflowRunner, with added disk per VM:

```bash
weather-mv rg --uris "gs://your-bucket/*.nc" \
           --output_path "gs://regrid-bucket/" \
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --disk_size_gb 250 \ 
           --temp_location "gs://$BUCKET/tmp" \
           --experiment=use_runner_v2 \
           --sdk_container_image="gcr.io/$PROJECT/$REPO:latest"  \
           --job_name $JOB_NAME 
```

## Earth Engine

Using the subcommand alias `ee`:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
```

Preview ingestion with a dry run:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --dry-run
```

Authenticate earth engine using personal account:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --use_personal_account
```

Authenticate earth engine using a private key:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --service_account "my-service-account@...gserviceaccount.com" \
           --private_key "path/to/private_key.json"
```

Ingest asset as table in earth engine:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --ee_asset_type "TABLE"
```

Restrict merging all bands or grib normalization:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --disable_grib_schema_normalization
```

Control how weather data is opened with XArray:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
           --xarray_open_dataset_kwargs '{"engine": "cfgrib", "indexpath": "", "backend_kwargs": {"filter_by_keys": {"typeOfLevel": "surface", "edition": 1}}}' \
           --temp_location "gs://$BUCKET/tmp"
```

Limit EE requests:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --ee_qps 10 \
           --ee_latency 0.5 \
           --ee_max_concurrent 10
```

Custom Band names:

```bash
weather-mv ee --uris "gs://your-bucket/*.tif" \
           --asset_location "gs://$BUCKET/assets" \ # Needed to store assets generated from *.tif
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --band_names_mapping "filename.json"
```

Getting initialization and forecast/end date-time from the filename:

```bash
weather-mv ee --uris "gs://your-bucket/*.tif" \
           --asset_location "gs://$BUCKET/assets" \ # Needed to store assets generated from *.tif
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --initialization_time_regex "$REGEX" \
           --forecast_time_regex "$REGEX"
```

Example:

```bash
weather-mv ee --uris "gs://tmp-gs-bucket/3B-HHR-E_MS_MRG_3IMERG_20220901-S000000-E002959_0000_V06C_30min.tiff" \
           --asset_location "gs://$BUCKET/assets" \ # Needed to store assets generated from *.tif
           --ee_asset "projects/$PROJECT/assets/test_dir" \
           --initialization_time_regex "3B-HHR-E_MS_MRG_3IMERG_%Y%m%d-S%H%M%S-*tiff" \
           --forecast_time_regex "3B-HHR-E_MS_MRG_3IMERG_%Y%m%d-S*-E%H%M%S*tiff"
```

Using DataflowRunner:

```bash
weather-mv ee --uris "gs://your-bucket/*.grib" \
           --asset_location "gs://$BUCKET/assets" \  # Needed to store assets generated from *.grib
           --ee_asset "projects/$PROJECT/assets/test_dir"
           --runner DataflowRunner \
           --project $PROJECT \
           --region  $REGION \
           --temp_location "gs://$BUCKET/tmp" \
           --job_name $JOB_NAME
```
