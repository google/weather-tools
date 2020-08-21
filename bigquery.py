#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
#import glob as glob
from google.cloud import storage
from google.cloud import bigquery
client = bigquery.Client()
dataset_id = 'grid_intelligence_yes'
dataset_ref = client.dataset(dataset_id)
TABLE_NAME = 'yes_dalmp_proc'

# run loop through all csv files and import to bigquery with folders as keys
# find all csv files in given folder 
def get_blobs(bucket_name, prefix, delimiter=None):
    storage_client = storage.Client()
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(
        bucket_name, prefix=prefix, delimiter=delimiter
    )
    objs = []
    for blob in blobs:
        objs.append(blob.name)
    return objs

# after reading in csv define schema for upload to bigquery
def def_schem(df):
    schem=[]
    for f in df.columns:
        if f == 'datetime':
            schem.append(bigquery.SchemaField("datetime", "TIMESTAMP"))
        elif f == 'index':
            schem.append(bigquery.SchemaField("index", "INT64"))
        elif f == 'number':
            schem.append(bigquery.SchemaField("number", "INT64"))
        elif f == 'latitude':
            schem.append(bigquery.SchemaField("latitude", "FLOAT64"))
        elif f == 'longitude':
            schem.append(bigquery.SchemaField("longitude", "FLOAT64"))
        elif f == 'tp':
            schem.append(bigquery.SchemaField("tp", "FLOAT64"))
        elif f == 't2m':
            schem.append(bigquery.SchemaField("t2m", "FLOAT64"))
        elif f == 'v10':
            schem.append(bigquery.SchemaField("v10", "FLOAT64"))
        elif f == 'u10':
            schem.append(bigquery.SchemaField("u10", "FLOAT64"))
        elif f == 'd2m':
            schem.append(bigquery.SchemaField("d2m", "FLOAT64"))
        elif f == 'v100':
            schem.append(bigquery.SchemaField("v100", "FLOAT64"))
        elif f == 'u100':
            schem.append(bigquery.SchemaField("u100", "FLOAT64"))
        elif f == 'msl':
            schem.append(bigquery.SchemaField("msl", "FLOAT64"))
        elif f == 'i10fg':
            schem.append(bigquery.SchemaField("i10fg", "FLOAT64"))
        elif f == 'ssr':
            schem.append(bigquery.SchemaField("ssr", "FLOAT64"))
        elif f == 'str':
            schem.append(bigquery.SchemaField("str", "FLOAT64"))
        elif f == 'step':
            schem.append(bigquery.SchemaField("step", "STRING"))
        elif f == 'initialization_time':
            schem.append(bigquery.SchemaField("initialization_time", "TIMESTAMP"))
        elif f == 'surface':
            schem.append(bigquery.SchemaField("surface", "INT64"))
        elif f == 'publish_time':
            schem.append(bigquery.SchemaField("publish_time", "TIMESTAMP"))
        else:
            break
    return schem

def def_schem_grid(df):
    schem=[]
    for f in df.columns:
        if f == 'datetime':
            schem.append(bigquery.SchemaField("datetime", "TIMESTAMP"))
        elif f == 'baffinwind_u100':
            schem.append(bigquery.SchemaField('baffinwind_u100', "FLOAT64"))
        elif f == 'baffinwind_v100':
            schem.append(bigquery.SchemaField('baffinwind_v100', "FLOAT64"))
        elif f == 'baffinwind_u10':
            schem.append(bigquery.SchemaField('baffinwind_u10', "FLOAT64"))
        elif f == 'baffinwind_v10':
            schem.append(bigquery.SchemaField('baffinwind_v10', "FLOAT64"))
        elif f == 'anacachowindfarmllc_u100':
            schem.append(bigquery.SchemaField('anacachowindfarmllc_u100', "FLOAT64"))
        elif f == 'anacachowindfarmllc_v100':
            schem.append(bigquery.SchemaField('anacachowindfarmllc_v100', "FLOAT64"))
        elif f == 'bobcatbluffwindprojectllc_u100':
            schem.append(bigquery.SchemaField('bobcatbluffwindprojectllc_u100', "FLOAT64"))
        elif f == 'bobcatbluffwindprojectllc_v100':
            schem.append(bigquery.SchemaField('bobcatbluffwindprojectllc_v100', "FLOAT64"))
        elif f == 'briscoewindfarm_v100':
            schem.append(bigquery.SchemaField('briscoewindfarm_v100', "FLOAT64"))
        elif f == 'buckthornwindproject_u100':
            schem.append(bigquery.SchemaField('buckthornwindproject_u100', "FLOAT64"))
        elif f == 'buckthornwindproject_v100':
            schem.append(bigquery.SchemaField('buckthornwindproject_v100', "FLOAT64"))
        elif f == 'kingmountainwindranch1_v100':
            schem.append(bigquery.SchemaField('kingmountainwindranch1_v100', "FLOAT64"))
        elif f == 'roscoewindfarmllc_u100':
            schem.append(bigquery.SchemaField('roscoewindfarmllc_u100', "FLOAT64"))
        elif f == 'route66windplant_u100':
            schem.append(bigquery.SchemaField('route66windplant_u100', "FLOAT64"))
        elif f == 'sherbinoiwindfarm_u100':
            schem.append(bigquery.SchemaField('sherbinoiwindfarm_u100', "FLOAT64"))
        elif f == 'sherbinoiwindfarm_v100':
            schem.append(bigquery.SchemaField('sherbinoiwindfarm_v100', "FLOAT64"))
        elif f == 'southplainsii_u100':
            schem.append(bigquery.SchemaField('southplainsii_u100', "FLOAT64"))
        elif f == 'southplainsii_v100':
            schem.append(bigquery.SchemaField('southplainsii_v100', "FLOAT64"))
        elif f == 'sweetwaterwind1llc_u100':
            schem.append(bigquery.SchemaField('sweetwaterwind1llc_u100', "FLOAT64"))
        elif f == 'sweetwaterwind1llc_v100':
            schem.append(bigquery.SchemaField('sweetwaterwind1llc_v100', "FLOAT64"))
        elif f == 'trentwindfarmlp_u100':
            schem.append(bigquery.SchemaField('trentwindfarmlp_u100', "FLOAT64"))
        elif f == 'bruenningsbreezewindfarm_v100':
            schem.append(bigquery.SchemaField('bruenningsbreezewindfarm_v100', "FLOAT64"))
        elif f == 'kingmountainwindranch1_u100':
            schem.append(bigquery.SchemaField('kingmountainwindranch1_u100', "FLOAT64"))
        elif f == 'elbowcreekwindprojectllc_u100':
            schem.append(bigquery.SchemaField('elbowcreekwindprojectllc_u100', "FLOAT64"))
        elif f == 'briscoewindfarm_u100':
            schem.append(bigquery.SchemaField('briscoewindfarm_u100', "FLOAT64"))
        elif f == 'bruenningsbreezewindfarm_u100':
            schem.append(bigquery.SchemaField('bruenningsbreezewindfarm_u100', "FLOAT64"))
        elif f == 'route66windplant_v100':
            schem.append(bigquery.SchemaField('route66windplant_v100', "FLOAT64"))
        elif f == 'roscoewindfarmllc_v100':
            schem.append(bigquery.SchemaField('roscoewindfarmllc_v100', "FLOAT64"))
        elif f == 'cedrohillwindllc_v100':
            schem.append(bigquery.SchemaField('cedrohillwindllc_v100', "FLOAT64"))
        elif f == 'trentwindfarmlp_v100':
            schem.append(bigquery.SchemaField('trentwindfarmlp_v100', "FLOAT64"))
        elif f == 'cedrohillwindllc_u100':
            schem.append(bigquery.SchemaField('cedrohillwindllc_u100', "FLOAT64"))
        elif f == 'elbowcreekwindprojectllc_v100':
            schem.append(bigquery.SchemaField('elbowcreekwindprojectllc_v100', "FLOAT64"))
        else:
            break
    return schem

def def_schem_dalmp(df):
    schem=[]
    for f in df.columns:
        if f == 'datetime':
            schem.append(bigquery.SchemaField("datetime", "TIMESTAMP"))
        elif f == 'MARKETDAY':
            schem.append(bigquery.SchemaField('MARKETDAY', "STRING"))
        elif f == 'HOURENDING':
            schem.append(bigquery.SchemaField('HOURENDING', "INT64"))
        elif f == 'PEAKTYPE':
            schem.append(bigquery.SchemaField('PEAKTYPE', "STRING"))
        elif f.lower() == 'dalmp':
            schem.append(bigquery.SchemaField('dalmp', "FLOAT64"))
        else:
            break
    return schem


files = get_blobs('gi-sandbox','misc/sample_data/20200608/clean_data/dalmp/yes/cactusflatswindenergyproject')
fails = 0

for fil in files:
    try:
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.skip_leading_rows = 1
        #job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.CSV
        uri = '{}{}'.format('gs://gi-sandbox/',fil)
        df = pd.read_csv(uri)
        job_config.schema = def_schem_dalmp(df)

        load_job = client.load_table_from_uri(
            uri, dataset_ref.table(TABLE_NAME), job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))

        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        destination_table = client.get_table(dataset_ref.table(TABLE_NAME))
        print("Loaded {} rows.".format(destination_table.num_rows))
    except:
        fails += 1
        pass
    
print(fails)