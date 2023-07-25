# Deployment / Usage Instruction 

### User authorization required to set up the environment:
* roles/container.admin

### Authorization needed for the tool to operate:
We are not configuring any service account here hence make sure that compute engine default service account have roles:
* roles/storage.admin
* roles/bigquery.dataEditor
* roles/bigquery.jobUser

### Write the manifest location path
```
Please write down the manifest path at Line 43 of downloader.py.
Eg: "fs://test_manifest?projectId=XXX"
```

### Create docker image for downloader:
```
export REPO=<repo> eg:weather-tools

gcloud builds submit Dockerfile --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-downloader" --timeout=79200 --machine-type=e2-highcpu-32
```
