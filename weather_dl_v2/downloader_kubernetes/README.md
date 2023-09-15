# Deployment / Usage Instruction 

### User authorization required to set up the environment:
* roles/container.admin

### Authorization needed for the tool to operate:
We are not configuring any service account here hence make sure that compute engine default service account have roles:
* roles/storage.admin
* roles/bigquery.dataEditor
* roles/bigquery.jobUser

### Make changes in config/config.json, if required [for running locally]
```
Please make approriate changes in config/config.json, if required.
```

### Create docker image for downloader:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-downloader" --timeout=79200 --machine-type=e2-highcpu-32
```
