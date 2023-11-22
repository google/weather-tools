# Deployment / Usage Instruction 

### User authorization required to set up the environment:
* roles/container.admin

### Authorization needed for the tool to operate:
We are not configuring any service account here hence make sure that compute engine default service account have roles:
* roles/storage.admin
* roles/bigquery.dataEditor
* roles/bigquery.jobUser

### Make changes in weather_dl_v2/config.json, if required [for running locally]
```
export CONFIG_PATH=/path/to/weather_dl_v2/config.json
```

### Create docker image for downloader:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools
export SERVICE=weather-dl-v2-downloader
export VER=$(cat VERSION.txt)

gcloud builds submit --config=cloudbuild.yml --substitutions=_PROJECT_ID=$PROJECT_ID,_REPO=$REPO,_SERVICE=$SERVICE,_VER=$VER
```
