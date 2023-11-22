# Deployment Instructions & General Notes

### How to create environment
```
conda env create --name weather-dl-v2-license-dep --file=environment.yml

conda activate weather-dl-v2-license-dep
```

### Make changes in weather_dl_v2/config.json, if required [for running locally]
```
export CONFIG_PATH=/path/to/weather_dl_v2/config.json
```

### Create docker image for license deployment
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools
export SERVICE=weather-dl-v2-license-dep
export VER=$(cat VERSION.txt)

gcloud builds submit --config=cloudbuild.yml --substitutions=_PROJECT_ID=$PROJECT_ID,_REPO=$REPO,_SERVICE=$SERVICE,_VER=$VER
```