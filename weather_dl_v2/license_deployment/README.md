# Deployment Instructions & General Notes

### How to create environment
```
conda env create --name weather-dl-v2-license-dep --file=environment.yml

conda activate weather-dl-v2-license-dep
```

### Make changes in config/config.json, if required [for running locally]
```
Please make approriate changes in config/config.json, if required.
```

### Create docker image for license deployment
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-license-dep" --timeout=79200 --machine-type=e2-highcpu-32
```