# Deployment Instructions & General Notes

### How to create environment
```
conda env create --name weather-dl-v2-license-dep --file=environment.yml

conda activate weather-dl-v2-license-dep
```

### Make changes in deployment config, if required
```
Please make approriate changes in deployment_config.json, if required.
```

### Create docker image for license deployment
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-license-dep" --timeout=79200 --machine-type=e2-highcpu-32
```