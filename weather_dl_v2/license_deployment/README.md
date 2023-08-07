# Deployment Instructions & General Notes

### How to create environment
```
conda env create --name weather-dl-v2-license-dep --file=environment.yml

conda activate weather-dl-v2-license-dep
```

### Add path of created downloader image in downloader.yaml
```
Please write down the downloader's docker image path at Line 11 of downloader.yaml.
```

### Add manifest collection name in manifest.py
```
Please write down the manifest collection name at Line 500 of manifest.py.
```

### Create docker image for license deployment
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-license-dep" --timeout=79200 --machine-type=e2-highcpu-32
```