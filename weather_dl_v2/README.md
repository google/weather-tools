## weather-dl-v2

<!-- TODO: Create a setup script to ease deployment process. -->

> **_NOTE:_**   weather-dl-v2 only supports python 3.10

### Sequence of steps:
1) Refer to downloader_kubernetes/README.md
2) Refer to license_deployment/README.md
3) Refer to fastapi-server/README.md
4) Refer to cli/README.md


### To create docker images of services

```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools
```

Choose any ONE service from below:

1. CLI
```
export SERVICE=weather-dl-v2-cli
export FOLDER=cli
```

2. weather-dl-v2-downloader
```
export SERVICE=weather-dl-v2-downloader
export FOLDER=downloader_kubernetes
```

3. weather-dl-v2-license-dep
```
export SERVICE=weather-dl-v2-license-dep
export FOLDER=license_deployment
```

4. weather-dl-v2-server
```
export SERVICE=weather-dl-v2-server
export FOLDER=fastapi-server
```

Finally run:
```
export VER=$(cat $FOLDER/VERSION.txt)

gcloud builds submit --config=cloudbuild.yml --substitutions=_PROJECT_ID=$PROJECT_ID,_REPO=$REPO,_SERVICE=$SERVICE,_VER=$VER,_FOLDER=$FOLDER
```