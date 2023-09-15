## weather-dl-v2

### Sequence of steps:
1) Refer to downloader_kubernetes/README.md
2) Refer to license_deployment/README.md
3) Create ConfigMap using weather_dl_v2/config.json

    - Make necessary changes to config.json

    - Run these commands
    ```
    export PROJECT_ID=<your-project-here>
    export ZONE=<zone> eg: us-west1-a
    export CLUSTER_NAME=<cluster-name> eg: weather-dl-v2-cluster
    export DOWNLOAD_NODE_POOL=downloader-pool

    gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE --project $PROJECT_ID

    kubectl create configmap dl-v2-config --from-file=/path/to/config.json
    ```
4) Refer to fastapi-server/README.md
5) Refer to cli/README.md

