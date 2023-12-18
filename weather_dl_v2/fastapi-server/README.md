# Deployment Instructions & General Notes

### User authorization required to set up the environment:
* roles/container.admin

### Authorization needed for the tool to operate:
We are not configuring any service account here hence make sure that compute engine default service account have roles:
* roles/pubsub.subscriber
* roles/storage.admin
* roles/bigquery.dataEditor
* roles/bigquery.jobUser

### Install kubectl:
```
apt-get update

apt-get install -y kubectl
```
 
### Create cluster:
```
export PROJECT_ID=<your-project-here>
export REGION=<region> eg: us-west1
export ZONE=<zone> eg: us-west1-a
export CLUSTER_NAME=<cluster-name> eg: weather-dl-v2-cluster
export DOWNLOAD_NODE_POOL=downloader-pool
export SERVER_NODE_POOL=server-pool

gcloud beta container --project $PROJECT_ID clusters create $CLUSTER_NAME --zone $ZONE --no-enable-basic-auth --cluster-version "1.27.2-gke.1200" --release-channel "regular" --machine-type "e2-standard-8" --image-type "COS_CONTAINERD" --disk-type "pd-balanced" --disk-size "1100" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/cloud-platform" --max-pods-per-node "16" --num-nodes "4" --logging=SYSTEM,WORKLOAD --monitoring=SYSTEM --enable-ip-alias --network "projects/$PROJECT_ID/global/networks/default" --subnetwork "projects/$PROJECT_ID/regions/$REGION/subnetworks/default" --no-enable-intra-node-visibility --default-max-pods-per-node "16" --enable-autoscaling --min-nodes "4" --max-nodes "100" --location-policy "BALANCED" --no-enable-master-authorized-networks --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0 --enable-managed-prometheus --enable-shielded-nodes --node-locations $ZONE --node-labels preemptible=false && 

gcloud beta container --project $PROJECT_ID node-pools create $DOWNLOAD_NODE_POOL --cluster $CLUSTER_NAME --zone $ZONE --machine-type "e2-standard-8" --image-type "COS_CONTAINERD" --disk-type "pd-balanced" --disk-size "1100" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/cloud-platform" --max-pods-per-node "16" --num-nodes "1" --enable-autoscaling --min-nodes "1" --max-nodes "100" --location-policy "BALANCED" --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0 --node-locations $ZONE --node-labels preemptible=false

gcloud beta container --project $PROJECT_ID node-pools create $SERVER_NODE_POOL --cluster $CLUSTER_NAME --zone $ZONE --node-version "1.27.7-gke.1056000" --machine-type "e2-standard-8" --image-type "COS_CONTAINERD" --disk-type "pd-balanced" --disk-size "250" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/cloud-platform" --num-nodes "1" --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0
```

### Connect to Cluster:
```
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE --project $PROJECT_ID
```

### How to create environment:
```
conda env create --name weather-dl-v2-server --file=environment.yml

conda activate weather-dl-v2-server
```

### Make changes in weather_dl_v2/config.json, if required [for running locally]
```
export CONFIG_PATH=/path/to/weather_dl_v2/config.json
```

### To run fastapi server:
```
uvicorn main:app --reload
```

* Open your browser at http://127.0.0.1:8000.


### Create docker image for server:
Refer instructions in weather_dl_v2/README.md

### Add path of created server image in server.yaml:
```
Please write down the fastAPI server's docker image path at Line 42 of server.yaml.
```

### Create ConfigMap of common configurations for services:
Make necessary changes to weather_dl_v2/config.json and run following command.  
ConfigMap is used for:
- Having a common configuration file for all services.
- Decoupling docker image and config files.
```
kubectl create configmap dl-v2-config --from-file=/path/to/weather_dl_v2/config.json
```

### Deploy fastapi server on kubernetes:
```
kubectl apply -f server.yaml --force
```

## General Commands
### For viewing the current pods:
```
kubectl get pods
```

### For deleting existing deployment:
```
kubectl delete -f server.yaml --force