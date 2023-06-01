# Deployment / Usage Instruction

* **User authorization required to set up the environment**:
* roles/container.admin

* **Authorization needed for the tool to operate**:
We are not configuring any service account here hence make sure that compute engine default service account have roles:
* roles/pubsub.subscriber
* roles/storage.admin
* roles/bigquery.dataEditor
* roles/bigquery.jobUser

* **Install kubectl**:
```
apt-get update

apt-get install -y kubectl
```
 
* **Create cluster**:
```
export PROJECT_ID=<your-project-here>
export REGION=<region> eg: us-central1
export ZONE=<zone> eg: us-central1-c
export CLUSTER_NAME=<cluster-name> eg: weather-dl-1-5-cluster

gcloud beta container --project $PROJECT_ID clusters create $CLUSTER_NAME --zone $ZONE --no-enable-basic-auth --cluster-version "1.25.8-gke.500" --release-channel "regular" --machine-type "e2-standard-8" --image-type "COS_CONTAINERD" --disk-type "pd-balanced" --disk-size "1100" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/cloud-platform" --max-pods-per-node "16" --num-nodes "4" --logging=SYSTEM,WORKLOAD --monitoring=SYSTEM --enable-ip-alias --network "projects/$PROJECT_ID/global/networks/default" --subnetwork "projects/$PROJECT_ID/regions/$REGION/subnetworks/default" --no-enable-intra-node-visibility --default-max-pods-per-node "16" --enable-autoscaling --min-nodes "4" --max-nodes "100" --location-policy "BALANCED" --no-enable-master-authorized-networks --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0 --enable-shielded-nodes --node-locations $ZONE --node-labels preemptible=false
```

* **Connect to Cluster**:
```
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE --project $PROJECT_ID
```

* **Deploying the Custom Metrics Adapter**:
```
kubectl create clusterrolebinding cluster-admin-binding \
    --clusterrole cluster-admin --user "$(gcloud config get-value account)"
    
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/k8s-stackdriver/master/custom-metrics-stackdriver-adapter/deploy/production/adapter_new_resource_model.yaml
```

* **Create a pub/sub topic**:
```
gcloud services enable cloudresourcemanager.googleapis.com pubsub.googleapis.com

gcloud pubsub topics create weather-dl-1.5

gcloud pubsub subscriptions create weather-dl-1.5-read --topic=weather-dl-1.5
```
> Note: Please write down the above created topic path at Line 82 weather_dl/download_pipeline/fetcher.py & subscription path
> at Line 23 weather_dl/download_kubernetes/pub-sub-subscriber.py

* **Specify Firestore manifest location in downloader code**:
Please write down the FS manifest location that will be used in fetcher code (weather-dl) at Line 36 weather_dl/download_kubernetes/downloader.py.
Format: `fs://<collection-name>?projectId=<project-id>`

* **Create docker image for downloader**:
```
export REPO=weather-tools

gcloud builds submit downloader.Dockerfile --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-1.5-downloader" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Add path of created downloader image in downloader.yaml**:
```
Please write down the downloader's docker image path at Line 11 of downloader.yaml.
```

* **Create docker image for subscriber**:
```
gcloud builds submit subscriber.Dockerfile --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-1.5-subscriber" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Add path of created subscriber image in pub-sub-subscriber.yaml**:
```
Please write down the subscriber's docker image path at Line 16 of pub-sub-subscriber.yaml.
```

* **Deploy subscriber code**:
```
kubectl apply -f pub-sub-subscriber.yaml --force
```

## General Commands
* **For viewing the current pods**:
```
kubectl get pods
```

* **For deleting existing deployment**:
```
kubectl delete -f ./pub-sub-subscriber.yaml --force
```
