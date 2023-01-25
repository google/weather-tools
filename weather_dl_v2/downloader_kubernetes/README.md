# Deployment / Usage Instruction 

* **Install kubectl**:
```
apt-get update

apt-get install -y kubectl
```
 
* **Create cluster**:
```
export PROJECT_ID=<your-project-here>
export REGION=<region> eg: us-central1
export ZONE=<zone> eg: us-cental1-c
export CLUSTER_NAME=<cluster-name> eg: weather-dl-v2-cluster

gcloud beta container --project $PROJECT_ID clusters create $CLUSTER_NAME --zone $ZONE --no-enable-basic-auth --cluster-version "1.24.7-gke.900" --release-channel "regular" --machine-type "e2-medium" --image-type "COS_CONTAINERD" --disk-type "pd-balanced" --disk-size "2000" --metadata disable-legacy-endpoints=true --scopes "https://www.googleapis.com/auth/cloud-platform" --max-pods-per-node "110" --num-nodes "3" --logging=SYSTEM,WORKLOAD --monitoring=SYSTEM --enable-ip-alias --network "projects/$PROJECT_ID/global/networks/default" --subnetwork "projects/$PROJECT_ID/regions/$REGION/subnetworks/default" --no-enable-intra-node-visibility --default-max-pods-per-node "110" --enable-autoscaling --min-nodes "3" --max-nodes "100" --location-policy "BALANCED" --no-enable-master-authorized-networks --addons HorizontalPodAutoscaling,HttpLoadBalancing,GcePersistentDiskCsiDriver --enable-autoupgrade --enable-autorepair --max-surge-upgrade 1 --max-unavailable-upgrade 0 --enable-shielded-nodes --node-locations $ZONE
```

* **Connect to Cluster**:
```
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE --project $PROJECT_ID
```

* **Use of redis or pub-sub**:
```
Please note that by default we use redis as the queue b/w fetching & dowloading steps.
But you can change it to make use of pub-sub by setting the value of constant USE_REDIS to False
in subscriber.py & ecmwf_fetching_server/fetch.py
```

* **Deploying the Custom Metrics Adapter (Required for pub-sub implementation)**:
```
kubectl create clusterrolebinding cluster-admin-binding \
    --clusterrole cluster-admin --user "$(gcloud config get-value account)"
    
kubectl apply -f https://raw.githubusercontent.com/GoogleCloudPlatform/k8s-stackdriver/master/custom-metrics-stackdriver-adapter/deploy/production/adapter_new_resource_model.yaml
```

* **Create a pub/sub topic**:
```
gcloud services enable cloudresourcemanager.googleapis.com pubsub.googleapis.com

gcloud pubsub topics create weather-dl-v2

gcloud pubsub subscriptions create weather-dl-v2-read --topic=weather-dl-v2
```
> Note: We are not configuring any service account here hence make sure that compute engine default service account has
> role: "roles/pubsub.subscriber".

* **Write the subscription path**
```
Please write down the above created subscription path at Line 44 of subscriber.py.
```
> Note: You can get the subscription path using `gcloud pubsub subscriptions list`.
> eg: projects/$PROJECT_ID/subscriptions/weather-dl-v2-read

* **Write the manifest location path**
```
Please write down the manifest path at Line 43 of downloader.py.
Eg: "fs://weather-dl-v2?projectId=XXX"
```
> Note: Currently we support firestore manifest but going futher we will update this to
> BiqQuery manifest.

* **Create docker image for downloader**:
```
export REPO=<repo> eg:weather-tools

gcloud builds submit Dockerfile.downloader --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-downloader" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Add path of created downloader image in downloader.yaml**:
```
Please write down the downloader's docker image path at Line 11 of downloader.yaml.
```

* **Create docker image for subsctiber**:
```
gcloud builds submit Dockerfile.subscriber --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-subscriber" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Add path of created subscriber image in subscriber.yaml**:
```
Please write down the subscriber's docker image path at Line 16 of subscriber.yaml.
```

* **Deploy subscriber code**:
```
kubectl apply -f subscriber.yaml --force
```

## General Commands
* **For viewing the current pods**:
```
kubectl get pods
```

* **For deleting existing deployment**:
```
kubectl delete -f ./subscriber.yaml --force
```
