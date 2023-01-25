## Deployment Instructions:

Due to our org level policy we can't expose external-ip using LoadBalancer Service
while deploying our fastapi-fetch server.

In case your project don't have any such restriction a
then no need to create a nginx-server on VM to access this fastapi server
instead directly hit the external-ip exposed by LoadBalancer service on kubernetes.

* **Replace the fetcher-api Pod's IP in nginx.conf**:
```
Please write down the fetcher-api Pod's IP at Line 8 of nginx.conf.
```
> Note: Command to get the Pod IP : `kubectl get pods -o wide`.
>
> Though note that in case of Pod restart IP might get change. So we need to look
> for better solution for the same.

* **Create docker image for nginx-server**:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-nginx-server" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Create a VM using above craeted docker-image**:
```
export ZONE=<zone> eg: us-cental1-a
export SERVICE_ACCOUNT=<service account> # Let's keep this as Compute Engine Default Service Account
export IMAGE_PATH=<container-image-path> # The above created image-path

gcloud compute instances create-with-container weather-dl-v2-nginx-server  \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=e2-medium \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=$SERVICE_ACCOUNT \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=http-server,https-server \
    --image=projects/cos-cloud/global/images/cos-stable-101-17162-40-52 \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-balanced \
    --boot-disk-device-name=weather-dl-v2-nginx-server \
    --container-image=$IMAGE_PATH \
    --container-restart-policy=on-failure \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=container-vm=cos-stable-101-17162-40-52
```

* **Hit our fastapi server after doing ssh in the above create VM**:
```
curl localhost:8080
```