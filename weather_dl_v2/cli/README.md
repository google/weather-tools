# weather-dl-cli
This is a command line interface for talking to the weather-dl-v2 FastAPI server.

- Due to our org level policy we can't expose external-ip using LoadBalancer Service
while deploying our FastAPI server. Hence we need to deploy the CLI on a VM to interact
through our fastapi server.

Replace the FastAPI server pod's IP in cli_config.json.
```
Please make approriate changes in cli_config.json, if required.
```
> Note: Command to get the Pod IP : `kubectl get pods -o wide`.
>
> Though note that in case of Pod restart IP might get change. So we need to look
> for better solution for the same.

## Create docker image for weather-dl-cli 

```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-cli" --timeout=79200 --machine-type=e2-highcpu-32
```

## Create a VM using above created docker-image
```
export ZONE=<zone> eg: us-west1-a
export SERVICE_ACCOUNT=<service account> # Let's keep this as Compute Engine Default Service Account
export IMAGE_PATH=<container-image-path> # The above created image-path

gcloud compute instances create-with-container weather-dl-v2-cli \
    --project=$PROJECT_ID \
    --zone=$ZONE \
    --machine-type=e2-medium \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=$SERVICE_ACCOUNT \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=http-server,https-server \
    --image=projects/cos-cloud/global/images/cos-stable-105-17412-101-24 \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-balanced \
    --boot-disk-device-name=weather-dl-v2-cli \
    --container-image=$IMAGE_PATH \
    --container-restart-policy=on-failure \
    --container-tty \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --labels=goog-ec-src=vm_add-gcloud,container-vm=cos-stable-105-17412-101-24 \
    --metadata-from-file=startup-script=vm-startup.sh
```

## Use the cli after doing ssh in the above created VM
```
weather-dl-v2 --help
```
