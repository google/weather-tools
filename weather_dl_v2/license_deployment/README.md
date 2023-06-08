# Deployment Instructions & General Notes

* **How to create environment:**
```
conda env create --name weather-dl-v2-license-dep --file=environment.yml

conda activate weather-dl-v2-license-dep
```

* **Create docker image for license deployment**:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-license-dep" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Add path of created license deployment image in license_deployment.yaml**:
```
Please write down the license deployment's docker image path at Line 22 of license_deployment.yaml.
```

* **Deploy license deployment's on kubernetes:**
We are not required to this step manually, this will be done by fastapi-server.
```
kubectl apply -f license_deployment.yaml -- --license <license-id>
```

## General Commands
* **For viewing the current pods**:
```
kubectl get pods
```

* **For deleting existing deployment**:
```
kubectl delete -f ./license_deployment.yaml --force