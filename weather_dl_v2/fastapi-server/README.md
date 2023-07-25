# Deployment Instructions & General Notes

### How to create environment:
```
conda env create --name weather-dl-v2-server --file=environment.yml

conda activate weather-dl-v2-server
```

### To run fastapi server:
```
uvicorn main:app --reload
```

* Open your browser at http://127.0.0.1:8000.


### Add path of created license deployment image in license_dep/license_deployment.yaml:
```
Please write down the license deployment's docker image path at Line 22 of license_deployment.yaml.
```


### Create docker image for server:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-server" --timeout=79200 --machine-type=e2-highcpu-32
```

### Add path of created server image in server.yaml:
```
Please write down the fastAPI server's docker image path at Line 42 of server.yaml.
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