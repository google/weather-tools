# Deployment Instructions & General Notes

* **How to create environment:**
```
conda env create --name weather-dl-v2-fetcher --file=environment.yml

conda activate weather-dl-v2-fetcher
```

* **To run fastapi server:**
```
uvicorn main:app --reload
```

* Open your browser at http://127.0.0.1:8000.

* **Create docker image for fetcher**:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-fetcher" --timeout=79200 --machine-type=e2-highcpu-32
```

* **Add path of created fetcher image in fetcher.yaml**:
```
Please write down the fetcher's docker image path at Line 42 of fetcher.yaml.
```

* **Deploy fetcher-fastapi server on kubernetes:**
```
kubectl apply -f fetcher.yaml --force
```

## General Commands
* **For viewing the current pods**:
```
kubectl get pods
```

* **For deleting existing deployment**:
```
kubectl delete -f ./fetcher.yaml --force