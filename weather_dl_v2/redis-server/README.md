## Deployment of redis-master on kubernetes (Required when using Redis implementation)

* **Deploy redis-master service on kubernetes:**
```
kubectl apply -f redis-server.yaml --force
```

## General Commands
* **For viewing the current pods**:
```
kubectl get pods
```

* **For viewing the redis-master service**:
```
kubectl describe svc redis-master
```

* **For deleting existing deployment**:
```
kubectl delete -f ./redis-master.yaml --force