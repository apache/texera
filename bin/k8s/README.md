# Texera Kubernetes Deployment

## Prerequisites
- A running Kubernetes cluster
- Port `30080` is available

## Launch the whole stack
Run the following command in current directory :
```shell
helm install texera texera-helmchart --namespace texera-dev --create-namespace
```
This will create a release named `texera`, a namespace named `texera-dev` and all the deployments and services under the namespace `texera-dev`.

**Please wait for around 1 minute for all the pods to be ready**. Once all the pods are ready, the service should be accessible

## Access the service

Access `http://localhost:30080`.

## Terminate the whole stack

Run the following command:
```shell
 helm uninstall texera --namespace texera-dev
```