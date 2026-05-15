---
title: "How to run Texera on local Kubernetes"
weight: 60
---

This document explains how to run Texera on Kubernetes locally for development purposes.

---

## 1. Prerequisites

Before you begin, you will need a local Kubernetes cluster manager. We use **Minikube** in this instruction.

1.  [Install Minikube](https://minikube.sigs.k8s.io/docs/start/).
2.  Start your cluster:
    ```bash
    minikube start
    ```
3.  Verify that your node is running. You should see `minikube` in your node list when you run:
    ```bash
    kubectl get nodes
    ```
4. [Install Helm](https://helm.sh/docs/intro/install/).
5. Install local path plugin:
   ```bash
   kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/master/deploy/local-path-storage.yaml
   ```
---

## 2. Install Texera using Helm

All the necessary Kubernetes files are located in the `bin/k8s` directory of this repository.

1.  Navigate to the `bin` directory:
    ```bash
    cd bin
    ```
2.  Install the Texera Helm chart. This command will install all Texera services into a new `texera-dev` namespace.
    ```bash
    helm install texera k8s --namespace texera-dev --create-namespace
    ```

> **Note:** If you get an error about missing Helm dependencies, navigate to the `k8s` directory and run the dependency update command, then try the installation again:
>
> ```bash
> cd k8s
> helm dependency update
> cd ..
> helm install texera k8s --namespace texera-dev --create-namespace
> ```

---

## 3. Verify the Installation

Wait for the required deployments to be in the `Running` state. You can check their status by running:

```bash
kubectl get deployments -n texera-dev
````

The key deployments required to run Texera are:

  * `texera-webserver`
  * `texera-file-service`
  * `texera-workflow-computing-unit-manager`

-----

## 4\. Accessing the Texera UI

Once the deployments are running, you can access the Texera web interface.

1.  **Port-Forwarding (If Required)**

    By default, the UI should be available at **http://localhost:30080**.

    If you get a "connection refused" error, you may need to manually forward the ingress port. Open a new terminal and run:

    ```bash
    kubectl port-forward -n envoy-gateway-system service/$(kubectl get svc -n envoy-gateway-system -l gateway.envoyproxy.io/owning-gateway-name=texera-gateway -o jsonpath='{.items[0].metadata.name}') 30080:80
    ```

2.  **Login**

    Open http://localhost:30080 in your browser and log in using the default username and password.

-----

## 5\. Troubleshooting

### File Upload Error

If you see an error when trying to upload a file to a dataset, you may need to forward the port for MinIO (our file storage service).

Run the following command in a new terminal:

```bash
kubectl port-forward -n texera-dev service/texera-minio 31000:9000
```

This maps the service's port `9000` to your local port `31000`.

### Using Custom-Built Images

To test custom changes, you can update the `bin/k8s/values.yaml` file to use your own Docker images. After modifying the `values.yaml` file, upgrade the Helm release to apply the changes:

```bash
helm upgrade texera k8s --namespace texera-dev
```


## 6. Security Recommendation
For any deployment, especially in production, it's crucial to apply the principle of least privilege to limit potential damage from a security vulnerability. While the OS user deploying the chart needs kubectl and helm permissions, a more critical concern is the user running the application inside the containers.

### Run Containers as a Non-Root User
By default, many container images run as the root user. If an attacker exploits a vulnerability in an application (like the running code on computing unit), they would gain root privileges within the container, giving them full control to access or modify its contents and potentially attack other services.

To prevent this, you should configure the Kubernetes deployments to run the processes as a specific, unprivileged user.

The following is a sample template you can use:
```yaml
spec:
  template:
    spec:
      securityContext:
        # Run as a non-root user (e.g., user 1001)
        runAsUser: 1001
        runAsGroup: 1001
        # Enforce that the container cannot run as root
        runAsNonRoot: true
        # Make the root filesystem read-only
        readOnlyRootFilesystem: true
      containers:
      - name: texera-webserver
        image: ...
```