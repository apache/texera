---
title: "Deploying Texera on Google Cloud Platform (GCP)"
weight: 64
---

This document explains how to deploy Texera on Google Cloud Platform (GCP) using a Google Kubernetes Engine (GKE) cluster.

---

## Prerequisites: Check your quota

Your GCP account should be able to allocate at least 20 vCPUs and 1 TB of SSD. To check your quota, go to the [GCP Quotas](https://console.cloud.google.com/iam-admin/quotas?referrer=search&pageState=(%22allQuotasTable%22:(%22f%22:%22%255B%257B_22k_22_3A_22Name_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22CPUs_5C_22_22_2C_22s_22_3Atrue_2C_22i_22_3A_22displayName_22%257D_2C%257B_22k_22_3A_22_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22OR_5C_22_22_2C_22o_22_3Atrue_2C_22s_22_3Atrue%257D_2C%257B_22k_22_3A_22Name_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22Persistent%2520Disk%2520SSD%2520%2528GB%2529_5C_22_22_2C_22s_22_3Atrue_2C_22i_22_3A_22displayName_22%257D_2C%257B_22k_22_3A_22Dimensions%2520%2528e.g.%2520location%2529_22_2C_22t_22_3A10_2C_22v_22_3A_22_5C_22region_3Aus-central1_5C_22_22_2C_22s_22_3Atrue_2C_22i_22_3A_22displayDimensions_22%257D%255D%22))) page.
You should be able to see a pre-populated query for listing the CPUs and SSDs in the `us-central1` region by default. If you plan to deploy Texera in another region, you need to change the `Dimensions` part of the query.
<img width="1420" alt="quota2" src="https://github.com/user-attachments/assets/cf0c9b41-20df-463a-b15e-9673de29d9fa" />
If your quota does not have at least 20 CPUs and 1 TB SSD, you need to request a quota increase by clicking the 3-dot button on the right -> "Edit Quota".
<img width="1528" alt="quota3" src="https://github.com/user-attachments/assets/d9132094-3e35-4a50-9993-d10de5aa6ad4" />


---
## 1. Create an Autopilot GKE cluster

> 💡 Note: If you already have a GKE cluster and wish to use it for deploying Texera, you can skip this step and proceed directly to Step 2.

Navigate to GCP console -> Kubernetes Engine -> [Clusters](https://console.cloud.google.com/kubernetes/list/overview). Click on the `create` button.

> 💡 Note: You may need to enable the Kubernetes API if you haven't done so.

<img width="2318" alt="step0 1" src="https://github.com/user-attachments/assets/081720e9-96a2-41cc-bd4c-49363099d3bb" />

Use all default values to create a cluster. You can also customize the cluster accordingly if needed.
After 15-20 minutes, you should be able to see the status of your cluster to be in a green checkmark(<img width="25" alt="step0 0" src="https://github.com/user-attachments/assets/e949dfb3-9699-40e1-a963-08f1bcdd1f57" />) state, with 0 vCPUs and 0 memory usage.
<img width="1406" alt="step-0-2" src="https://github.com/user-attachments/assets/9cdf3fc4-8164-477c-a583-e328efd782c0" />
Click the three dots on the right, and choose "connect".
<img width="1141" alt="step-0-3" src="https://github.com/user-attachments/assets/a0249ee2-9a1d-44c2-93a3-72fa48eb5e08" />

---

In the pop-up window, copy the **project** and **region** to your clipboard. Then click **"Run in Cloud Shell"**.
Press Enter for the first command shown on the terminal.
<img width="994" alt="step-0-4" src="https://github.com/user-attachments/assets/085b2147-2192-4766-a3b6-6ba01fac05d2" />


## 2. Reserve Two Static IPs (for Texera website and MinIO)

After accessing your cluster using Cloud Shell, define the following variables based on your region and project in Step 1.
```bash
REGION="<YOUR_REGION>"
PROJECT="<YOUR_PROJECT>"
```

Execute the following bash commands to reserve two Public IP addresses.

```bash
gcloud compute addresses create texera-ip  --region=$REGION --project=$PROJECT
TEXERA_IP=$(gcloud compute addresses describe texera-ip --region=$REGION --format="get(address)"  --project $PROJECT)
gcloud compute addresses create minio-ip  --region=$REGION --project=$PROJECT
MINIO_IP=$(gcloud compute addresses describe minio-ip   --region=$REGION --format="get(address)"  --project $PROJECT)
```

Execute the following bash commands to create two nginx controllers with helm.
```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update
helm install nginx-texera ingress-nginx/ingress-nginx \
  --namespace texera --create-namespace \
  --set controller.ingressClassResource.name=nginx \
  --set controller.ingressClassResource.controllerValue="k8s.io/ingress-nginx" \
  --set controller.ingressClass=nginx \
  --set controller.service.loadBalancerIP=$TEXERA_IP \
  --set controller.service.annotations."cloud\.google\.com/load-balancer-type"="External" \
  --set rbac.create=true

helm install nginx-minio ingress-nginx/ingress-nginx \
  --namespace texera \
  --set controller.ingressClassResource.name=nginx-minio \
  --set controller.ingressClassResource.controllerValue="k8s.io/nginx-minio" \
  --set controller.ingressClass=nginx-minio \
  --set controller.service.loadBalancerIP=$MINIO_IP \
  --set controller.service.annotations."cloud\.google\.com/load-balancer-type"="External" \
  --set rbac.create=true
```

---

## 3. Prepare Texera Installation

Execute the following bash commands.

```bash
curl -L -o texera.zip https://github.com/Texera/texera/releases/download/1.1.0/texera-cluster-1-1-0-release.zip
unzip texera.zip -d texera-cluster
rm texera.zip
helm dependency build texera-cluster
```

---

## 4. Deploy Texera

Execute the following bash command.

```bash
helm install texera texera-cluster --namespace texera --create-namespace \
  --set postgresql.primary.persistence.storageClass=standard-rwo \
  --set ingress-nginx.enabled=false \
  --set metrics-server.enabled=false \
  --set exampleDataLoader.enabled=false \
  --set minio.customIngress.enabled=true \
  --set minio.customIngress.ingressClassName=nginx-minio \
  --set minio.customIngress.texeraHostname="http://$TEXERA_IP" \
  --set minio.persistence.storageClass=standard-rwo \
  --set-string lakefs.lakefsConfig="$(cat <<EOF
database:
  type: postgres
blockstore:
  type: s3
  s3:
    endpoint: http://texera-minio:9000
    pre_signed_expiry: 15m
    pre_signed_endpoint: http://$MINIO_IP
    force_path_style: true
    credentials:
      access_key_id: texera_minio
      secret_access_key: password
EOF
)" \
  --set ingressPaths.hostname=""
```

---

## Done!

* It may take 10-15 minutes to fully launch the deployment.
* During the process, you can periodically execute `kubectl get pods -n texera` to see the status of the deployed pods.
* Once every pod is in a `Running` or `Completed` status, you can execute `echo $TEXERA_IP` to get the public IP of the Texera WebUI.
* Then you can access Texera using `http://<TEXERA_IP>`.


**To remove the Texera deployment from your Kubernetes cluster, execute the following bash commands.**

```
helm uninstall texera -n texera
helm uninstall nginx-texera -n texera
helm uninstall nginx-minio -n texera
```
> Note: You also need to release the 2 allocated IP addresses on [GCP](https://console.cloud.google.com/networking/addresses/list)

---

## Advanced Configuration
You can customize the deployment by adding the following --set flags to your helm install command. These flags allow you to configure authentication, resource limits, and the number of pods for Texera deployment.

### Texera Credentials
Texera relies on Postgres, MinIO and LakeFS that require credentials. You can change the default values to make your deployment more secure.

**Default Texera Admin User**

Texera ships with a built-in administrator account (username: texera, password: texera).
To supply your own credentials during installation, pass the following Helm overrides:

```bash
# USER_SYS_ADMIN_USERNAME
--set texeraEnvVars[0].value="<user-name>" \
# USER_SYS_ADMIN_PASSWORD
--set texeraEnvVars[1].value="<password>" \
```

**MinIO Authentication**
```
--set minio.auth.rootUser=texera_minio \
--set minio.auth.rootPassword=password \
```

**PostgreSQL Authentication (username is always postgres)**
```
--set postgresql.auth.postgresPassword=root_password \
```

> 💡 Note: If you change the PostgreSQL password, you also need to change the following and add it to the install command:
<pre><code>--set lakefs.secrets.databaseConnectionString="postgres://postgres:<b>root_password</b>@texera-postgresql:5432/texera_lakefs?sslmode=disable" \</code></pre>

**LakeFS Authentication**
```
--set lakefs.auth.username=texera-admin \
--set lakefs.auth.accessKey=AKIAIOSFOLKFSSAMPLES \
--set lakefs.auth.secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
--set lakefs.secrets.authEncryptSecretKey=random_string_for_lakefs \
```

### Allocating Resources
If your cluster has more available resources, you can allocate additional CPU, memory, and disks to Texera to improve the performance.

**Postgres**

To allocate more CPU, Memory and disk to Postgres, do:
```
--set postgresql.primary.resources.requests.cpu=4 \
--set postgresql.primary.resources.requests.memory=4Gi \
--set postgresql.primary.persistence.size=50Gi \
```

**MinIO**

To increase the storage for user's input dataset, do:
```
--set minio.persistence.size=100Gi
```

**Computing Unit**

To customize options for the computing unit, do:
```bash
# MAX_NUM_OF_RUNNING_COMPUTING_UNITS_PER_USER
--set texeraEnvVars[5].value="2" \
# CPU_OPTION_FOR_COMPUTING_UNIT
--set texeraEnvVars[6].value="1,2,4" \
# MEMORY_OPTION_FOR_COMPUTING_UNIT
--set texeraEnvVars[7].value="2Gi,4Gi,16Gi" \
# GPU_LIMIT_OPTIONS
--set texeraEnvVars[8].value="0,1" \ # to allow 0 or 1 GPU resource to be allocated
```

### Adjusting Number of Pods
Scale out individual services for high availability or increased performance:

```
--set webserver.numOfPods=2 \
--set workflowCompilingService.numOfPods=2 \
--set pythonLanguageServer.replicaCount=2 \
```


### Retaining User Data
By default, all user data stored by Texera will be deleted when the cluster deployment is removed. Since user data is valuable, you can preserve all datasets and files even after uninstalling the cluster by setting:
```
--set persistence.removeAfterUninstall=false
```
