---
title: "Deploying Texera on Amazon Web Services (AWS)"
weight: 62
---

This document explains how to deploy Texera on Amazon Web Services (AWS) using an Elastic Kubernetes Service (EKS) cluster.

---

## 1. Create an EKS cluster on AWS
Go to the [EKS Console](https://console.aws.amazon.com/eks/home) and log in with your AWS account. Click "Create Cluster".

<img width="1759" alt="eks" src="https://github.com/user-attachments/assets/d115d4c1-bfd4-4565-9431-756cdfb54f68" />

---

Use the default configuration to create your cluster, and give it a name of your choice.
If "Cluster IAM role" and "Node IAM role" are empty, click "Create recommended role" and follow the guided steps.
Then, click "Create".

<img width="1653" alt="create" src="https://github.com/user-attachments/assets/a25d289d-d4be-4ba9-b024-fe51b65e1d6b" />

---

The cluster will take about 15–20 minutes to be created and reach an Active state. You can refresh the dashboard to monitor the progress.

<img width="1650" alt="dashboard" src="https://github.com/user-attachments/assets/93e47083-2c5a-48e4-95cd-97d9a73b19bd" />

## 2. Install AWS CLI and Helm
To access the cluster, you have the following 2 options:

1. Follow the [AWS CLI installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) to install the AWS CLI on your local enviornment.
    Once installed, create a passkey by clicking your account name in the top right corner → Security credentials.

    <img width="2250" alt="pass1" src="https://github.com/user-attachments/assets/7517ad37-6ec3-4a35-841a-554264320eed" />

    ---

    On the security credentials page, click "Create access key".

    <img width="1644" alt="pass2" src="https://github.com/user-attachments/assets/4a5327be-18df-4fb5-8f34-caf90f67bedf" />

    ---

    Follow the prompt and copy the access key and secret.

    <img width="1488" alt="pass3" src="https://github.com/user-attachments/assets/f64da71a-9461-4005-a2ea-ba8b24fd029f" />

    ---
    Then open a terminal on your computer and enter `aws configure`, paste the copied credentials.
    Also enter the default region shown on your cluster dashboard when prompted, leave the "output format" as its default value:

    <img width="1647" alt="conf1" src="https://github.com/user-attachments/assets/8ae32560-dcca-44ce-97db-00207e491773" />

    ---
2. Use [AWS CloudShell](https://aws.amazon.com/cloudshell/). Make sure the cloudshell is running on the same region with your cluster.

   <img width="2026" alt="cloudshell" src="https://github.com/user-attachments/assets/0de96a2d-7792-4758-bb00-35bd88e0edbe" />

   ---

Once you have a terminal (either local or CloudShell) ready to run `aws` commands, set the environment variable:

```
EKS_CLUSTER_NAME=<your-cluster-name>
```
Update your kubeconfig to use the new cluster:

```
aws eks update-kubeconfig --name $EKS_CLUSTER_NAME
```
Verify the connection:

```
kubectl get all
```

By default, AWS does not assign external IPs to LoadBalancers unless the subnets are properly tagged.
To enable both public and internal LoadBalancers, run the following command to tag your subnets:
```
aws ec2 create-tags \
  --resources $(aws eks describe-cluster --name $EKS_CLUSTER_NAME --query "cluster.resourcesVpcConfig.subnetIds" --output text) \
  --tags Key=kubernetes.io/role/elb,Value=1 Key=kubernetes.io/role/internal-elb,Value=1
```

## 3. Create two Nginx controllers for texera:

Install Helm by following the [Helm installation guide](https://helm.sh/docs/intro/install/). Then execute the following commands in your terminal:

```bash
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm repo update

helm upgrade --install nginx-texera ingress-nginx/ingress-nginx \
  --namespace texera --create-namespace \
  --set controller.replicaCount=1 \
  --set controller.ingressClassResource.name=nginx \
  --set controller.ingressClassResource.controllerValue="k8s.io/ingress-nginx" \
  --set controller.ingressClass=nginx \
  --set controller.service.type=LoadBalancer \
  --set controller.service.annotations."service\.beta\.kubernetes\.io/aws-load-balancer-scheme"="internet-facing"

helm upgrade --install nginx-minio ingress-nginx/ingress-nginx \
  --namespace texera --create-namespace \
  --set controller.replicaCount=1 \
  --set controller.ingressClassResource.name=nginx-minio \
  --set controller.ingressClassResource.controllerValue="k8s.io/nginx-minio" \
  --set controller.ingressClass=nginx-minio \
  --set controller.service.type=LoadBalancer \
  --set controller.service.annotations."service\.beta\.kubernetes\.io/aws-load-balancer-scheme"="internet-facing"
```

Wait for 1-2 minutes, then run:
```
kubectl get svc -n texera
```

When the EXTERNAL-IP fields are populated, assign the hostnames to environment variables:
```
TEXERA_IP=$(kubectl get svc nginx-texera-ingress-nginx-controller -n texera -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
MINIO_IP=$(kubectl get svc nginx-minio-ingress-nginx-controller -n texera -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
```

## 4. Create a StorageClass for Texera
Create a file named `ebs-storage.yaml` with the following content:
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: auto-ebs-sc
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.eks.amazonaws.com
volumeBindingMode: WaitForFirstConsumer
parameters:
  type: gp3
  encrypted: "true"
```
Then apply it:
```
kubectl apply -f ebs-storage.yaml
```
Validate the StorageClass:
```
kubectl get storageclass --all-namespaces
```

## 5. Prepare Texera deployment:
Execute the following bash commands.

```bash
curl -L -o texera.zip https://github.com/Texera/texera/releases/download/1.1.0/texera-cluster-1-1-0-release.zip
unzip texera.zip -d texera-cluster
rm texera.zip
helm dependency build texera-cluster
```

## 6. Deploy Texera

```bash
helm install texera texera-cluster --namespace texera --create-namespace \
  --set postgresql.primary.persistence.storageClass="auto-ebs-sc" \
  --set ingress-nginx.enabled=false \
  --set metrics-server.enabled=false \
  --set exampleDataLoader.enabled=false \
  --set minio.customIngress.enabled=true \
  --set minio.customIngress.ingressClassName=nginx-minio \
  --set minio.customIngress.texeraHostname="http://$TEXERA_IP" \
  --set minio.persistence.storageClass="auto-ebs-sc" \
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
