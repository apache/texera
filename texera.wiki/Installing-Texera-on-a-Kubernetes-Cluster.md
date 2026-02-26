This document describes the **five main parts** that users are expected to configure when deploying Texera using this Helm chart. All other values should generally be left with their defaults unless users are aware of specific customizations needed. 

---

## Prerequisites
Before configuring and deploying the Texera platform, ensure the following prerequisites are met:

1. **Kubernetes Cluster**: A working Kubernetes cluster (e.g., local `minikube`, or a cloud-based cluster). There should be at least 10 free CPU cores and 8 GB of RAM available. 
2. **Helm Installed**: Helm v3 or later must be installed on your system to deploy the chart.
3. **Custom Hostnames**: 
  - In production environment with HTTPS support, two valid hostnames must be available—one for the Texera services and another for MinIO access. For example, `texera.my.org` for Texera services and `minio.my.org` for Minio should be available for deployment and external access.
  - In testing environment, e.g. localhost or exposing services via HTTP: one valid hostname(i.e. IP address of the server or `localhost`) is enough. **Port `30080` and Port `31000` will be occupied by default in this setting**. To change the port occupation, see the below instructions.
4. **TLS Configuration**: You should either:
   - Have a pre-created TLS secret, or
   - Use [cert-manager](https://cert-manager.io/docs/tutorials/) with a valid Issuer.

---

## Configuration Location
All configuration options mentioned in this guide are defined in the `values.yaml` file located under the `deployment/k8s/texera-helmchart` directory.

## 1. Username and Password Configuration
Credentials are used across different components such as PostgreSQL, MinIO, and LakeFS.

### PostgreSQL
```yaml
postgresql:
  auth:
    postgresPassword: root_password
```
- **postgresPassword**: The superuser password used during database initialization. Required by LakeFS and Texera backend services.

### MinIO
```yaml
minio:
  auth:
    rootUser: texera_minio
    rootPassword: password
```
- **rootUser** and **rootPassword**: Credentials used to access MinIO. These must match the S3 credentials provided to LakeFS.

### LakeFS Secrets
```yaml
lakefs:
  secrets:
    authEncryptSecretKey: random_string_for_lakefs
    databaseConnectionString: postgres://postgres:root_password@texera-postgresql:5432/texera_lakefs?sslmode=disable
  auth:
    username: texera-admin
    accessKey: AKIAIOSFOLKFSSAMPLES
    secretKey: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```
- Ensure `databaseConnectionString` includes the correct PostgreSQL credentials.
- `auth` decides the credentials of initializing LakeFS's admin user and accessing LakeFS via API calls.

### Texera Environment Variables
```yaml
texeraEnvVars:
  - name: STORAGE_JDBC_USERNAME
    value: postgres
  - name: USER_SYS_ENABLED
    value: "true"
  - name: MAX_NUM_OF_RUNNING_COMPUTING_UNITS_PER_USER
    value: "10"
```
- These variables must reflect the credentials you configured for PostgreSQL, i.e. the user of the PostgreSQL.
- These variables control the behavior of the Texera system.
---

## 2. Custom Hostnames and TLS Configuration
Customize the domain names used for accessing Texera services via Ingress. TLS is optional but recommended for production.

### Ingress Hostnames and TLS
```yaml
ingressPaths:
  enabled: true
  hostname: "localhost"
  tlsSecretName: ""       # Optional TLS secret
  issuer: ""              # Optional cert-manager issuer
```
- **hostname**: Set this to the custom domain for your Texera deployment (e.g., `texera.example.com`).
- **tlsSecretName**: Optional. Set to the name of a Kubernetes TLS secret if using HTTPS.
- **issuer**: Optional. Set to a cert-manager issuer if certificates should be managed automatically.

### MinIO Ingress
```yaml
minio:
  ingress:
    hostname: "localhost"
    tlsSecretName: ""
    issuer: ""
```
- These settings follow the same rules as Texera's ingress. Configure `hostname`, `tlsSecretName`, and `issuer` as needed.

---

## 3. CPU and Memory Resource Configuration
Adjust resource requests to fit your cluster's capacity.

### PostgreSQL
```yaml
postgresql:
  primary:
    resources:
      requests:
        cpu: "4"
        memory: "4Gi"
```
- Tune based on expected database workload.

> Other components (e.g., webserver, file service, envoy, and language servers) use default resource settings and are not expected to be changed.

---

## 4. Custom Storage Class Configuration
The chart defaults to using `local-path` which may not be suitable for all clusters.

### PostgreSQL
```yaml
postgresql:
  primary:
    persistence:
      enabled: true
      size: 10Gi
      storageClass: local-path
```

### MinIO
```yaml
minio:
  persistence:
    enabled: true
    size: 20Gi
    storageClass: local-path
```

- Replace `local-path` with your cluster's preferred StorageClass (e.g., `gp2`, `standard`, etc.).

---

## 5. Number of Replicas
You can scale some components by changing their replica count using the parameters below:

### Components with Replica Settings
```yaml
webserver:
  numOfPods: 1

yWebsocketServer:
  replicaCount: 1

pythonLanguageServer:
  replicaCount: 8

envoy:
  replicas: 1

workflowComputingUnitManager:
  numOfPods: 1

workflowCompilingService:
  numOfPods: 1

fileService:
  numOfPods: 1
```
- Increase these values to scale each component horizontally based on workload needs.

---

## 6. Custom Ports for testing environment
By default, Texera services will occupy port `30080`, and Minio will occupy `31000`. If you want to change it, go to the corresponding sections in the `values.yaml`:
```yaml
minio:
  service:
    type: NodePort
    nodePorts:
      api: 31000 # change here
```
```yaml
ingress-nginx:
  controller:
    replicaCount: 1
    service:
      type: NodePort
      nodePorts:
        http: 30080 # change here
```

---

By configuring these five areas—**credentials**, **hostnames/TLS**, **resources**, **storage classes**, and **replica counts**—you can tailor the deployment to suit your environment while relying on sane defaults for all other settings.

> If you are installing Texera in your local Kubernetes environment, you don't need to change any of the above configurations unless needed.

---

## Installing and Uninstalling Texera

### Launch the Whole Stack
Run the following command from the root directory of the repository:

```bash
helm install texera texera-helmchart --namespace texera-dev --create-namespace
```

This will:
- Create a Helm release named `texera`
- Create a namespace named `texera-dev`
- Deploy all Texera components under that namespace

Please wait about **1-3 minutes** for all pods to be ready. Once the deployment is complete, Texera should be accessible at:

```
http://<your-hostname-for-texera>
```

> **Note**: If you're using a non-default kubeconfig file, append `--kubeconfig /path/to/your/kubeconfig` to the Helm command.

### Terminate the Whole Stack
To uninstall Texera and clean up all related resources:

```bash
helm uninstall texera --namespace texera-dev
```

