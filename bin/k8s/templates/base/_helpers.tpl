{{/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/}}

{{/*
Object-storage (S3) resolution helpers.

When storage.s3.endpoint is set the services talk to that external
S3-compatible store (credentials come from storage.s3.existingSecret, or a
chart-generated "<release>-s3-credentials" Secret). When it is empty the
services fall back to the in-cluster RustFS Service and its auto-generated
"<release>-rustfs-secret" Secret, so the default install is unchanged.
*/}}

{{/* S3 endpoint URL. */}}
{{- define "texera.s3.endpoint" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.endpoint -}}
{{- else -}}
{{- printf "http://%s-rustfs-svc:9000" .Release.Name -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the S3 credentials. */}}
{{- define "texera.s3.secretName" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.existingSecret | default (printf "%s-s3-credentials" .Release.Name) -}}
{{- else -}}
{{- printf "%s-rustfs-secret" .Release.Name -}}
{{- end -}}
{{- end -}}

{{/* Secret data key for the S3 access key id. */}}
{{- define "texera.s3.accessKeyIdKey" -}}
{{- if .Values.storage.s3.endpoint -}}access-key-id{{- else -}}RUSTFS_ACCESS_KEY{{- end -}}
{{- end -}}

{{/* Secret data key for the S3 secret access key. */}}
{{- define "texera.s3.secretAccessKeyKey" -}}
{{- if .Values.storage.s3.endpoint -}}secret-access-key{{- else -}}RUSTFS_SECRET_KEY{{- end -}}
{{- end -}}

{{/*
Observability emission env for a Scala service pod.

Renders OTEL_EXPORTER_OTLP_ENDPOINT (pointing at the in-cluster OTel
Collector Service) plus TEXERA_OTEL_ALLOWED_HOSTS. OtelInit validates the
endpoint host against an allowlist whose default is localhost only, so the
collector's Service name must be added explicitly or the SDK rejects it and
emits nothing. Renders nothing unless both the observability stack and the
collector are enabled, so the default install is unchanged. Include inside a
container's `env:` list, e.g. `{{- include "texera.observability.env" . | nindent 12 }}`.
*/}}
{{- define "texera.observability.env" -}}
{{- if and .Values.observability.enabled .Values.observability.collector.enabled }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: "http://{{ .Release.Name }}-otel-collector:{{ .Values.observability.collector.grpcPort }}"
- name: TEXERA_OTEL_ALLOWED_HOSTS
  value: "{{ .Release.Name }}-otel-collector"
{{- end }}
{{- end -}}

{{/*
The audience the mounter's service-account tokens are bound to. Fixed rather than
configurable: it is one half of a credential contract between access-control-service and the
mounter -- the projected token declares it and the mounter's TokenReview requires it -- so
the two must always agree, and there is no deployment in which a different value is useful.
*/}}
{{- define "texera.mounter.audience" -}}
texera-mounter
{{- end -}}

{{/*
The service-account username allowed to request a mount from the per-node mounter, in the
form MOUNTER_ALLOWED_CALLERS expects. This is access-control-service and nothing else: it
is the deployment's authorization authority for computing units, so it is the one component
that can decide whether a given user may mount into a given CU. Deliberately not
configurable -- widening it is a security decision, not a deployment preference, and a
values override would let an install quietly hand the privileged mounter to another caller.
*/}}
{{- define "texera.mounter.allowedCallers" -}}
{{- printf "system:serviceaccount:%s:%s" .Release.Namespace .Values.accessControlService.serviceAccountName -}}
{{- end -}}

{{/* Jupyter base path, as exactly one leading slash and no trailing one. Several places
append the uid to it, so a bare value would render "http://<origin>jupyter/7". */}}
{{- define "texera.jupyter.basePath" -}}
{{- printf "/%s" (trimAll "/" .Values.jupyterPool.basePath) -}}
{{- end -}}

{{/* Start a release-scoped Deployment; the caller supplies the pod spec. */}}
{{- define "texera.deploymentHeader" -}}
{{- $app := printf "%s-" .release.Name -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $app }}{{ .name }}
  namespace: {{ .release.Namespace }}
  labels:
    app: {{ $app }}{{ .name }}
spec:
  replicas: {{ .replicas }}
  {{- if .recreate }}
  # Single writer bound to one RWO volume; never run two at once.
  strategy:
    type: Recreate
  {{- end }}
  selector:
    matchLabels:
      app: {{ $app }}{{ .name }}
  template:
    metadata:
      labels:
        app: {{ $app }}{{ .name }}
{{- end -}}

{{- define "texera.observability.containerSecurityContext" -}}
securityContext:
  {{- if . }}
  readOnlyRootFilesystem: true
  {{- end }}
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]
{{- end -}}

{{- define "texera.observability.service" -}}
{{- $app := printf "%s-%s" .release.Name .app -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ $app }}
  namespace: {{ .release.Namespace }}
  labels:
    app: {{ $app }}
spec:
  type: ClusterIP
  selector:
    app: {{ $app }}
  ports:
    {{- range .ports }}
    - name: {{ .name }}
      protocol: TCP
      port: {{ .port }}
      targetPort: {{ .port }}
    {{- end }}
{{- end -}}

{{- define "texera.observability.dataClaim" -}}
{{- $app := printf "%s-%s" .release.Name .app -}}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ $app }}-data
  namespace: {{ .release.Namespace }}
  labels:
    app: {{ $app }}
spec:
  accessModes: ["ReadWriteOnce"]
  {{- if .storage.storageClass }}
  storageClassName: {{ .storage.storageClass }}
  {{- end }}
  resources:
    requests:
      storage: {{ .storage.storage }}
{{- end -}}

{{/* Grafana datasource provisioning body. Kept as a helper so the Deployment's
checksum/config annotation can hash just this content; hashing the template that
holds the annotation would make it reference itself and recurse. */}}
{{- define "texera.observability.grafanaDatasources" -}}
apiVersion: 1
datasources:
  # Metrics: VictoriaMetrics speaks the Prometheus query API. Default.
  - name: Metrics
    type: prometheus
    uid: metrics
    access: proxy
    url: http://{{ .Release.Name }}-victoriametrics:{{ .Values.observability.metrics.port }}
    isDefault: true
    editable: false
{{- if .Values.observability.logs.enabled }}
  # Logs: Loki, LogQL over OTLP-ingested logs.
  - name: Loki
    type: loki
    uid: loki
    access: proxy
    url: http://{{ .Release.Name }}-loki:{{ .Values.observability.logs.port }}
    editable: false
{{- end }}
{{- if .Values.observability.traces.enabled }}
  # Traces: Jaeger query API / UI backend.
  - name: Jaeger
    type: jaeger
    uid: jaeger
    access: proxy
    url: http://{{ .Release.Name }}-jaeger:{{ .Values.observability.traces.queryPort }}
    editable: false
{{- end }}
{{- end -}}

{{/* Loki server config body. A helper for the same reason as the Grafana one,
and so the checksum/config annotation hashes the config instead of its template.
Retention is enforced here: the compactor deletes chunks older than
observability.logs.retentionPeriod, which Loki otherwise ignores. */}}
{{- define "texera.observability.lokiConfig" -}}
# No multi-tenancy: one implicit tenant for the in-cluster stack.
auth_enabled: false
server:
  http_listen_port: {{ .Values.observability.logs.port }}
  grpc_listen_port: 9096
common:
  instance_addr: 127.0.0.1
  # Every component's data path defaults under this prefix, the PVC at /loki.
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory
schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h
limits_config:
  # Required for OTLP ingest (OTLP attributes -> structured metadata).
  allow_structured_metadata: true
  reject_old_samples: false
  # Age after which log chunks are eligible for deletion by the compactor.
  retention_period: {{ .Values.observability.logs.retentionPeriod }}
compactor:
  working_directory: /loki/compactor
  # Without this the retention_period above is parsed but never enforced,
  # so logs accumulate until the volume fills.
  retention_enabled: true
  delete_request_store: filesystem
{{- end -}}
{{- define "texera.releaseService" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ .name }}-svc
  namespace: {{ .release.Namespace }}
spec:
  type: {{ .serviceType }}
  selector:
    app: {{ .release.Name }}-{{ .name }}
  ports:
    {{- if .portName }}
    - name: {{ .portName }}
      protocol: TCP
    {{- else }}
    - protocol: TCP
    {{- end }}
      port: {{ .port }}
      targetPort: {{ .port }}
      {{- if .useNodePort }}
      # if service type is set to NodePort, include nodePort attribute
      {{- if eq .serviceType "NodePort" }}
      nodePort: {{ .nodePort }}
      {{- end }}
      {{- end }}
{{- end -}}

{{- define "texera.localPersistence" -}}
{{- $hostBasePath := .hostBasePath }}
{{- $name := .name }}
{{- $persistence := .persistence }}
{{- $volumeName := printf "%s-data-pv" $name }}
{{- $claimName := printf "%s-data-pvc" $name }}
{{- $storageClass := $persistence.storageClass | default "local-path" }}
{{- $size := $persistence.size | default .defaultSize }}
{{- $hostPath := printf "%s/%s/%s" $hostBasePath $.root.Release.Name $name }}

{{/* Only create PV for local-path storage class */}}
{{- if and (eq $storageClass "local-path") (ne $hostBasePath "") }}
apiVersion: v1
kind: PersistentVolume
metadata:
  name: {{ $volumeName }}
  {{- if not $.root.Values.persistence.removeAfterUninstall }}
  annotations:
    "helm.sh/resource-policy": keep
  {{- end }}
  labels:
    type: local
    app: {{ $.root.Release.Name }}
    component: {{ $name }}
spec:
  storageClassName: {{ $storageClass }}
  capacity:
    storage: {{ $size }}
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  hostPath:
    path: {{ $hostPath }}
---
{{- end }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ $claimName }}
  namespace: {{ $.root.Release.Namespace }}
  {{- if not $.root.Values.persistence.removeAfterUninstall }}
  annotations:
    "helm.sh/resource-policy": keep
  {{- end }}
  labels:
    app: {{ $.root.Release.Name }}
    component: {{ $name }}
spec:
  storageClassName: {{ $storageClass }}
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: {{ $size }}
  {{- if and (eq $storageClass "local-path") (ne $hostBasePath "") }}
  volumeName: {{ $volumeName }}
  {{- end }}
{{- end }}
