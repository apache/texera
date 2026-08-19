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
Sub-chart resource names.

The postgresql and minio sub-charts are pinned to fixed names via
fullnameOverride in values.yaml, because sibling sub-charts (lakefs,
lakekeeper) must address them from *their own* values -- and Helm does not
template values files, so those references cannot be release-derived. These
helpers resolve the same name the sub-chart itself computes, falling back to
the default "<release>-<chart>" when the override is cleared.
*/}}

{{- define "texera.postgresql.fullname" -}}
{{- .Values.postgresql.fullnameOverride | default (printf "%s-postgresql" .Release.Name) -}}
{{- end -}}

{{- define "texera.minio.fullname" -}}
{{- .Values.minio.fullnameOverride | default (printf "%s-minio" .Release.Name) -}}
{{- end -}}

{{/*
Object-storage (S3) resolution helpers.

When storage.s3.endpoint is set the services talk to that external
S3-compatible store (credentials come from storage.s3.existingSecret, or the
chart-generated "texera-s3-credentials" Secret). When it is empty the services
fall back to the in-cluster MinIO Service and its auto-generated Secret, so the
default install is unchanged.
*/}}

{{/* S3 endpoint URL. */}}
{{- define "texera.s3.endpoint" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.endpoint -}}
{{- else -}}
{{- printf "http://%s:9000" (include "texera.minio.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the S3 credentials. */}}
{{- define "texera.s3.secretName" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.existingSecret | default "texera-s3-credentials" -}}
{{- else -}}
{{- include "texera.minio.fullname" . -}}
{{- end -}}
{{- end -}}

{{/* Secret data key for the S3 access key id. */}}
{{- define "texera.s3.accessKeyIdKey" -}}
{{- if .Values.storage.s3.endpoint -}}access-key-id{{- else -}}root-user{{- end -}}
{{- end -}}

{{/* Secret data key for the S3 secret access key. */}}
{{- define "texera.s3.secretAccessKeyKey" -}}
{{- if .Values.storage.s3.endpoint -}}secret-access-key{{- else -}}root-password{{- end -}}
{{- end -}}
