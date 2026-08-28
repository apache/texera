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
services fall back to the in-cluster MinIO Service and its auto-generated
"<release>-minio" Secret, so the default install is unchanged.
*/}}

{{/* S3 endpoint URL. */}}
{{- define "texera.s3.endpoint" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.endpoint -}}
{{- else -}}
{{- printf "http://%s-minio:9000" .Release.Name -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the S3 credentials. */}}
{{- define "texera.s3.secretName" -}}
{{- if .Values.storage.s3.endpoint -}}
{{- .Values.storage.s3.existingSecret | default (printf "%s-s3-credentials" .Release.Name) -}}
{{- else -}}
{{- printf "%s-minio" .Release.Name -}}
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

