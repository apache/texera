<!--
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
-->

# Helm template layout

Templates are grouped by **where they apply**, so a reader can tell at a glance
which resources are deployment-agnostic and which are tied to a particular
hosting environment. Helm renders every file under `templates/` recursively, so
these subdirectories are purely organizational — they do not change rendering.

| Folder | Contains | Renders when |
|--------|----------|--------------|
| `base/` | Resources every deployment needs: the Texera micro-service Deployments/Services, the Envoy Gateway + routes, Postgres/LakeFS/Lakekeeper wiring, the computing-unit pool, RBAC and namespaces. | Always. |
| `on-prem/` | Resources only used by a self-hosted / local deployment, e.g. the in-cluster MinIO persistence. | Gated on the relevant on-prem value (e.g. `minio.enabled`). |
| `aws/` | Resources only used on AWS/EKS, e.g. the external-S3 credentials Secret, the AWS NLB/EIP `EnvoyProxy`, and the autoscaler warm-pool placeholder. | Gated so they render to nothing off AWS (empty by default). |

Within `base/`, templates are further grouped into one subfolder per
component, named after the service it belongs to, so every manifest for a given
piece sits together:

```
base/
  access-control-service/   # access-control-service Deployment + Service
  agent-service/            # agent-service Deployment + Service + Secret + traffic policy
  config-service/
  file-service/
  gateway/                  # Envoy Gateway + routes + backends + security policy
  lakefs/
  lakekeeper/
  litellm/
  postgresql/               # in-cluster Postgres PV/PVC + init scripts
  webserver/                # dashboard / webserver Deployment + Service
  workflow-compiling-service/
  workflow-computing-unit-manager/   # the CU manager service + RBAC
  workflow-computing-unit-pool/      # CU pool namespace, quota, prepull, service
  example-data-loader/
  external-names/           # ExternalName service aliases
  pylsp/                    # python language server
  shared-editing-server/    # y-websocket collaborative editing
```

This nesting is also purely organizational — Helm still renders every file
recursively.

Guidelines for adding a template:
- Default to `base/`, in the subfolder for the component it belongs to (create
  a new one if it is a new component). Most resources are shared; only move a
  file out to `aws/`/`on-prem/` when it is genuinely specific to one hosting
  environment.
- Anything under `aws/` or `on-prem/` **must** be guarded by an `{{- if ... }}`
  on an opt-in value so that the default (on-prem) install is unaffected and an
  AWS install does not pick up on-prem-only resources.
