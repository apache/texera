#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# One-command install/upgrade of the Texera Helm chart.
#
# `helm install` on its own fails on a fresh checkout because the subchart
# dependencies (postgresql, minio, lakefs, envoy-gateway, lakekeeper,
# metrics-server) are declared in Chart.yaml but not vendored into charts/.
# This wrapper fetches them first (idempotent), then upgrades-or-installs.
#
# Usage (run from anywhere):
#   bin/k8s/install.sh                                  # app only
#   bin/k8s/install.sh --set observability.enabled=true # app + observability
#   RELEASE=texera NAMESPACE=texera bin/k8s/install.sh --set observability.enabled=true
#
# Any extra args are forwarded verbatim to `helm upgrade --install`, so
# --set / -f values.override.yaml / --namespace etc. all work.

set -euo pipefail

CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RELEASE="${RELEASE:-texera}"
NAMESPACE="${NAMESPACE:-texera}"

command -v helm >/dev/null 2>&1 || { echo "helm not found on PATH" >&2; exit 1; }

# Fetch/refresh subcharts into charts/. `dependency build` uses Chart.lock when
# present (reproducible); it falls back to `dependency update` if there is no
# lock yet. Both are safe to re-run.
echo "==> Resolving chart dependencies"
if [ -f "${CHART_DIR}/Chart.lock" ]; then
  helm dependency build "${CHART_DIR}"
else
  helm dependency update "${CHART_DIR}"
fi

echo "==> helm upgrade --install ${RELEASE} (namespace: ${NAMESPACE})"
exec helm upgrade --install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" \
  --create-namespace \
  "$@"
