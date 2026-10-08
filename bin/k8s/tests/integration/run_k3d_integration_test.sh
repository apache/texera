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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Deploys the Helm chart in bin/k8s into a throwaway k3d cluster and runs
# k8s_integration_test.py against it through the Envoy gateway.
#
# Needs docker, k3d, kubectl, helm and python3 (with requirements.txt installed).
# Run from anywhere:
#   bin/k8s/tests/integration/run_k3d_integration_test.sh
#
# Environment:
#   IMAGE_REGISTRY   registry prefix of the Texera images  (default ghcr.io/apache)
#   IMAGE_TAG        tag of the Texera images              (default latest)
#   CLUSTER_NAME     k3d cluster name                      (default texera-it)
#   GATEWAY_PORT     host port mapped to the gateway       (default 8080)
#   K3S_IMAGE        node image                            (default rancher/k3s:v1.35.5-k3s1)
#   K3D_CREATE_ARGS  extra `k3d cluster create` arguments, e.g. --host-alias for a local resolver
#   KEEP_CLUSTER     "true" leaves the cluster up afterwards
#   DIAGNOSTICS_DIR  where pod state and logs go on failure (default ./k8s-integration-diagnostics)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

IMAGE_REGISTRY="${IMAGE_REGISTRY:-ghcr.io/apache}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CLUSTER_NAME="${CLUSTER_NAME:-texera-it}"
GATEWAY_PORT="${GATEWAY_PORT:-8080}"
K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:v1.35.5-k3s1}"
read -r -a K3D_CREATE_ARGS <<< "${K3D_CREATE_ARGS:-}"
KEEP_CLUSTER="${KEEP_CLUSTER:-false}"
DIAGNOSTICS_DIR="${DIAGNOSTICS_DIR:-$PWD/k8s-integration-diagnostics}"

# The gateway routes name their backends `texera-*`, so the release name is fixed.
RELEASE=texera
NAMESPACE=texera
CU_NAMESPACE=texera-workflow-computing-unit-pool
KUBE_CONTEXT="k3d-$CLUSTER_NAME"

SECONDS=0
log() { printf '\n[%4ds] %s\n' "$SECONDS" "$*"; }
kc() { kubectl --context "$KUBE_CONTEXT" "$@"; }

collect_diagnostics() {
  mkdir -p "$DIAGNOSTICS_DIR"
  log "Collecting diagnostics into $DIAGNOSTICS_DIR"
  kc get all,pvc,jobs,httproutes,gateways -A -o wide > "$DIAGNOSTICS_DIR/resources.txt" 2>&1 || true
  kc get events -A --sort-by=.lastTimestamp > "$DIAGNOSTICS_DIR/events.txt" 2>&1 || true
  kc describe pods -n "$NAMESPACE" > "$DIAGNOSTICS_DIR/describe-$NAMESPACE.txt" 2>&1 || true
  kc describe pods -n "$CU_NAMESPACE" > "$DIAGNOSTICS_DIR/describe-$CU_NAMESPACE.txt" 2>&1 || true
  local ns pod
  for ns in "$NAMESPACE" "$CU_NAMESPACE"; do
    for pod in $(kc get pods -n "$ns" -o name 2>/dev/null); do
      kc logs -n "$ns" "$pod" --all-containers --prefix --tail=2000 \
        > "$DIAGNOSTICS_DIR/logs-$ns-${pod#pod/}.txt" 2>&1 || true
    done
  done
  kc get pods -A -o wide || true
}

cleanup() {
  local status=$?
  if [ "$status" -ne 0 ]; then
    collect_diagnostics
  fi
  if [ "$KEEP_CLUSTER" != "true" ]; then
    log "Deleting k3d cluster $CLUSTER_NAME"
    k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  log "Finished with exit status $status"
  exit "$status"
}
trap cleanup EXIT

log "Creating k3d cluster $CLUSTER_NAME (gateway on localhost:$GATEWAY_PORT)"
k3d cluster delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
k3d cluster create "$CLUSTER_NAME" \
  --image "$K3S_IMAGE" \
  --agents 0 \
  --k3s-arg "--disable=traefik@server:0" \
  --port "$GATEWAY_PORT:80@loadbalancer" \
  --wait --timeout 300s \
  "${K3D_CREATE_ARGS[@]}"
kc wait --for=condition=Ready node --all --timeout=300s

log "Building chart dependencies"
helm dependency update "$CHART_DIR"

log "Installing $RELEASE with $IMAGE_REGISTRY/*:$IMAGE_TAG"
helm --kube-context "$KUBE_CONTEXT" install "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" --create-namespace \
  --values "$SCRIPT_DIR/values-ci.yaml" \
  --set texera.imageRegistry="$IMAGE_REGISTRY" \
  --set texera.imageTag="$IMAGE_TAG" \
  --timeout 20m

log "Waiting for the deployments to roll out"
DEPLOYMENTS=(
  envoy-gateway
  "$RELEASE-lakefs"
  "$RELEASE-lakekeeper"
  "$RELEASE-rustfs"
  "$RELEASE-webserver"
  "$RELEASE-file-service"
  "$RELEASE-config-service"
  "$RELEASE-access-control-service"
  "$RELEASE-workflow-compiling-service"
  "$RELEASE-workflow-computing-unit-manager"
)
for deployment in "${DEPLOYMENTS[@]}"; do
  kc rollout status "deployment/$deployment" -n "$NAMESPACE" --timeout=20m
done
kc wait --for=condition=complete "job/$RELEASE-lakekeeper-init" -n "$NAMESPACE" --timeout=10m

# Envoy Gateway creates the proxy deployment for the Gateway resource itself, so it only
# exists once the controller has reconciled it.
log "Waiting for the Envoy proxy"
for _ in $(seq 1 60); do
  proxy=$(kc get deployment -n "$NAMESPACE" \
    -l gateway.envoyproxy.io/owning-gateway-name="$RELEASE-gateway" -o name 2>/dev/null || true)
  [ -n "$proxy" ] && break
  sleep 5
done
[ -n "$proxy" ] || { echo "Envoy proxy deployment never appeared" >&2; exit 1; }
kc rollout status "$proxy" -n "$NAMESPACE" --timeout=10m
log "Deployment ready"

log "Running the integration test"
TEXERA_URL="http://localhost:$GATEWAY_PORT" python3 "$SCRIPT_DIR/k8s_integration_test.py"
