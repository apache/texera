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

# Parca profiles (PR 5)

This directory holds configuration for the **profiles** signal in the
Texera observability stack. The compose service definitions that
consume these files land in PR 6; PR 5 ships the configuration only,
so the agent's deploy posture can be reviewed in isolation from the
broader compose changes.

Components — both Apache-2.0 (see
[`docs/observability/LICENSING.md`](../../../docs/observability/LICENSING.md)):

| File | Component | Image |
|---|---|---|
| `parca.yaml` | Parca server v0.28.0 | `ghcr.io/parca-dev/parca:v0.28.0` |
| `parca-agent.env` | Parca eBPF agent v0.47.1 | `ghcr.io/parca-dev/parca-agent:v0.47.1` |

## Deploy posture

The Parca agent uses eBPF to sample stack traces from running
processes. That puts a few non-negotiable requirements on the host:

- **Linux only.** eBPF is a Linux kernel feature. macOS and Windows
  developers cannot run the agent; the rest of the observability
  stack (logs, metrics, traces) works on all platforms.
- **Privileged container.** The agent needs `CAP_SYS_ADMIN`-class
  permissions to load eBPF programs and mount the perf-event
  facility. The PR 6 compose service will set `privileged: true`
  and bind-mount `/sys/kernel/debug`, `/proc`, and `/sys` read-only
  into the container.
- **Read-only on host filesystems.** The bind-mounts above are
  `ro` — the agent reads kernel state but cannot write to it. No
  network exposure outside the cluster: the agent only opens an
  outbound connection to the bundled Parca server on
  `parca:7070`.

## Opt-out

For developers on non-Linux dev machines, or for any deploy that
chooses not to run profiles, set this in the host environment before
`docker compose up`:

```
TEXERA_OBSERVABILITY_PROFILES=disabled
```

PR 6's compose file gates the `parca-agent` (and optionally the
`parca` server too) on this flag — the rest of the stack continues
to run with `disabled` panels in the UI.

## What gets profiled

The agent's default behaviour is to discover and profile every
process on the host. We attach two static labels via
`parca-agent.env`:

- `deployment=texera`
- `cluster=local` (override per env)

When the PR 7 Texera query gateway runs Parca queries, it filters on
`deployment=texera` so the dashboard only ever shows Texera-process
profiles, never the operator's other workloads.

We do **not** label profiles with `workflow.id` / `execution.id`. As
with metrics, those are unbounded identifiers and would blow up
Parca's storage cardinality. Per-execution profile views are reached
by joining on `trace_id` at query time (the Parca query API supports
this).

## What is not in PR 5

- The docker-compose service definitions (PR 6).
- The Angular flame-graph panel that renders pprof data (PR 11 in
  the [PR plan](../../../docs/observability/PR-PLAN.md)).
- Kubernetes Helm templates for the agent DaemonSet (deferred to a
  later series — single-node compose first).
- TLS between agent and server. The agent dials Parca over plain
  gRPC because the bundled deployment binds both to the docker
  bridge network. Any deploy that opens those ports to a wider
  network must enable TLS at the compose / k8s layer.
