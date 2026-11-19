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

# Parca profiles

Configuration for the **profiles** signal in the Texera observability
stack: the Parca server and its eBPF agent. The compose services that
run them are in `bin/single-node/docker-compose.yml`; this directory
holds only their configuration.

Both components are Apache-2.0 (see
[`docs/observability/LICENSING.md`](../../../docs/observability/LICENSING.md)):

| File | Component | Image |
|---|---|---|
| `parca.yaml` | Parca server v0.28.0 | `ghcr.io/parca-dev/parca:v0.28.0` |
| `parca-agent.env` | Parca eBPF agent v0.47.1 | `ghcr.io/parca-dev/parca-agent:v0.47.1` |

## Host requirements

The agent uses eBPF to sample stack traces, which constrains the host:

- **Linux only.** eBPF is a Linux kernel feature; the agent cannot run
  on macOS or Windows. The rest of the stack (logs, metrics, traces)
  is cross-platform.
- **Privileged container.** The agent needs `CAP_SYS_ADMIN`-class
  permissions to load eBPF programs and open the perf-event facility,
  so the compose service runs `privileged: true` and bind-mounts
  `/sys/kernel/debug`, `/proc`, and `/sys` read-only.
- **Outbound only.** The agent writes to the bundled Parca server on
  `parca:7070` over plain gRPC on the docker bridge network and opens
  no other ports. Any deploy that exposes those ports to a wider
  network must add TLS at the compose/k8s layer.

## Opt-in

Profiling is off by default. It is enabled per host by selecting the
`profiles` profile before `docker compose up`; see `bin/single-node/.env`
for the flags. Enable it only after reviewing the privilege and scope
notes below.

## What gets profiled

The agent profiles **every process on the host**, not only Texera's.
It is not scoped to Texera's containers; doing that would require
configuring the agent against specific cgroups, which is not done
here. The static labels in `parca-agent.env` (`deployment=texera`,
`cluster=local`) only let a query filter which profiles are *shown*,
not what is *collected*. Treat enabling the agent as profiling the
whole machine.

Profiles are not labeled with `workflow.id` / `execution.id` (unbounded
identifiers that would blow up Parca's cardinality). The agent samples
stack traces and does not read application-level OpenTelemetry trace
ids, so profiles cannot be joined to individual traces by `trace_id`;
they correlate only by the coarse labels above and by time.
