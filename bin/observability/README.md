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

# Texera Observability Stack

Texera's backend services are instrumented with [OpenTelemetry](https://opentelemetry.io/)
and export OTLP (logs, metrics, and traces). This directory is an **optional,
bundled reference stack** that an operator can start alongside a single-node
deployment to collect and visualize that telemetry. It is opt-in: nothing here
runs unless you enable the observability compose profiles (see below), and
Texera emits no telemetry unless `OTEL_SDK_DISABLED=false` is set.

## Topology

A single OpenTelemetry Collector is the only OTLP ingress. It fans each signal
out to a dedicated backend, and Grafana sits on top as the query UI:

```
                          Texera services
                                │  OTLP/gRPC (4317) or OTLP/HTTP (4318)
                                ▼
                         otel-collector
          ┌──────────────┬──────┴────────┬───────────────┐
   OTLP   │       remote-write    OTLP    │               │  (eBPF agent push)
          ▼              ▼                ▼               ▼
        Loki      VictoriaMetrics      Jaeger          Parca
       (logs)       (metrics)         (traces)       (profiles)
          │              │                │
          └──────────────┼────────────────┘
                         ▼
                      Grafana
             (reads Loki + VictoriaMetrics + Jaeger)
```

- **Loki** stores logs; the collector exports OTLP to `http://loki:3100/otlp`.
- **VictoriaMetrics** stores metrics; the collector exports Prometheus
  remote-write to `http://victoriametrics:8428/api/v1/write`.
- **Jaeger** stores traces; the collector exports OTLP to `jaeger:4317`.
- **Parca** stores continuous profiles, pushed by its eBPF agent (not read by
  Grafana; use the Parca UI on `:7070`).
- **Grafana** reads Loki, VictoriaMetrics (via the Prometheus query API), and
  Jaeger as pre-provisioned datasources.

## Starting it

The stack is driven by docker-compose profiles. Enable the signals you want in
`bin/single-node/.env` via `COMPOSE_PROFILES`, then bring the deployment up:

```
# bin/single-node/.env
COMPOSE_PROFILES=observability-collector,observability-logs,observability-metrics,observability-traces,observability-grafana
```

| Profile                   | Service(s)                              |
|---------------------------|-----------------------------------------|
| `observability-collector` | otel-collector (OTLP ingress; required) |
| `observability-logs`      | loki                                    |
| `observability-metrics`   | victoriametrics                         |
| `observability-traces`    | jaeger                                  |
| `observability-profiles`  | parca + privileged eBPF parca-agent     |
| `observability-grafana`   | grafana                                 |

Also set `OTEL_SDK_DISABLED=false` so the services actually emit telemetry, then:

```
cd bin/single-node && docker compose up -d
# or the dedicated entry point:
cd bin/observability && docker compose up -d
```

Reach **Grafana at http://localhost:3000**. The default login is
`admin` / `texera` (set via `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` in
`.env`); change it for anything reachable beyond localhost.

### Pre-provisioned datasources

Grafana comes with three datasources already wired up
(`grafana/provisioning/datasources/datasources.yaml`), so you can start querying
immediately:

- **Metrics** (default) - VictoriaMetrics at `http://victoriametrics:8428`,
  queried through the Prometheus API.
- **Loki** - logs at `http://loki:3100`.
- **Jaeger** - traces at `http://jaeger:16686`.

The backend query ports are also published on `127.0.0.1` for ad-hoc `curl`
debugging (Loki `3100`, VictoriaMetrics `8428`, Jaeger `16686`, Parca `7070`).

## Using your own stack

Nothing here is mandatory. The Collector speaks standard OTLP and
Prometheus remote-write, so an operator can point their own tooling at the same
endpoints instead of running the bundled backends:

- Point an **external Grafana** at the bundled Loki / VictoriaMetrics / Jaeger,
  or swap the backends entirely.
- Send logs to **OpenSearch** (or any OTLP-capable log store) by changing the
  collector's logs exporter in `otel-collector/config.yaml`.
- Replace the metrics or traces exporters with any Prometheus-remote-write or
  OTLP-compatible destination.
- Or drop the collector and have an external collector scrape/receive directly;
  the services only need an OTLP endpoint via `OTEL_EXPORTER_OTLP_ENDPOINT`.

## Licensing

> **NOTE:** **Grafana and Loki are licensed under the AGPLv3.** They are
> included here only as an **optional, operator-run** convenience and are **NOT
> bundled into, or distributed as part of, any Apache Texera release artifact**.
> An operator who runs them does so on their own; their images are pulled at
> deploy time, not shipped by the project.
>
> The rest of the stack - the **OpenTelemetry Collector, VictoriaMetrics,
> Jaeger, and Parca** - is **Apache-2.0** licensed, consistent with Texera
> itself. Only Apache-2.0 configuration files (collector, Loki, Grafana
> datasources, Parca) live in this repository.
