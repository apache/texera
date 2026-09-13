<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Texera local dev tooling

This folder backs `bin/local-dev.sh` — the single entry point for bringing
up, tearing down, building, and watching the Texera local dev stack on a
contributor's machine.

Run everything through `bin/local-dev.sh`:

```sh
bin/local-dev.sh up         # bring up the full stack (infra in docker, JVM + frontend native)
bin/local-dev.sh auto       # rebuild + bounce only services whose source changed
bin/local-dev.sh -i         # interactive Textual dashboard
bin/local-dev.sh --help     # full reference
```

Everything in this folder is wired up by the wrapper at `bin/local-dev.sh`;
you should not need to invoke any file inside `bin/local-dev/` directly.

## Supported platforms

macOS and Linux. Where the two differ, `main.sh` picks the right probe by
platform — there is nothing to configure:

| Concern | macOS | Linux |
| --- | --- | --- |
| Host LAN IP (the MinIO endpoint) | `route get default`, `ipconfig getifaddr` | `ip route show default`, `ip -4 addr show scope global` |
| Artifact mtime (`watch`'s ARTIFACT MTIME column) | BSD `stat -f` | GNU `stat -c` |
| Port → PID | `lsof` | `lsof`, falling back to `ss` |

On top of the toolchain in [AGENTS.md](../../AGENTS.md) — JDK 17, Scala 2.13,
Python 3.12, Node 24 — plus sbt and docker, Linux needs:

* **iproute2** (`ip`, `ss`). Essential on every mainstream distro, so normally
  already installed. `ip` is required: without it the host LAN IP cannot be
  detected and `up` refuses to start rather than publish an endpoint that only
  works from one side.
* **docker with the compose v2 plugin.** On stock Ubuntu that is
  `apt install docker.io docker-compose-v2`; `docker-compose-plugin` only
  exists in Docker's own apt repository.
* **your user in the `docker` group** — `sudo usermod -aG docker "$USER"`,
  then log in again. Group membership does not apply to shells that are
  already running, so a `docker compose` permission error right after that
  command is expected.

`HOST_LAN_IP` is detected for you; export it only to override the choice, e.g.
to pin one interface on a multi-homed host. The address has to be reachable
from **both** the host JVMs and the containers, which is why the scan skips
container bridges (`docker0`, `br-*`, `veth*`) and overlay/VPN interfaces
(tailscale, zerotier, wireguard): `172.17.0.1` looks like a perfectly good
local IPv4 but is not reachable from inside another container's network
namespace.

Email verification is on by default in the product, and refuses to issue a code when no
SMTP sender is configured rather than logging it. A local stack has no sender, so
`local-dev` exports `USER_SYS_EMAIL_VERIFICATION=false` and registration works as it
always did. To exercise the real flow, fill in the `USER_SYS_GOOGLE_SMTP_*` credentials
and `export USER_SYS_EMAIL_VERIFICATION=true` — an explicit export always wins over the
default set here.

## Layout

```
bin/local-dev/
├── main.sh                       shell engine — sbt builds, service lifecycle, port checks
├── tui.py                        Textual dashboard surfaced by `bin/local-dev.sh -i`
├── docker-compose.override.yml   overlay on top of bin/single-node/docker-compose.yml
│                                 (host-LAN-IP MinIO endpoint, Lakekeeper warehouse, etc.)
└── tests/
    ├── test_local_dev_sh.sh      bash smoke: license header, syntax, version, --help,
    │                             error-on-bad-input, regression guards
    └── test_local_dev_tui.py     pytest unit tests: version parsing, sbt-graph parsing,
                                  dirty detection, service-catalog invariants
```

## Running tests locally

```sh
bash bin/local-dev/tests/test_local_dev_sh.sh
python -m pytest bin/local-dev/tests/ -v
```

Both suites also run in CI under the `infra` job (`.github/workflows/build.yml`).
The job auto-discovers any `test_*.sh` under `bin/` (`find` + `bash`) and any
`test_*.py` (`pytest bin/`), so new tests dropped into this folder pick up
without a workflow edit.

## State directory

The script keeps logs, PIDs, build stamps, and animated phase markers under
`/tmp/texera-local-dev/` by default (override via the `TEXERA_LOCAL_DEV_DIR`
env var). It's safe to `rm -rf` between runs — it'll be recreated on the next
invocation.

## Rebuilding the Jupyter image

`jupyter` is the only managed service that runs from a Texera-built image instead of
natively, so edits to its customizations under
`notebook-migration-service/src/main/resources/` (`custom.js`, `custom.css`,
`start-texera-jupyter.sh`) do nothing until the image is rebuilt. CI publishes it, but a
local edit needs a local build under the same tag:

```sh
docker build -f bin/dockerfiles/jupyter.dockerfile -t ghcr.io/apache/texera-jupyter:latest .
bin/local-dev.sh up
```

Delete that local tag when you are done, otherwise it shadows the published image and you
keep running your old build:

```sh
docker rmi ghcr.io/apache/texera-jupyter:latest
```

## Adding a new managed service

1. Drop the launch command into `main.sh`'s `start_one` switch.
2. Add the row to the service catalog (port, type, sbt project, sibling group).
3. If it has its own source tree, add an entry so the dirty-source detector
   can hash it.
4. Add a row to `tui.py`'s service catalog for the dashboard.

The wrapper `bin/local-dev.sh` does not need to be touched.
