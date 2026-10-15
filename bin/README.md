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

# `bin/`

This directory holds the scripts, Dockerfiles, and configuration used to
develop, build, and deploy Texera. Most scripts expect to be run **from the
`texera` project root** (the parent of this directory).

## Local development

| Entry point | Purpose |
| --- | --- |
| `local-dev.sh` | Single entry point for the local dev stack — brings infra up/down in Docker while backend, frontend, and agent-service run natively. Run `bin/local-dev.sh --help`. Implementation lives in [`local-dev/`](local-dev/README.md). |

## Deployment

| Entry point | Purpose |
| --- | --- |
| `single-node.sh` | Single entry point for the single-node Docker Compose stack. Run `bin/single-node.sh --help`. Implementation and setup docs live in [`single-node/`](single-node/README.md). |
| `k8s/` | Helm chart and values for the Kubernetes deployment. See [`k8s/README.md`](k8s/README.md). |

## Docker images

`dockerfiles/` collects the per-service Dockerfiles, e.g.
`texera-web-application.dockerfile`, `file-service.dockerfile`, and
`computing-unit-master.dockerfile`. Each builds one Texera microservice and
must be built with the project root as the Docker build context:

```bash
docker build -f bin/dockerfiles/texera-web-application.dockerfile -t your-repo/texera-web-application:test .
```

### Reproducible builds

Built from the same commit with the same builder, an image comes out byte for
byte the same. Everything that would otherwise drift is pinned:

| Input | Pinned by |
| --- | --- |
| Base images | the digest in each `FROM` (Renovate refreshes them weekly) |
| apt and pip packages | `PACKAGE_SNAPSHOT`, a UTC date: [`dockerfiles/snapshot/install.sh`](dockerfiles/snapshot/install.sh) installs from snapshot.ubuntu.com / snapshot.debian.org and from PyPI as they stood at 00:00 that day |
| Timestamps (jars, dist zips, the frontend build number, `/etc/shadow`, layer mtimes) | `SOURCE_DATE_EPOCH`, the commit time |

`build-images.sh` and the image workflow pass both. To rebuild one image by
hand (after generating the jOOQ sources with `sbt DAO/jooqGenerate`):

```bash
export SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
docker buildx build --platform linux/amd64 \
  --build-arg SOURCE_DATE_EPOCH --build-arg PACKAGE_SNAPSHOT=$(date -u -d @$SOURCE_DATE_EPOCH +%F) \
  --output type=oci,dest=config-service.tar,rewrite-timestamp=true \
  -f bin/dockerfiles/config-service.dockerfile .
```

| Script | Purpose |
| --- | --- |
| `build-images.sh` | Convenience wrapper to build (and push) platform-dependent images. Run `bin/build-images.sh --help`. |
| `merge-image-tags.sh` | Merge per-platform image tags into a single multi-arch manifest. |

Prebuilt images published by the Texera team are on the
[Texera DockerHub repository](https://hub.docker.com/repositories/texera).

## Code generation & formatting

| Script | Purpose |
| --- | --- |
| `frontend-proto-gen.sh` | Generate the frontend (TypeScript) code from protobuf definitions. |
| `python-proto-gen.sh` | Generate the Python code from protobuf definitions. |
| `fix-format.sh` | Run the repository's code formatters. |
| `protoc-version.txt` | Pins the `protoc` version used by the proto-gen scripts. |

## Benchmarks

| Script | Purpose |
| --- | --- |
| `run-benchmarks.sh` | Single entry point for all Texera benchmarks; CI calls this script verbatim. |

## Licensing

`licensing/` contains the scripts that audit JAR licenses and generate the
binary `NOTICE`/`LICENSE` files (`audit_jar_licenses.py`,
`check_binary_deps.py`, `concat_license_binary.py`,
`generate_notice_binary.py`) plus their unit tests.

## Other components & configuration

| Path | Purpose |
| --- | --- |
| `utils/` | Shared shell helpers (`resolve-texera-home.sh`, `texera-logging.sh`) sourced by other scripts. |
| `pylsp/` | Dockerized Python language server used by the UDF editor. |
| `y-websocket-server/` | Dockerized Yjs websocket server backing collaborative editing. |
| `forum/` | Flarum-based community forum setup (install scripts and seed SQL). |
| `config.php`, `.htaccess` | Flarum runtime config and Apache rewrite rules for the forum. |
| `litellm-config.yaml` | LiteLLM proxy configuration for AI features. |
