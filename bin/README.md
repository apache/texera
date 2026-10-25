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

# Texera Deployment

This directory contains Dockerfiles and configuration files for building and deploying Texera's microservices.

## Dockerfiles

This directory includes several Dockerfiles, such as `file-service.dockerfile` and `computing-unit-master.dockerfile`. Each Dockerfile builds a specific Texera microservice. All Dockerfiles must be built from the `texera` project root as the Docker build context.

For example, to build the image using `texera-web-application.dockerfile`, run the following command **from the project root**:

```bash
docker build -f bin/texera-web-application.dockerfile -t your-repo/texera-web-application:test .
```

Two shell scripts, `build-images.sh` and `build-services.sh` are included for building platform-dependent images conveniently. 

You can also find prebuilt images published by the Texera team on the [Texera DockerHub Repository](https://hub.docker.com/repositories/texera).

## Deployment using images

<<<<<<< HEAD
Subdirectories `single-node` and `k8s` contain configuration files for deploying Texera using the above Docker images.
=======
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
| `config.php`, `extend.php`, `.htaccess` | Flarum runtime config, extenders, and Apache rewrite rules for the forum. |
| `litellm-config.yaml` | LiteLLM proxy configuration for AI features. |
>>>>>>> d108cc19f (fix: strip the development forum's accounts and sessions from the Flarum seed (#9127))
