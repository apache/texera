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
