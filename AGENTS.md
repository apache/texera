# AGENTS.md (Texera)

Quick orientation for agents working on Apache Texera (Incubating). Pair this file with the root `README.md` and the developer wiki linked from it.

## Big picture (modules + service boundaries)
- Repo is an **sbt multi-project Scala backend** (Scala 2.13.12, JDK-based) plus an **Angular frontend** under `frontend/`.
- Backend services (Dropwizard/Jersey) live at the top level and share code under `common/`. The sbt module graph is defined in `build.sbt`; note that sbt project names do not always match folder names (e.g., `amber/` is the sbt project `WorkflowExecutionService`).
- Shared libraries (`common/`):
  - `common/dao` (DAO), `common/config` (Config), `common/auth` (Auth, JWT setup), `common/workflow-core` (WorkflowCore), `common/workflow-operator` (WorkflowOperator, operator definitions and descriptors), `common/pybuilder` (PyBuilder — a `pyb"..."` macro DSL for composing Python code for Python operators).
- Services (top-level folders):
  - `amber/` — main web application + workflow execution engine ("Amber" actor-based dataflow runtime). Serves the Angular GUI, REST API, and the collaboration WebSocket.
  - `workflow-compiling-service/` — compiles workflow JSON into executable plans.
  - `file-service/` — datasets/files (works with LakeFS / Iceberg catalogs, see `sql/`).
  - `config-service/` — runtime configuration.
  - `access-control-service/` — ACL + the AI assistant chat/completion endpoints.
  - `computing-unit-managing-service/` — lifecycle of compute units (master/worker pods, scaling).
  - `pyright-language-service/` — Pyright-backed language server for Python UDF editing.
- Python runtime companion: `amber/src/main/python/` (`pytexera`, `pyamber`, `core`, `proto`, `texera_run_python_worker.py`) — used by Python workers spawned by the Amber engine.
- A secondary, source-only copy of the operator library lives at `core/workflow-operator/src/...` (legacy / build artifact source; prefer `common/workflow-operator` for new code).
- Build note: `build.sbt` injects ASF licensing files (`LICENSE`, `NOTICE`, `DISCLAIMER-WIP`) into `META-INF/` of every JAR via `asfLicensingSettings`.

## Service port map (default config)
| Service | App port | Admin port | Source config |
| --- | --- | --- | --- |
| amber (`TexeraWebApplication`) | 8080 | 8081 | `amber/src/main/resources/web-config.yml` |
| workflow-compiling-service | 9090 | — | `workflow-compiling-service/src/main/resources/workflow-compiling-service-config.yaml` |
| file-service | 9092 | — | `file-service/src/main/resources/file-service-web-config.yaml` |
| config-service | 9094 | — | `config-service/src/main/resources/config-service-web-config.yaml` |
| access-control-service (AI assistant, models) | 9096 | — | `access-control-service/src/main/resources/access-control-service-web-config.yaml` |
| computing-unit-managing-service | 8888 | 8082 | `computing-unit-managing-service/src/main/resources/computing-unit-managing-service-config.yaml` |
| WebSocket (collaboration, `/wsapi`) | 8085 | — | served by `amber` |
| y-websocket (shared editing `/rtc`) | 1234 | — | `bin/shared-editing-server.sh`, `bin/y-websocket-server/` |

Frontend dev proxy routing (`frontend/proxy.config.json`) mirrors this split — e.g., `/api/compile` → 9090, `/api/dataset` → 9092, `/api/config/**` → 9094, `/api/models` and `/api/chat/completion` → 9096, `/api/computing-unit` → 8888, everything else `/api` → 8080.

## Runtime flow & cross-component communication
- **REST base path:** every service mounts Jersey at `/api/*` (`environment.jersey.setUrlPattern("/api/*")` in each `Application.run`).
- **Web GUI serving:** `amber` serves Angular static output via `FileAssetsBundle("../../frontend/dist", "/", "index.html")` and redirects 404s to `/` so Angular client-side routing works (`TexeraWebApplication.scala`).
- **WebSockets:** collaboration is wired through Dropwizard `WebsocketBundle(classOf[CollaborationResource])`; the Jetty WS idle timeout is explicitly set to 1 hour via `WebSocketUpgradeFilter` in `TexeraWebApplication.run(...)`.
- **Database init pattern:** services call `SqlServer.initConnection(StorageConfig.jdbcUrl, ...)` during startup (see `TexeraWebApplication.scala`, `WorkflowCompilingService.scala`, etc.). DDL lives in `sql/texera_ddl.sql`; Iceberg / LakeFS / Lakekeeper bootstrap SQL is also under `sql/`.
- **Auth:** JWT auth is installed via `setupJwtAuth(environment)` in `amber`, plus `AuthValueFactoryProvider.Binder[SessionUser]` and `RolesAllowedDynamicFeature`. Resources under `.../resource/auth/` (`AuthResource`, `GoogleAuthResource`) own login; `AuthResource.createAdminUser()` runs at startup.
- **Request logging filter:** every service adds a Jetty request-log filter that logs through SLF4J logger `org.eclipse.jetty.server.RequestLog` (level controlled by env var `TEXERA_SERVICE_LOG_LEVEL`). Note the servlet-API split: `amber` currently uses `javax.servlet.*` while `workflow-compiling-service` (and other newer services) use `jakarta.servlet.*`. There is a TODO to consolidate onto `common/auth`'s `RequestLoggingFilter.register()` once `amber` upgrades to Dropwizard 4.x.
- **Config loading:** every service uses `SubstitutingSourceProvider` + `EnvironmentVariableSubstitutor(false)` so YAML configs support `${ENV_VAR}` expansion.

## Where to make changes (project-specific conventions)
- **New backend endpoint:** create a Jersey `*Resource` under `<service>/src/main/scala/.../resource/` and **register it in that service's `Application.run(...)`** via `environment.jersey.register(classOf[YourResource])`. `amber`'s `TexeraWebApplication.run(...)` already registers a long list — `AuthResource`, `WorkflowResource`, `DashboardResource`, `ProjectResource`, `HubResource`, `GmailResource`, `AIAssistantResource`, etc. Follow that pattern; don't rely on classpath scanning.
- **Shared backend logic** belongs in `common/*` (honor the dependency graph in `build.sbt`): `common/dao` for DB, `common/config` for config, `common/auth` for auth, `common/workflow-core` + `common/workflow-operator` for dataflow model/operators, `common/pybuilder` for Python code generation.
- **New operators:** add to `common/workflow-operator/...`; Python-backed operators typically use `common/pybuilder` and interact with the Python worker under `amber/src/main/python/`.
- **Frontend code** is isolated under `frontend/src/` (Angular, yarn-managed). `amber` only serves the built output from `frontend/dist`. The app modules live in `frontend/src/app/{common,dashboard,hub,workspace}`.
- When adding Jackson-touched types, respect the per-service Jackson `dependencyOverrides` already in `build.sbt` — different services pin different versions for Dropwizard 3 vs 4 compatibility.

## Critical developer workflows
- **Build everything:** `bin/build.sh` runs `bin/build-services.sh` (which runs `sbt clean dist` and unzips the `target/universal/*.zip` artifacts into per-service `target/` dirs) and then `bin/frontend.sh` (`yarn install && yarn run build` in `frontend/`).
- **Run locally after build:**
  - `bin/server.sh` — starts `amber` from `amber/target/texera-*/bin/texera-web-application`.
  - `bin/workflow-compiling-service.sh`, `bin/file-service.sh`, `bin/config-service.sh`, `bin/computing-unit-managing-service.sh`, `bin/workflow-computing-unit.sh` — start the other services from their unzipped `target/` dirs.
  - `bin/frontend-dev.sh` — frontend dev server with the proxy config above.
  - `bin/shared-editing-server.sh` — y-websocket server for `/rtc` collaboration.
  - `bin/python-language-service.sh` / `bin/pylsp/` — Python language service.
- **Docker images:** Dockerfiles in `bin/*.dockerfile` **must be built from the repo root** as context (see `bin/README.md`). Example: `docker build -f bin/texera-web-application.dockerfile -t your-repo/texera-web-application:test .`. Helpers: `bin/build-images.sh`, `bin/merge-image-tags.sh`.
- **Deployment references:** single-node Docker Compose at `bin/single-node/docker-compose.yml` (+ `nginx.conf`, `examples/`); Kubernetes Helm chart at `bin/k8s/` (`Chart.yaml`, `values.yaml`, `values-development.yaml`, `templates/`).
- **Formatting/lint:** `bin/fix-format.sh`; Scalafmt config at `.scalafmt.conf`, Scalafix at `.scalafix.conf`.
- **Proto codegen:** `bin/python-proto-gen.sh`, `bin/frontend-proto-gen.sh`.
- **Service entrypoints** typically call `new <Service>().run("server", <path-to-yaml>)`. The YAML path either resolves via `TEXERA_HOME` (e.g., `WorkflowCompilingService`) or via `Utils.amberHomePath` (e.g., `TexeraWebApplication`).

## Map of the code (high-signal entrypoints)
- sbt module graph and ASF licensing task: `build.sbt`
- `amber` web app + REST registration + GUI serving + WebSocket + JWT: `amber/src/main/scala/org/apache/texera/web/TexeraWebApplication.scala`
- Amber dataflow engine: `amber/src/main/scala/org/apache/texera/amber/engine/architecture/{controller,worker,pythonworker,scheduling,messaginglayer,sendsemantics,deploysemantics,logreplay,common}/`
- Compilation service: `workflow-compiling-service/src/main/scala/org/apache/texera/service/WorkflowCompilingService.scala`
- Other service entrypoints: `*/src/main/scala/org/apache/texera/service/*Service.scala` (`FileService`, `ConfigService`, `AccessControlService`, `ComputingUnitManagingService`)
- Dashboard/user/hub/admin/AI resources: `amber/src/main/scala/org/apache/texera/web/resource/` (top-level + `auth/`, `aiassistant/`, `dashboard/{admin,hub,user}/`)
- Python worker + pytexera SDK: `amber/src/main/python/`
- Deployment artifacts: `bin/*.dockerfile`, `bin/*.sh`, `bin/single-node/`, `bin/k8s/`
- SQL DDL and catalog bootstrap: `sql/texera_ddl.sql`, `sql/texera_lakefs.sql`, `sql/texera_lakekeeper.sql`, `sql/iceberg_postgres_catalog.sql`, `sql/updates/`
- Root docs: `README.md` (links to developer wiki), `CONTRIBUTING.md`, `SECURITY.md`, `DISCLAIMER-WIP`.
