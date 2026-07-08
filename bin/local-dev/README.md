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

## Adding a new managed service

1. Drop the launch command into `main.sh`'s `start_one` switch.
2. Add the row to the service catalog (port, type, sbt project, sibling group).
3. If it has its own source tree, add an entry so the dirty-source detector
   can hash it.
4. Add a row to `tui.py`'s service catalog for the dashboard.

The wrapper `bin/local-dev.sh` does not need to be touched.
