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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import base64
import builtins
import importlib.util
import io
import json
import os
import sys
import types
from pathlib import Path
from unittest import mock

import pytest
from loguru import logger

import texera_run_python_worker as entry


def _encode(config) -> str:
    """Encode a config the way PythonWorkflowWorker does: Base64-encoded JSON.

    The JVM side passes the startup config as Base64 so it survives command-line
    argv quoting on every platform (a raw JSON string loses its quotes on Windows).
    """
    return base64.b64encode(json.dumps(config).encode("utf-8")).decode("ascii")


def _full_config() -> dict:
    """A complete startup config matching the keys PythonWorkflowWorker emits."""
    return {
        "workerId": "worker-1",
        "outputPort": "5005",
        "loggerLevel": "INFO",
        "rPath": "",
        "icebergCatalogType": "postgres",
        "icebergPostgresCatalogUriWithoutScheme": "host:5432/db",
        "icebergPostgresCatalogUsername": "pg-user",
        "icebergPostgresCatalogPassword": "pg-pass",
        "icebergRestCatalogUri": "",
        "icebergRestCatalogWarehouseName": "",
        "icebergTableNamespace": "result_ns",
        "icebergTableStateNamespace": "state_ns",
        "icebergFileStorageDirectoryPath": "/tmp/files",
        "icebergTableCommitBatchSize": "100",
        "s3Endpoint": "http://s3:9000",
        "s3Region": "us-west-2",
        "s3AuthUsername": "s3-user",
        "s3AuthPassword": "s3-pass",
        "s3LargeBinariesBaseUri": "s3://bucket/base",
    }


def _patched_collaborators():
    """Patch the heavy collaborators so main() exercises only the config wiring."""
    return (
        mock.patch.object(entry, "StorageConfig"),
        mock.patch.object(entry, "PythonWorker"),
        mock.patch.object(entry, "init_loguru_logger"),
    )


def test_full_config_keys_match_the_expected_set():
    # Guards against the sample config in this test drifting from the contract.
    assert set(_full_config()) == set(entry.EXPECTED_CONFIG_KEYS)


def test_main_maps_named_config_to_storage_and_worker():
    """Each named field reaches the correct StorageConfig.initialize argument and
    worker parameter — guarding against the silent misalignment that positional
    argv passing allowed."""
    config = _full_config()
    storage_patch, worker_patch, _logger_patch = _patched_collaborators()
    with storage_patch as storage_config, worker_patch as python_worker, _logger_patch:
        entry.main(_encode(config))

    storage_config.initialize.assert_called_once_with(
        "postgres",
        "host:5432/db",
        "pg-user",
        "pg-pass",
        "",
        "",
        "result_ns",
        "state_ns",
        "/tmp/files",
        "100",
        "http://s3:9000",
        "us-west-2",
        "s3-user",
        "s3-pass",
        "s3://bucket/base",
    )
    python_worker.assert_called_once_with(
        worker_id="worker-1", host="localhost", output_port=5005
    )
    python_worker.return_value.run.assert_called_once()


def test_main_mapping_is_independent_of_key_order():
    """Reordering the JSON keys must not change where values land (it is a dict)."""
    reordered = dict(reversed(list(_full_config().items())))
    storage_patch, worker_patch, _logger_patch = _patched_collaborators()
    with storage_patch as storage_config, worker_patch as python_worker, _logger_patch:
        entry.main(_encode(reordered))

    storage_config.initialize.assert_called_once_with(
        "postgres",
        "host:5432/db",
        "pg-user",
        "pg-pass",
        "",
        "",
        "result_ns",
        "state_ns",
        "/tmp/files",
        "100",
        "http://s3:9000",
        "us-west-2",
        "s3-user",
        "s3-pass",
        "s3://bucket/base",
    )
    python_worker.assert_called_once_with(
        worker_id="worker-1", host="localhost", output_port=5005
    )


def test_main_sets_r_home_when_r_path_present(monkeypatch):
    monkeypatch.delenv("R_HOME", raising=False)
    config = _full_config()
    config["rPath"] = "/opt/R"
    storage_patch, worker_patch, _logger_patch = _patched_collaborators()
    with storage_patch, worker_patch, _logger_patch:
        import os

        entry.main(_encode(config))
        assert os.environ["R_HOME"] == "/opt/R"


@pytest.mark.parametrize("missing_key", sorted(_full_config().keys()))
def test_parse_rejects_a_missing_key(missing_key):
    """A missing key fails loudly rather than being silently misassigned."""
    config = _full_config()
    del config[missing_key]
    with pytest.raises(ValueError, match="key mismatch"):
        entry.parse_startup_config(_encode(config))


def test_parse_rejects_an_unexpected_key():
    """An extra key (e.g. the JVM side added a field) fails instead of being ignored,
    and is reported on the `unexpected` side — that message is the whole diagnostic
    an operator gets when the two sides drift, so naming the wrong side sends the
    reader to the wrong end of the contract."""
    config = _full_config()
    config["someNewField"] = "value"
    with pytest.raises(
        ValueError, match=r"missing=\[\], unexpected=\['someNewField'\]"
    ):
        entry.parse_startup_config(_encode(config))


def test_parse_rejects_a_non_string_value():
    """A wrongly-typed value (e.g. a number instead of a string) fails."""
    config = _full_config()
    config["outputPort"] = 5005  # number instead of the expected string
    with pytest.raises(TypeError, match="must be strings"):
        entry.parse_startup_config(_encode(config))


def test_parse_rejects_a_non_object_payload():
    with pytest.raises(TypeError, match="must be a JSON object"):
        entry.parse_startup_config(_encode(["not", "an", "object"]))


def test_parse_round_trips_a_base64_encoded_config():
    """The config is passed as Base64-encoded JSON; parsing decodes it back."""
    config = _full_config()
    assert entry.parse_startup_config(_encode(config)) == config


ENTRY_MODULE_PATH = Path(entry.__file__)


def _exec_entry_module(module_name: str):
    """Execute the entry script afresh from its file under the given module name.

    `exec_module` deliberately does not register the result in `sys.modules`, so a
    run that aborts half-way through its imports — or one executed as `__main__` —
    cannot disturb the already-imported `texera_run_python_worker` that the tests
    above use. Loading by file path keeps the executed code objects anchored to the
    real source file, so coverage still attributes the lines to it.
    """
    spec = importlib.util.spec_from_file_location(module_name, ENTRY_MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _exec_entry_module_with_failing_collaborator_imports(error):
    """Re-run the entry script with its `core.*` imports raising `error`.

    Only the `core` package is failed: the stdlib and loguru imports above the
    try/except must still succeed, otherwise the failure would never reach the
    handler under test.
    """
    real_import = builtins.__import__

    def failing_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.split(".")[0] == "core":
            raise error
        return real_import(name, globals, locals, fromlist, level)

    with mock.patch.object(builtins, "__import__", failing_import):
        return _exec_entry_module("texera_run_python_worker_reexec")


@pytest.fixture
def restored_loguru_sink():
    """Undo `init_loguru_logger`'s process-global handler surgery.

    `logger.remove()` stops every loguru handler for the remainder of the pytest
    process, so a test that lets it run must hand a working stderr sink back to the
    suites that follow. The stopped handler objects cannot be re-registered, so a
    fresh sink is added instead. `LOGURU_LEVEL` is safe to reuse verbatim: loguru
    validates it while configuring its own default handler at import time, so an
    unusable value would already have failed long before this fixture runs.
    """
    yield
    logger.remove()
    logger.add(
        sys.__stderr__ or sys.stderr, level=os.environ.get("LOGURU_LEVEL", "DEBUG")
    )


def test_missing_proto_package_exits_with_generation_guidance():
    """The generated proto bindings are not checked in, so their absence is the
    single most likely first-run failure; it must exit with the regeneration
    recipe rather than a bare ImportError traceback."""
    with pytest.raises(SystemExit) as excinfo:
        _exec_entry_module_with_failing_collaborator_imports(
            ModuleNotFoundError("No module named 'proto'", name="proto")
        )

    message = str(excinfo.value)
    assert "amber/src/main/python/proto/" in message
    assert "bin/python-proto-gen.sh" in message


def test_missing_proto_submodule_exits_with_generation_guidance():
    """A partially generated tree reports a submodule rather than `proto` itself,
    which has to be recognised as the same failure."""
    with pytest.raises(SystemExit) as excinfo:
        _exec_entry_module_with_failing_collaborator_imports(
            ModuleNotFoundError(
                "No module named 'proto.edu'", name="proto.edu.uci.ics.amber"
            )
        )

    assert "bin/python-proto-gen.sh" in str(excinfo.value)


@pytest.mark.parametrize(
    "missing_name",
    [
        "numpy",
        # A dependency whose name merely *begins with* "proto" — protobuf sits right
        # next to the generated `proto/` package in this project — must not match.
        "protobuf",
        # `core` is the very package the try block imports: a broken PYTHONPATH or a
        # worker spawned from the wrong cwd makes it unimportable, and blaming the
        # proto bindings for that sends the operator off to regenerate bindings that
        # are already present.
        "core",
        "core.models",
    ],
)
def test_unrelated_missing_dependency_propagates(missing_name):
    """A genuinely missing third-party dependency must not be misreported as
    missing proto bindings: the original error has to reach the caller. The guard
    has to be specific as well as complete — matching a name that merely starts
    with or contains "proto" is as wrong as failing to match "proto." itself."""
    with pytest.raises(ModuleNotFoundError) as excinfo:
        _exec_entry_module_with_failing_collaborator_imports(
            ModuleNotFoundError(f"No module named {missing_name!r}", name=missing_name)
        )

    assert excinfo.value.name == missing_name


def test_missing_dependency_without_a_name_propagates():
    """`ModuleNotFoundError.name` is optional, so the proto check must tolerate a
    `None` name instead of failing with an AttributeError of its own."""
    with pytest.raises(ModuleNotFoundError) as excinfo:
        _exec_entry_module_with_failing_collaborator_imports(
            ModuleNotFoundError("the import machinery reported no module name")
        )

    assert excinfo.value.name is None


@pytest.mark.parametrize(
    ("requested_level", "info_expected"),
    # Two rows, and a probe *between* DEBUG(10) and WARNING(30), are what make the
    # level parameter observable. A single row bounded only by debug and warning is
    # satisfied by every hardcoded constant in {INFO, SUCCESS, WARNING}, because
    # loguru puts INFO(20) and SUCCESS(25) inside that window.
    [("WARNING", False), ("INFO", True)],
)
def test_init_loguru_logger_replaces_handlers_at_the_requested_level(
    monkeypatch, restored_loguru_sink, requested_level, info_expected
):
    """Configuring the logger means *replacing* loguru's handlers: any sink that
    was already registered (loguru installs one by default) must be gone, and the
    new stderr sink must honour the level it was *asked* for rather than a level of
    its own choosing."""
    pre_existing_records = []
    logger.add(pre_existing_records.append, level="DEBUG")
    stderr_sink = io.StringIO()
    monkeypatch.setattr(sys, "stderr", stderr_sink)

    entry.init_loguru_logger(requested_level)

    logger.debug("below-every-requested-level")
    logger.info("between-the-requested-levels")
    logger.warning("at-or-above-every-requested-level")

    emitted = stderr_sink.getvalue()
    assert "at-or-above-every-requested-level" in emitted
    assert "below-every-requested-level" not in emitted
    assert ("between-the-requested-levels" in emitted) is info_expected
    assert pre_existing_records == []


def test_script_entry_point_starts_the_worker_from_argv(restored_loguru_sink):
    """Run as a script — the way PythonWorkflowWorker on the JVM side spawns it —
    the module must start a worker from the config in argv[1]."""
    python_worker = mock.MagicMock()
    worker_module = types.ModuleType("core.python_worker")
    worker_module.PythonWorker = python_worker
    storage_module = types.ModuleType("core.storage.storage_config")
    storage_module.StorageConfig = mock.MagicMock()
    # The third element exists only so that index 1 and index -1 differ, making the
    # argv *index* observable. The JVM side passes exactly one argument, so nothing
    # is asserted about trailing arguments beyond their not being what gets parsed.
    argv = [
        "src/main/python/texera_run_python_worker.py",
        _encode(_full_config()),
        "trailing-argument-the-entry-point-must-ignore",
    ]

    with (
        mock.patch.dict(
            sys.modules,
            {
                "core.python_worker": worker_module,
                "core.storage.storage_config": storage_module,
            },
        ),
        mock.patch.object(sys, "argv", argv),
    ):
        _exec_entry_module("__main__")

    python_worker.assert_called_once_with(
        worker_id="worker-1", host="localhost", output_port=5005
    )
    python_worker.return_value.run.assert_called_once()


def test_parse_names_the_missing_key_on_the_missing_side():
    """The companion of test_parse_rejects_an_unexpected_key: a key the JVM stopped
    sending has to be reported as *missing*. The 19-way parametrized test above can
    only afford a loose match, so the side of the diagnostic is fenced here."""
    config = _full_config()
    del config["s3Region"]
    with pytest.raises(ValueError, match=r"missing=\['s3Region'\], unexpected=\[\]"):
        entry.parse_startup_config(_encode(config))


def _encode_raw_utf8(config) -> str:
    """Encode a config the way Jackson actually does: with raw, unescaped UTF-8.

    `_encode` cannot model the real producer here. `json.dumps` defaults to
    `ensure_ascii=True`, which escapes every non-ASCII character, so any payload it
    builds is pure ASCII. PythonWorkflowWorker on the JVM side does
    `objectMapper.writeValueAsString(...)` then
    `Base64.getEncoder.encodeToString(json.getBytes(StandardCharsets.UTF_8))`, and
    Jackson emits non-ASCII characters literally. `ensure_ascii=False` is therefore
    the entire point of this helper - do not "simplify" it back to `_encode`.
    """
    return base64.b64encode(
        json.dumps(config, ensure_ascii=False).encode("utf-8")
    ).decode("ascii")


def test_parse_decodes_non_ascii_values_as_utf8():
    """Config values arrive as raw UTF-8 bytes, so they must be decoded as UTF-8.

    A non-ASCII value is entirely reachable in production - a Windows profile
    directory under a non-ASCII user name is the obvious case - and decoding the
    payload as ASCII would kill every worker on such a host at startup, before a
    single log line.
    """
    non_ascii_path = "/tmp/\u0444\u0430\u0439\u043b\u044b-\u6570\u636e"
    config = _full_config()
    config["icebergFileStorageDirectoryPath"] = non_ascii_path

    parsed = entry.parse_startup_config(_encode_raw_utf8(config))

    assert parsed["icebergFileStorageDirectoryPath"] == non_ascii_path


def test_main_forwards_the_configured_logger_level():
    """`loggerLevel` carries the JVM's UdfConfig.pythonLogStreamHandlerLevel, and it
    is the one named field no other test observes reaching its destination: the three
    main() tests replace init_loguru_logger with a mock and never look at the call.
    The level used here differs from the sample config's on purpose, so no hardcoded
    constant at the call site can satisfy the assertion."""
    assert _full_config()["loggerLevel"] != "WARNING"
    config = _full_config()
    config["loggerLevel"] = "WARNING"
    storage_patch, worker_patch, _unused_logger_patch = _patched_collaborators()
    with (
        storage_patch,
        worker_patch,
        mock.patch.object(entry, "init_loguru_logger") as init_logger,
    ):
        entry.main(_encode(config))

    init_logger.assert_called_once_with("WARNING")


def test_main_keeps_the_rest_catalog_uri_and_warehouse_name_in_order():
    """Fence the one adjacent pair of StorageConfig.initialize arguments the sample
    config cannot tell apart: `icebergRestCatalogUri` and
    `icebergRestCatalogWarehouseName` are both "" there, so a swap of the two would
    satisfy assert_called_once_with in either order. On a REST catalog both fields
    are non-empty, and swapping them points the catalog URI at a warehouse name.

    The values are overridden locally rather than in `_full_config()` so the shared
    fixture, and the two full-mapping assertions built on it, stay untouched.
    """
    config = _full_config()
    config["icebergRestCatalogUri"] = "http://rest-catalog:8181"
    config["icebergRestCatalogWarehouseName"] = "warehouse-name"
    storage_patch, worker_patch, logger_patch = _patched_collaborators()
    with storage_patch as storage_config, worker_patch, logger_patch:
        entry.main(_encode(config))

    positional = storage_config.initialize.call_args.args
    assert positional[4] == "http://rest-catalog:8181"
    assert positional[5] == "warehouse-name"


def test_main_leaves_r_home_unset_when_r_path_is_blank(monkeypatch):
    """The false arm of the R_HOME guard: a non-R worker must not be handed an empty
    R_HOME. An empty value is worse than an absent one for rpy2's R discovery, which
    is exactly what the guard exists to prevent. Every other main() test runs the
    false arm too, but none of them looks at the environment."""
    # Load-bearing, not decorative: test_main_sets_r_home_when_r_path_present lets
    # production write R_HOME directly, and monkeypatch.delenv records nothing to
    # restore when the variable was absent to begin with - so R_HOME leaks out of
    # that test and this one would otherwise depend on collection order.
    monkeypatch.delenv("R_HOME", raising=False)
    config = _full_config()
    assert config["rPath"] == ""
    storage_patch, worker_patch, logger_patch = _patched_collaborators()
    with storage_patch, worker_patch, logger_patch:
        entry.main(_encode(config))

    assert "R_HOME" not in os.environ
