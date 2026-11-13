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

import datetime
import importlib.util
import json
import os
from pathlib import Path

import pytest

ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "common/workflow-operator").is_dir()
)
CONTRACT = json.loads(
    (
        ROOT
        / "common/workflow-operator/src/test/resources/udf/ui-parameter-contract.json"
    ).read_text()
)
TYPE_CONTRACT = json.loads(
    (
        ROOT / "common/workflow-operator/src/main/resources/udf/ui-parameter-types.json"
    ).read_text()
)


@pytest.mark.parametrize("kind,aliases", TYPE_CONTRACT["types"].items())
def test_python_sdk_aliases_match_the_shared_manifest(kind, aliases):
    from core.models.schema.attribute_type import AttributeType, RAW_TYPE_MAPPING

    expected = RAW_TYPE_MAPPING[kind.upper()]
    for alias in aliases:
        assert getattr(AttributeType, alias) is expected


def test_python_sdk_resources_match_the_shared_manifest():
    from pytexera.udf.udf_operator import Resource

    assert {resource.name: resource.value for resource in Resource} == TYPE_CONTRACT[
        "resources"
    ]


def normalize(value, kind):
    if kind in ("BOOL", "BOOLEAN"):
        return bool(value)
    if kind == "TIMESTAMP":
        return value.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)


@pytest.mark.parametrize("case", CONTRACT["values"], ids=lambda case: case["id"])
def test_python_ui_parameter_value_contract(case):
    from core.models.schema.attribute_type import AttributeType
    from pytexera.udf.udf_operator import _UiParameterSupport

    kind = getattr(AttributeType, case["type"])
    if case.get("error"):
        with pytest.raises(ValueError):
            _UiParameterSupport._parse(case["raw"], kind)
    else:
        assert (
            normalize(_UiParameterSupport._parse(case["raw"], kind), case["type"])
            == case["expected"]
        )


@pytest.fixture(scope="module")
def plugin_fixtures():
    requested = os.getenv("TEXERA_R_INTEGRATION") == "1"
    if importlib.util.find_spec("texera_r") is None:
        if requested:
            pytest.fail(
                "TEXERA_R_INTEGRATION=1 requires the actual texera-rudf plugin."
            )
        pytest.skip("Optional texera-rudf plugin is not installed.")
    path = Path(
        os.getenv(
            "TEXERA_UI_PARAMETER_FIXTURES",
            str(ROOT / "common/workflow-operator/target/ui-parameter-fixtures.json"),
        )
    )
    if not path.is_file():
        if requested:
            pytest.fail(
                "Generate compiler fixtures with Test/runMain UiParameterIntegrationFixtures first."
            )
        pytest.skip("Compiler integration fixtures have not been generated.")
    data = Path("/tmp/texera-r-ui-contract-data")
    model = Path("/tmp/texera-r-ui-contract-model")
    data.mkdir(exist_ok=True)
    model.mkdir(exist_ok=True)
    (data / "fixture.csv").write_text("id\n1\n2\n")
    (model / "model.txt").write_text("model fixture\n")
    return json.loads(path.read_text())


@pytest.mark.integration
@pytest.mark.parametrize("case_id", [case["id"] for case in CONTRACT["values"]])
def test_compiled_python_and_r_value_contract(plugin_fixtures, case_id):
    import rpy2.robjects as r
    from core.architecture.managers.executor_manager import ExecutorManager

    case = next(case for case in plugin_fixtures["values"] if case["id"] == case_id)
    if "compileError" in case:
        assert case.get("error"), case
        return
    manager = ExecutorManager()
    manager.initialize_executor(case["pythonCode"], False, "python")
    from texera_r.ui_parameters import load_user_function

    function = load_user_function(case["rCode"])
    if case.get("error"):
        with pytest.raises(Exception):
            manager.executor.open()
        with pytest.raises(Exception):
            function()
        return
    manager.executor.open()
    assert normalize(manager.executor.result, case["type"]) == case["expected"]
    actual = function()
    if case["type"] in ("BOOL", "BOOLEAN"):
        actual = bool(actual[0])
    elif case["type"] == "TIMESTAMP":
        actual = str(r.r["format"](actual, format="%Y-%m-%dT%H:%M:%SZ", tz="UTC")[0])
    else:
        actual = str(r.r["as.character"](actual)[0])
    if case["type"] == "DOUBLE":
        actual = actual.lower()
    assert actual == case["expected"]


@pytest.mark.integration
@pytest.mark.parametrize(
    "source,tuple_api", [(False, False), (False, True), (True, False), (True, True)]
)
def test_real_texera_r_executors_and_arrow(plugin_fixtures, source, tuple_api):
    import pandas as pd
    import pyarrow as pa
    from core.architecture.managers.executor_manager import ExecutorManager
    from core.models import Tuple
    from core.models.schema import Schema

    case = next(
        item
        for item in plugin_fixtures["operators"]
        if item["source"] == source and item["tuple"] == tuple_api
    )
    manager = ExecutorManager()
    manager.initialize_executor(
        case["code"], source, "r-tuple" if tuple_api else "r-table"
    )
    executor = manager.executor
    assert type(executor).__module__.startswith("texera_r.")
    if source:
        rows = list(executor.produce())
    elif tuple_api:
        schema = Schema(arrow_schema=pa.schema([pa.field("id", pa.int32())]))
        rows = list(executor.process_tuple(Tuple({"id": 3}, schema=schema), 0))
    else:
        rows = list(executor.process_table(pd.DataFrame({"id": [3, 4]}), 0))
    rows = [row.as_dict() for row in rows if row is not None]
    assert len(rows) == (2 if not source and not tuple_api else 1)
    for row in rows:
        assert row["count"] == 7
        assert row["label"] == "café\n\"; stop('injected') #"
        assert row["enabled"] is True
        assert row["score"] == 2.5
        assert row["long_value"] == 9223372036854775807
        assert normalize(row["created"], "TIMESTAMP") == "2026-11-03T12:30:00Z"
        assert row["resource_rows"] == 2
        assert row["model_text"] == "model fixture"
