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

"""
End-to-end check of a Texera deployment, driven the way the browser drives it:
every call goes through the Envoy gateway.

  login -> (warehouse) -> dataset + upload + version -> workflow create/read
        -> computing unit -> websocket execute -> paginated result -> verify

Configured by environment variables:
  TEXERA_URL               gateway base URL (default http://localhost:8080)
  TEXERA_USERNAME          (default texera)
  TEXERA_PASSWORD          (default texera)
  TEXERA_STEP_TIMEOUT_S    upper bound for each waiting step (default 900)
"""

import base64
import json
import os
import sys
import time
import uuid
from urllib.parse import quote, urlencode

import requests
import websocket

BASE_URL = os.environ.get("TEXERA_URL", "http://localhost:8080").rstrip("/")
USERNAME = os.environ.get("TEXERA_USERNAME", "texera")
PASSWORD = os.environ.get("TEXERA_PASSWORD", "texera")
STEP_TIMEOUT_S = int(os.environ.get("TEXERA_STEP_TIMEOUT_S", "900"))

RUN_ID = uuid.uuid4().hex[:8]
DATASET_NAME = f"ci-people-{RUN_ID}"
CSV_NAME = "people.csv"
CSV_CONTENT = (
    "name,age,city\n"
    "alice,34,Irvine\n"
    "bob,27,Seattle\n"
    "carol,45,Austin\n"
    "dave,31,Boston\n"
    "erin,19,Denver\n"
)
# Filter keeps age > 30; the Python UDF appends name_upper.
EXPECTED_ROWS = [
    {"name": "alice", "age": 34, "city": "Irvine", "name_upper": "ALICE"},
    {"name": "carol", "age": 45, "city": "Austin", "name_upper": "CAROL"},
    {"name": "dave", "age": 31, "city": "Boston", "name_upper": "DAVE"},
]

UDF_CODE = """from pytexera import *

class ProcessTupleOperator(UDFOperatorV2):
    @overrides
    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        tuple_["name_upper"] = tuple_["name"].upper()
        yield tuple_
"""

SCAN_ID = f"CSVFileScan-operator-{uuid.uuid4()}"
FILTER_ID = f"Filter-operator-{uuid.uuid4()}"
UDF_ID = f"PythonUDFV2-operator-{uuid.uuid4()}"


class IntegrationTestFailure(Exception):
    pass


def step(title):
    print(f"\n=== {title}", flush=True)


def info(message):
    print(f"    {message}", flush=True)


def check(condition, message):
    if not condition:
        raise IntegrationTestFailure(message)


def jwt_claims(token):
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def find_key(obj, key):
    """Depth-first search for `key`, so the test does not pin response nesting."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for value in obj.values():
            found = find_key(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_key(value, key)
            if found is not None:
                return found
    return None


class Client:
    def __init__(self):
        self.session = requests.Session()
        self.token = None

    def call(self, method, path, expected=(200,), **kwargs):
        headers = kwargs.pop("headers", {})
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        response = self.session.request(
            method, f"{BASE_URL}{path}", headers=headers, timeout=60, **kwargs
        )
        if response.status_code not in expected:
            raise IntegrationTestFailure(
                f"{method} {path} -> HTTP {response.status_code}: {response.text[:2000]}"
            )
        return response

    def json(self, method, path, **kwargs):
        return self.call(method, path, **kwargs).json()


def wait_until(description, probe, interval_s=5):
    deadline = time.monotonic() + STEP_TIMEOUT_S
    last = None
    while time.monotonic() < deadline:
        try:
            result = probe()
            if result:
                return result
            last = "not ready"
        except (requests.RequestException, IntegrationTestFailure) as e:
            last = e
        time.sleep(interval_s)
    raise IntegrationTestFailure(f"timed out waiting for {description}: {last}")


def login(client):
    step("Log in")

    def attempt():
        response = client.session.post(
            f"{BASE_URL}/api/auth/login",
            json={"username": USERNAME, "password": PASSWORD},
            timeout=30,
        )
        if response.status_code != 200:
            raise IntegrationTestFailure(
                f"HTTP {response.status_code}: {response.text[:500]}"
            )
        return response.json()["accessToken"]

    client.token = wait_until("the login endpoint behind the gateway", attempt)
    claims = jwt_claims(client.token)
    info(f"logged in as uid={claims.get('userId')} email={claims.get('email')}")
    return claims


def prepare_warehouse(client):
    step("Resolve the execution warehouse")
    response = client.call("GET", "/api/warehouse/status", expected=(200, 404))
    if response.status_code == 404 or not response.json().get("enabled"):
        info("per-user warehouses are off; executions use the shared default")
        return None
    created = client.json("POST", "/api/warehouse", json={"name": f"ci-{RUN_ID}"})
    info(
        f"created warehouse whid={created['whid']} ({created['lakekeeperWarehouseName']})"
    )
    return created["whid"]


def upload_dataset(client, owner_email):
    step(f"Create dataset '{DATASET_NAME}' and upload {CSV_NAME}")
    created = client.json(
        "POST",
        "/api/dataset/create",
        json={
            "datasetName": DATASET_NAME,
            "datasetDescription": "k3d integration test",
            "isDatasetPublic": False,
        },
    )
    did = find_key(created, "did")
    check(did is not None, f"dataset create returned no did: {created}")
    info(f"dataset did={did}")

    payload = CSV_CONTENT.encode()
    common = {
        "ownerEmail": owner_email,
        "datasetName": DATASET_NAME,
        "filePath": CSV_NAME,
    }
    init = client.json(
        "POST",
        "/api/dataset/multipart-upload?"
        + urlencode(
            {
                **common,
                "type": "init",
                "fileSizeBytes": len(payload),
                "partSizeBytes": len(payload),
            }
        ),
    )
    check("missingParts" in init, f"multipart init returned no missingParts: {init}")
    client.call(
        "POST",
        "/api/dataset/multipart-upload/part?" + urlencode({**common, "partNumber": 1}),
        data=payload,
        headers={"Content-Type": "application/octet-stream"},
    )
    client.call(
        "POST",
        "/api/dataset/multipart-upload?" + urlencode({**common, "type": "finish"}),
    )

    version = client.json(
        "POST",
        f"/api/dataset/{did}/version/create",
        data="",
        headers={"Content-Type": "text/plain"},
    )
    version_name = find_key(version, "name")
    check(version_name, f"version create returned no version name: {version}")
    info(f"created version '{version_name}'")

    listing = client.json("GET", "/api/dataset/list")
    check(
        any(find_key(entry, "name") == DATASET_NAME for entry in listing),
        f"dataset '{DATASET_NAME}' missing from /api/dataset/list",
    )
    return did, version_name


def port(port_id, display_name="", dependencies=None):
    descriptor = {
        "portID": port_id,
        "displayName": display_name,
        "allowMultiInputs": False,
        "isDynamicPort": False,
    }
    if dependencies is not None:
        descriptor["dependencies"] = dependencies
    return descriptor


def operator(operator_id, operator_type, properties, inputs, view_result=False):
    return {
        "operatorID": operator_id,
        "operatorType": operator_type,
        "operatorVersion": "N/A",
        "operatorProperties": properties,
        "inputPorts": [port(p, dependencies=[]) for p in inputs],
        "outputPorts": [port("output-0")],
        "showAdvanced": False,
        "isDisabled": False,
        "customDisplayName": operator_type,
        "dynamicInputPorts": False,
        "dynamicOutputPorts": False,
        "viewResult": view_result,
    }


def link(source_id, target_id):
    return {
        "linkID": f"link-{uuid.uuid4()}",
        "source": {"operatorID": source_id, "portID": "output-0"},
        "target": {"operatorID": target_id, "portID": "input-0"},
    }


def workflow_content(file_path):
    operators = [
        operator(
            SCAN_ID,
            "CSVFileScan",
            {
                "fileEncoding": "UTF_8",
                "customDelimiter": ",",
                "hasHeader": True,
                "fileName": file_path,
            },
            inputs=[],
        ),
        operator(
            FILTER_ID,
            "Filter",
            {"predicates": [{"attribute": "age", "condition": ">", "value": "30"}]},
            inputs=["input-0"],
        ),
        operator(
            UDF_ID,
            "PythonUDFV2",
            {
                "code": UDF_CODE,
                "workers": 1,
                "retainInputColumns": True,
                "defaultEnv": True,
                "outputColumns": [
                    {"attributeName": "name_upper", "attributeType": "string"}
                ],
            },
            inputs=["input-0"],
            view_result=True,
        ),
    ]
    return {
        "operators": operators,
        "operatorPositions": {
            op["operatorID"]: {"x": 200 * i, "y": 100} for i, op in enumerate(operators)
        },
        "links": [link(SCAN_ID, FILTER_ID), link(FILTER_ID, UDF_ID)],
        "commentBoxes": [],
        "settings": {"dataTransferBatchSize": 400},
    }


def create_workflow(client, file_path):
    step("Create and read back the workflow")
    name = f"ci-workflow-{RUN_ID}"
    created = client.json(
        "POST",
        "/api/workflow/create",
        json={"name": name, "content": json.dumps(workflow_content(file_path))},
    )
    wid = find_key(created, "wid")
    check(wid is not None, f"workflow create returned no wid: {created}")
    info(f"workflow wid={wid}")

    stored = client.json("GET", f"/api/workflow/{wid}")
    content = stored["content"]
    content = json.loads(content) if isinstance(content, str) else content
    stored_ids = sorted(op["operatorID"] for op in content["operators"])
    check(
        stored_ids == sorted([SCAN_ID, FILTER_ID, UDF_ID]),
        f"stored workflow operators differ: {stored_ids}",
    )
    return wid, content


def logical_plan(content):
    """Mirrors ExecuteWorkflowService.getLogicalPlanRequest in the frontend."""
    operators = {op["operatorID"]: op for op in content["operators"]}

    def ordinal(ports, port_id):
        return next(i for i, p in enumerate(ports) if p["portID"] == port_id)

    return {
        "operators": [
            {
                **op["operatorProperties"],
                "operatorID": op["operatorID"],
                "operatorType": op["operatorType"],
                "inputPorts": op["inputPorts"],
                "outputPorts": op["outputPorts"],
            }
            for op in content["operators"]
        ],
        "links": [
            {
                "fromOpId": l["source"]["operatorID"],
                "fromPortId": {
                    "id": ordinal(
                        operators[l["source"]["operatorID"]]["outputPorts"],
                        l["source"]["portID"],
                    ),
                    "internal": False,
                },
                "toOpId": l["target"]["operatorID"],
                "toPortId": {
                    "id": ordinal(
                        operators[l["target"]["operatorID"]]["inputPorts"],
                        l["target"]["portID"],
                    ),
                    "internal": False,
                },
            }
            for l in content["links"]
        ],
        "opsToViewResult": [
            op["operatorID"] for op in content["operators"] if op.get("viewResult")
        ],
        "opsToReuseResult": [],
    }


def create_computing_unit(client):
    step("Create a Kubernetes computing unit")
    limits = client.json("GET", "/api/computing-unit/limits")
    info(f"limit options: {limits}")
    memory = limits["memoryLimitOptions"][0]
    created = client.json(
        "POST",
        "/api/computing-unit/create",
        json={
            "name": f"ci-cu-{RUN_ID}",
            "unitType": "kubernetes",
            "cpuLimit": limits["cpuLimitOptions"][0],
            "memoryLimit": memory,
            "gpuLimit": limits["gpuLimitOptions"][0],
            "jvmMemorySize": "1G",
            "shmSize": "64Mi",
        },
    )
    cuid = created["computingUnit"]["cuid"]
    info(f"computing unit cuid={cuid}, status={created['status']}")

    def running():
        unit = client.json("GET", f"/api/computing-unit/{cuid}")
        info(f"cuid={cuid} status={unit['status']} reason={unit.get('statusReason')}")
        return unit["status"] == "Running"

    wait_until(f"computing unit {cuid} to run", running, interval_s=10)
    return cuid


def websocket_url(wid, uid, cuid, token):
    ws_base = BASE_URL.replace("https://", "wss://").replace("http://", "ws://")
    query = urlencode(
        {"wid": wid, "uid": uid, "cuid": cuid, "access-token": token}, quote_via=quote
    )
    return f"{ws_base}/wsapi/workflow-websocket?{query}"


def open_websocket(url):
    # The gateway answers 5xx while the unit's websocket endpoint is still coming up.
    def attempt():
        try:
            return websocket.create_connection(url, timeout=30)
        except (websocket.WebSocketException, OSError) as e:
            raise IntegrationTestFailure(f"websocket connect failed: {e}") from e

    return wait_until("the computing unit's websocket", attempt)


def receive(ws, deadline):
    while time.monotonic() < deadline:
        try:
            frame = ws.recv()
        except websocket.WebSocketTimeoutException:
            ws.send(json.dumps({"type": "HeartBeatRequest"}))
            continue
        if not frame:
            continue
        return json.loads(frame)
    raise IntegrationTestFailure("timed out waiting for a websocket event")


def execute_workflow(ws, content, cuid, warehouse_id):
    step("Execute the workflow over the websocket")
    request = {
        "type": "WorkflowExecuteRequest",
        "executionName": f"ci-execution-{RUN_ID}",
        "engineVersion": "ci",
        "logicalPlan": logical_plan(content),
        "replayFromExecution": None,
        "workflowSettings": content["settings"],
        "emailNotificationEnabled": False,
        "computingUnitId": cuid,
    }
    if warehouse_id is not None:
        request["warehouseId"] = warehouse_id
    ws.send(json.dumps(request))

    deadline = time.monotonic() + STEP_TIMEOUT_S
    seen_running = False
    while True:
        event = receive(ws, deadline)
        kind = event.get("type")
        if kind == "WorkflowStateEvent":
            state = event["state"]
            info(f"state -> {state}")
            seen_running |= state == "Running"
            if state == "Completed":
                check(
                    seen_running, "execution completed without ever reporting Running"
                )
                return
            if state in ("Failed", "Killed", "Terminated"):
                raise IntegrationTestFailure(f"execution ended in state {state}")
        elif kind == "WorkflowErrorEvent":
            raise IntegrationTestFailure(
                f"execution error: {json.dumps(event['fatalErrors'])[:4000]}"
            )
        elif kind == "ConsoleUpdateEvent":
            for message in event.get("messages", []):
                info(
                    f"console[{event.get('operatorId')}]: {message.get('title', '')}".rstrip()
                )


def fetch_result(ws, operator_id):
    step("Read the result page")
    request_id = str(uuid.uuid4())
    ws.send(
        json.dumps(
            {
                "type": "ResultPaginationRequest",
                "requestID": request_id,
                "operatorID": operator_id,
                "pageIndex": 1,
                "pageSize": 100,
            }
        )
    )
    deadline = time.monotonic() + 120
    while True:
        event = receive(ws, deadline)
        if (
            event.get("type") == "PaginatedResultEvent"
            and event.get("requestID") == request_id
        ):
            return event["table"]
        if event.get("type") == "WorkflowErrorEvent":
            raise IntegrationTestFailure(
                f"pagination error: {json.dumps(event)[:4000]}"
            )


def verify_rows(rows):
    info(f"received {len(rows)} row(s): {json.dumps(rows)}")
    actual = sorted(
        ({key: row.get(key) for key in EXPECTED_ROWS[0]} for row in rows),
        key=lambda row: row["name"],
    )
    normalized = [{key: str(value) for key, value in row.items()} for row in actual]
    expected = [
        {key: str(value) for key, value in row.items()} for row in EXPECTED_ROWS
    ]
    check(
        normalized == expected,
        f"result mismatch:\n  expected {expected}\n  actual   {normalized}",
    )
    info("result matches")


def terminate_computing_unit(client, cuid):
    step(f"Terminate computing unit {cuid}")
    client.call("DELETE", f"/api/computing-unit/{cuid}/terminate", expected=(200, 204))


def main():
    started = time.monotonic()
    client = Client()
    claims = login(client)
    warehouse_id = prepare_warehouse(client)
    _, version_name = upload_dataset(client, claims["email"])
    file_path = f"/dataset/{claims['email']}/{DATASET_NAME}/{version_name}/{CSV_NAME}"
    wid, content = create_workflow(client, file_path)
    cuid = create_computing_unit(client)
    ws = open_websocket(websocket_url(wid, claims["userId"], cuid, client.token))
    try:
        execute_workflow(ws, content, cuid, warehouse_id)
        verify_rows(fetch_result(ws, UDF_ID))
    finally:
        ws.close()
    terminate_computing_unit(client, cuid)
    print(f"\nPASS in {time.monotonic() - started:.0f}s", flush=True)


if __name__ == "__main__":
    try:
        main()
    except IntegrationTestFailure as failure:
        print(f"\nFAIL: {failure}", file=sys.stderr, flush=True)
        sys.exit(1)
