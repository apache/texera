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
Hamilton execution sidecar for Texera Agent Service.

Stateless service that:
1. Receives a Texera WorkflowContent JSON (operators, links, positions, etc.)
2. Translates it into a Hamilton DAG
3. Executes the DAG
4. Returns results in SyncExecutionResult format

The WorkflowContent format matches the frontend/agent-service representation:
  - operators[].operatorProperties.code  contains load()/process() Python code
  - links[].source.operatorID / links[].target.operatorID  define the DAG edges
"""

import inspect
import io
import sys
import traceback
import types
from typing import Any

import pandas as pd
from fastapi import FastAPI
from hamilton import driver
from pydantic import BaseModel

app = FastAPI(title="Hamilton Execution Sidecar")


# ---------------------------------------------------------------------------
# Request models — mirrors Texera WorkflowContent types
# ---------------------------------------------------------------------------


class PortRef(BaseModel):
    """Reference to an operator's port (source or target side of a link)."""
    operatorID: str
    portID: str


class PortDescription(BaseModel):
    portID: str
    displayName: str | None = None
    allowMultiInputs: bool | None = False
    isDynamicPort: bool | None = False

    class Config:
        extra = "allow"


class OperatorPredicate(BaseModel):
    """Operator as stored in WorkflowContent."""
    operatorID: str
    operatorType: str
    operatorVersion: str | None = None
    operatorProperties: dict[str, Any] = {}
    inputPorts: list[PortDescription] = []
    outputPorts: list[PortDescription] = []
    isDisabled: bool | None = False
    customDisplayName: str | None = None

    class Config:
        extra = "allow"


class OperatorLink(BaseModel):
    """Link between operators in WorkflowContent format."""
    linkID: str
    source: PortRef
    target: PortRef


class ExecuteRequest(BaseModel):
    """
    Accepts the full WorkflowContent shape plus execution parameters.
    operatorPositions, commentBoxes, settings are accepted but ignored.
    """
    operators: list[OperatorPredicate]
    links: list[OperatorLink] = []
    operatorPositions: dict[str, Any] | None = None
    commentBoxes: list[Any] | None = None
    settings: dict[str, Any] | None = None

    # Execution parameters (not part of WorkflowContent but added by the caller)
    targetOperatorIds: list[str] = []
    timeoutSeconds: int = 240
    maxResultChars: int = 20000
    maxCellChars: int = 4000


# ---------------------------------------------------------------------------
# Response models — matches SyncExecutionResult
# ---------------------------------------------------------------------------


class PortShape(BaseModel):
    portIndex: int
    rows: int
    columns: int


class OperatorInfo(BaseModel):
    state: str = "Completed"
    inputTuples: int = 0
    outputTuples: int = 0
    inputPortShapes: list[PortShape] = []
    resultMode: str = "table"
    result: list[dict[str, Any]] | None = None
    totalRowCount: int = 0
    displayedRows: int = 0
    truncated: bool = False
    consoleLogs: list[dict[str, str]] = []
    error: str | None = None
    warnings: list[str] = []


class ExecuteResponse(BaseModel):
    success: bool
    state: str
    operators: dict[str, OperatorInfo]
    compilationErrors: dict[str, str] | None = None
    errors: list[str] | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_exec_namespace() -> dict[str, Any]:
    """
    Build an exec namespace pre-populated with common imports that Texera
    Python UDF operators expect to be available (e.g. ``pd``, ``np``).
    """
    ns: dict[str, Any] = {"pd": pd}
    try:
        import numpy as np
        ns["np"] = np
    except ImportError:
        pass
    return ns


def _extract_function(code: str, name: str):
    """Execute *code* and return the function called *name*, or ``None``."""
    ns = _make_exec_namespace()
    exec(compile(code, "<operator>", "exec"), ns)
    return ns.get(name)


def _build_hamilton_module(
    operators: list[OperatorPredicate],
    links: list[OperatorLink],
) -> types.ModuleType:
    """
    Translate a Texera WorkflowContent into a Hamilton-compatible Python module.

    Strategy:
    - Each operator's ``load()``/``process()`` is extracted from
      ``operatorProperties.code``.
    - The function is renamed to the operator's ``operatorID`` so Hamilton
      treats it as a unique node.
    - For ``process()`` functions, the parameter names are rewritten to match
      the upstream operator IDs (derived from *links*) so Hamilton resolves
      dependencies automatically.
    """
    module = types.ModuleType("_hamilton_dag")
    # Register module in sys.modules so inspect.getmodule() can resolve it.
    # Hamilton's find_functions uses inspect.getmodule(fn) which looks up
    # fn.__module__ in sys.modules — without this registration it returns None.
    sys.modules[module.__name__] = module

    # Build upstream map: targetOpId -> {portIndex: sourceOpId}
    # Uses link.target.portID to resolve port indices via operator inputPorts.
    op_map: dict[str, OperatorPredicate] = {op.operatorID: op for op in operators}

    upstream: dict[str, dict[int, str]] = {}
    for link in links:
        target_op = op_map.get(link.target.operatorID)
        if target_op is None:
            continue
        # Resolve port index from portID string
        port_idx = 0
        for i, port in enumerate(target_op.inputPorts):
            if port.portID == link.target.portID:
                port_idx = i
                break
        upstream.setdefault(link.target.operatorID, {})[port_idx] = link.source.operatorID

    for op in operators:
        if op.isDisabled:
            continue

        code = (op.operatorProperties.get("code") or "").strip()
        if not code:
            continue

        # Try process() first (has upstream deps), then load() (source node)
        try:
            fn = _extract_function(code, "process") or _extract_function(code, "load")
        except Exception as e:
            raise _OperatorCompileError(op.operatorID, e) from e
        if fn is None:
            continue

        sig = inspect.signature(fn)
        param_names = list(sig.parameters.keys())
        upstream_ports = upstream.get(op.operatorID, {})

        # Get the original return annotation (Hamilton requires return type hints).
        # If the original function has one, use it; otherwise default to pd.DataFrame.
        return_annotation = fn.__annotations__.get("return", pd.DataFrame)

        if upstream_ports:
            # process() — rewrite parameter names to upstream operator IDs
            new_params: list[str] = []
            for i, pname in enumerate(param_names):
                if i in upstream_ports and upstream_ports[i]:
                    new_params.append(upstream_ports[i])
                else:
                    new_params.append(pname)

            args_str = ", ".join(new_params)
            call_args = ", ".join(new_params)
            wrapper_src = (
                f"def {op.operatorID}({args_str}):\n"
                f"    return __original_fn__({call_args})\n"
            )
            ns: dict[str, Any] = {"__original_fn__": fn}
            exec(compile(wrapper_src, "<wrapper>", "exec"), ns)
            wrapper = ns[op.operatorID]
            # Set type annotations: each param is pd.DataFrame, return is original
            wrapper.__annotations__ = {
                p: pd.DataFrame for p in new_params
            }
            wrapper.__annotations__["return"] = return_annotation
        else:
            # load() — no upstream dependencies, just rename
            def _make_loader(_fn=fn):
                def _loader() -> pd.DataFrame:
                    return _fn()
                return _loader

            wrapper = _make_loader(fn)
            wrapper.__name__ = op.operatorID
            wrapper.__qualname__ = op.operatorID
            wrapper.__annotations__ = {"return": return_annotation}

        # Hamilton introspects __module__ to filter module members
        wrapper.__module__ = module.__name__
        setattr(module, op.operatorID, wrapper)

    return module


def _to_dataframe(value: Any) -> pd.DataFrame:
    """Convert any Hamilton node output to a DataFrame."""
    if isinstance(value, pd.DataFrame):
        df = value
    elif isinstance(value, pd.Series):
        df = value.to_frame()
    elif isinstance(value, list):
        df = pd.DataFrame(value) if value else pd.DataFrame()
    elif isinstance(value, dict):
        df = pd.DataFrame([value])
    else:
        df = pd.DataFrame([{"result": value}])

    # Ensure column names are strings to satisfy Pydantic validation
    df.columns = df.columns.astype(str)
    return df


def _truncate_cell(value: Any, max_chars: int) -> Any:
    """Truncate a single cell value if it's a string exceeding the limit."""
    if isinstance(value, str) and len(value) > max_chars:
        half = (max_chars - len("...[truncated]...")) // 2
        if half > 0:
            return value[:half] + "...[truncated]..." + value[-half:]
        return value[:max_chars]
    return value


def _estimate_record_size(record: dict[str, Any]) -> int:
    """Estimate the JSON-serialized size of a single record."""
    # Fast approximation: sum of key and value string lengths + overhead
    size = 2  # braces
    for k, v in record.items():
        size += len(k) + 4  # key + quotes + colon + comma
        if v is None:
            size += 4
        elif isinstance(v, str):
            size += len(v) + 2
        elif isinstance(v, bool):
            size += 5
        elif isinstance(v, (int, float)):
            size += len(str(v))
        else:
            size += len(str(v))
    return size


def _to_result(
    value: Any, max_result_chars: int, max_cell_chars: int
) -> tuple[list[dict[str, Any]], int, int, bool]:
    """
    Convert any Hamilton node output to ``(records, total, displayed, truncated)``.

    Uses character-aware symmetric truncation (front half + back half) matching
    the Texera Scala backend's ``collectOperatorResult`` strategy.
    """
    df = _to_dataframe(value)
    total = len(df)

    if total == 0:
        return [], 0, 0, False

    records = df.to_dict(orient="records")

    # Apply per-cell truncation
    records = [
        {k: _truncate_cell(v, max_cell_chars) for k, v in rec.items()}
        for rec in records
    ]

    # Check if first record alone exceeds the budget
    first_size = _estimate_record_size(records[0])
    if first_size >= max_result_chars:
        return [records[0]], total, 1, True

    # Symmetric truncation: front half budget + back half budget
    half_limit = max_result_chars // 2

    # Collect front records
    front: list[dict[str, Any]] = []
    front_size = 0
    for rec in records:
        rec_size = _estimate_record_size(rec)
        if front_size + rec_size > half_limit and front:
            break
        front.append(rec)
        front_size += rec_size

    # If all records fit in front half, no need for back
    if len(front) >= total:
        return front, total, len(front), False

    # Collect back records (sliding window from remaining)
    back: list[tuple[dict[str, Any], int]] = []
    back_size = 0
    for rec in records[len(front):]:
        rec_size = _estimate_record_size(rec)
        back.append((rec, rec_size))
        back_size += rec_size
        # Evict from front of back buffer if over budget
        while back_size > half_limit and len(back) > 1:
            _, removed_size = back.pop(0)
            back_size -= removed_size

    result = front + [r for r, _ in back]
    return result, total, len(result), len(result) < total


# ---------------------------------------------------------------------------
# Error handling helpers
# ---------------------------------------------------------------------------


class _OperatorCompileError(Exception):
    """Raised when an operator's code fails to compile or load."""

    def __init__(self, operator_id: str, original: Exception):
        self.operator_id = operator_id
        self.original = original
        super().__init__(str(original))


def _parse_operator_error(exc: Exception) -> tuple[str | None, str]:
    """
    Extract the failing operator ID and a clean, single-line error message
    from a Hamilton execution exception.

    Returns ``(operator_id_or_none, clean_message)``.
    """
    # SyntaxError from compile() carries line info directly
    if isinstance(exc, SyntaxError) and exc.filename == "<operator>":
        return None, f"Line {exc.lineno}: SyntaxError: {exc.msg}"

    tb_entries = traceback.extract_tb(exc.__traceback__)
    operator_id: str | None = None
    operator_line: int | None = None
    operator_func: str | None = None

    for frame in tb_entries:
        if frame.filename == "<wrapper>":
            operator_id = frame.name
        elif frame.filename == "<operator>":
            operator_line = frame.lineno
            operator_func = frame.name

    error_type = type(exc).__name__
    error_msg = str(exc)

    if operator_line is not None:
        func_part = (
            f" in {operator_func}()"
            if operator_func and operator_func != "<module>"
            else ""
        )
        return operator_id, f"Line {operator_line}{func_part}: {error_type}: {error_msg}"

    return operator_id, f"{error_type}: {error_msg}"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/execute", response_model=ExecuteResponse)
def execute(req: ExecuteRequest):
    """
    Execute a Texera workflow via Hamilton.

    Accepts the full WorkflowContent JSON (operators, links, positions, etc.)
    plus ``targetOperatorIds`` and ``maxResultRows``.
    """
    captured = io.StringIO()
    old_stdout = sys.stdout

    try:
        module = _build_hamilton_module(req.operators, req.links)

        # Determine target nodes
        targets = req.targetOperatorIds
        if not targets:
            # Default: all leaf operators (no outgoing links)
            sources = {link.source.operatorID for link in req.links}
            targets = [
                op.operatorID
                for op in req.operators
                if not op.isDisabled and op.operatorID not in sources
            ]
            if not targets:
                targets = [
                    op.operatorID for op in req.operators if not op.isDisabled
                ]

        # Validate all targets exist in the module
        missing = [t for t in targets if not hasattr(module, t)]
        if missing:
            return ExecuteResponse(
                success=False,
                state="CompilationFailed",
                operators={},
                compilationErrors={
                    m: f"No executable code found for operator '{m}'" for m in missing
                },
            )

        # Also request direct upstream operators so we can report inputPortShapes
        upstream_ops: set[str] = set()
        for link in req.links:
            if link.target.operatorID in targets:
                upstream_ops.add(link.source.operatorID)
        all_requested = list(dict.fromkeys(targets + [u for u in upstream_ops if hasattr(module, u)]))

        sys.stdout = captured

        # Build and execute the Hamilton DAG
        try:
            dr = driver.Builder().with_modules(module).build()
            results = dr.execute(all_requested)
        except Exception as exec_err:
            sys.stdout = old_stdout
            console_output = captured.getvalue()
            op_id, clean_error = _parse_operator_error(exec_err)

            operators_info: dict[str, OperatorInfo] = {}
            if op_id and op_id in {op.operatorID for op in req.operators}:
                operators_info[op_id] = OperatorInfo(
                    state="Failed",
                    error=clean_error,
                    consoleLogs=(
                        [{"msgType": "PRINT", "message": console_output}]
                        if console_output
                        else []
                    ),
                )

            return ExecuteResponse(
                success=False,
                state="Failed",
                operators=operators_info,
                errors=[clean_error] if not op_id else None,
            )

        sys.stdout = old_stdout
        console_output = captured.getvalue()

        # ------------------------------------------------------------------
        # Build per-operator response
        # ------------------------------------------------------------------

        # Pre-compute upstream map for input shape reporting
        # opId -> {portIndex: sourceOpId}
        op_map = {op.operatorID: op for op in req.operators}
        input_map: dict[str, dict[int, str]] = {}
        for link in req.links:
            target_op = op_map.get(link.target.operatorID)
            if target_op is None:
                continue
            port_idx = 0
            for i, port in enumerate(target_op.inputPorts):
                if port.portID == link.target.portID:
                    port_idx = i
                    break
            input_map.setdefault(link.target.operatorID, {})[port_idx] = link.source.operatorID

        operators_info: dict[str, OperatorInfo] = {}

        for op_id in targets:
            value = results.get(op_id)
            if value is None:
                operators_info[op_id] = OperatorInfo(
                    state="Completed",
                    result=[],
                    totalRowCount=0,
                    displayedRows=0,
                )
                continue

            records, total, displayed, truncated = _to_result(
                value, req.maxResultChars, req.maxCellChars
            )
            columns = len(records[0]) if records else 0

            # Input port shapes from upstream results
            input_shapes: list[PortShape] = []
            if op_id in input_map:
                for port_idx, from_op in sorted(input_map[op_id].items()):
                    upstream_val = results.get(from_op)
                    if upstream_val is not None:
                        # For upstream shapes we only need row/col counts, not truncated data
                        up_df = _to_dataframe(upstream_val)
                        up_total = len(up_df)
                        up_cols = len(up_df.columns)
                        input_shapes.append(
                            PortShape(portIndex=port_idx, rows=up_total, columns=up_cols)
                        )

            console_logs: list[dict[str, str]] = []
            if console_output:
                console_logs = [{"msgType": "PRINT", "message": console_output}]

            operators_info[op_id] = OperatorInfo(
                state="Completed",
                inputTuples=sum(s.rows for s in input_shapes),
                outputTuples=total,
                inputPortShapes=input_shapes,
                result=records,
                totalRowCount=total,
                displayedRows=displayed,
                truncated=truncated,
                consoleLogs=console_logs,
            )

        return ExecuteResponse(
            success=True, state="Completed", operators=operators_info
        )

    except _OperatorCompileError as comp_err:
        sys.stdout = old_stdout
        _, clean_error = _parse_operator_error(comp_err.original)
        return ExecuteResponse(
            success=False,
            state="CompilationFailed",
            operators={
                comp_err.operator_id: OperatorInfo(
                    state="Failed", error=clean_error
                )
            },
            compilationErrors={comp_err.operator_id: clean_error},
        )
    except Exception:
        sys.stdout = old_stdout
        tb = traceback.format_exc()
        return ExecuteResponse(
            success=False,
            state="Failed",
            operators={},
            errors=[tb],
        )
    finally:
        # Clean up the temporary module from sys.modules
        sys.modules.pop("_hamilton_dag", None)
