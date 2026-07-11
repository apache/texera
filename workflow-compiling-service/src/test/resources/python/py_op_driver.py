#!/usr/bin/env python3
#
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
Driver that runs a Texera Python-native operator without spinning up the
Pekko/Arrow worker stack.

Symmetric to ``OpExecHarness`` on the JVM side: take an OpDesc's
``generatePythonCode()`` output (which defines a ``UDFOperatorV2`` /
``UDFTableOperator`` / ``UDFBatchOperator`` / ``UDFSourceOperator`` subclass),
load JSONL+sidecar inputs into ``Tuple`` instances, drive
``open -> process_tuple/on_finish per port -> close``, and write the emitted
tuples back as JSONL+sidecar in the same format ``TupleIO`` reads.

The harness invokes us as::

    python3 py_op_driver.py <config.json>

with ``PYTHONPATH`` pointing at ``amber/src/main/python`` so ``pytexera`` /
``pyamber`` import cleanly.

Config schema (all paths absolute)::

    {
      "operatorCode": "<verbatim string from generatePythonCode()>",
      "isSource": false,
      "portOrder": [0, 1],                        # input-port dependency order
      "inputs":  [{"portIndex": 0, "dataPath": "...", "schemaPath": "..."}],
      "outputs": [{"portIndex": 0, "dataPath": "...", "schema":
                       {"attributes": [{"attributeName": "...",
                                        "attributeType": "..."}]}}]
    }

Output schemas come from the JVM side (``PhysicalOp.propagateSchema``) so
this driver never has to infer them. The driver writes the schema back as a
``.jsonl.schema.json`` sidecar next to each ``dataPath``, matching
``TupleIO.writeTuples``.
"""
from __future__ import annotations

import base64
import inspect
import json
import pickle
import sys
import traceback
from pathlib import Path
from typing import Any, Iterable, Iterator, List, Mapping, Sequence

import pandas as pd

# pytexera re-exports the operator base classes and the Tuple/Table types.
# The Scala side prepends `amber/src/main/python` to PYTHONPATH so these
# resolve. If they don't, raise a clean error rather than a cryptic
# ImportError deep in user code.
try:
    from pytexera import (  # noqa: F401  (used dynamically in user code's globals)
        Batch,
        BatchLike,
        Iterator as PyIterator,  # noqa: F401
        Optional as PyOptional,  # noqa: F401
        Table,
        TableLike,
        Tuple,
        TupleLike,
        UDFBatchOperator,
        UDFOperatorV2,
        UDFSourceOperator,
        UDFTableOperator,
        Union as PyUnion,  # noqa: F401
        logger as pytexera_logger,  # noqa: F401
        overrides,  # noqa: F401
    )
    from core.models.schema.schema import Schema as TexeraSchema
    from core.models.schema.attribute_type import AttributeType, RAW_TYPE_MAPPING
except ImportError as exc:
    sys.stderr.write(
        "py_op_driver.py: failed to import pytexera/pyamber. The harness must "
        "set PYTHONPATH to `amber/src/main/python` and the venv must have all "
        "amber Python deps installed (see amber/requirements.txt).\n"
        f"Underlying error: {exc!r}\n"
    )
    raise


# --------------------------------------------------------------------------
# Schema sidecar I/O.
# --------------------------------------------------------------------------
# The JVM writes attributes using AttributeType's Jackson @JsonValue ("string",
# "integer", "long", "double", "boolean", "timestamp", "binary",
# "large_binary"). The Python Schema's RAW_TYPE_MAPPING uses uppercase keys
# ("STRING", "INTEGER", ...). Translate at the boundary; keep the rest of
# the pipeline using Python's AttributeType enum.
_SCALA_TO_PY_TYPE: Mapping[str, str] = {
    "string": "STRING",
    "integer": "INTEGER",
    "long": "LONG",
    "double": "DOUBLE",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "binary": "BINARY",
    "large_binary": "LARGE_BINARY",
}

_PY_TO_SCALA_TYPE: Mapping[AttributeType, str] = {
    AttributeType.STRING: "string",
    AttributeType.INT: "integer",
    AttributeType.LONG: "long",
    AttributeType.DOUBLE: "double",
    AttributeType.BOOL: "boolean",
    AttributeType.TIMESTAMP: "timestamp",
    AttributeType.BINARY: "binary",
    AttributeType.LARGE_BINARY: "large_binary",
}


def _schema_from_dict(payload: Mapping[str, Any]) -> TexeraSchema:
    raw: "dict[str, str]" = {}
    for attr in payload["attributes"]:
        raw_name = attr["attributeName"]
        raw_type = attr["attributeType"].lower()
        if raw_type not in _SCALA_TO_PY_TYPE:
            raise ValueError(
                f"py_op_driver: unknown attributeType {attr['attributeType']!r} "
                f"for attribute {raw_name!r}"
            )
        raw[raw_name] = _SCALA_TO_PY_TYPE[raw_type]
    return TexeraSchema(raw_schema=raw)


def _schema_to_dict(schema: TexeraSchema) -> "dict[str, Any]":
    return {
        "attributes": [
            {"attributeName": name, "attributeType": _PY_TO_SCALA_TYPE[attr_type]}
            for name, attr_type in schema.as_key_value_pairs()
        ]
    }


def _read_schema_sidecar(data_path: Path) -> TexeraSchema:
    sidecar = data_path.with_name(data_path.name + ".schema.json")
    with sidecar.open("r", encoding="utf-8") as fh:
        return _schema_from_dict(json.load(fh))


def _write_schema_sidecar(data_path: Path, schema: TexeraSchema) -> None:
    sidecar = data_path.with_name(data_path.name + ".schema.json")
    with sidecar.open("w", encoding="utf-8") as fh:
        json.dump(_schema_to_dict(schema), fh)


# --------------------------------------------------------------------------
# Tuple I/O. JSONL with sidecar — same on-disk shape as TupleIO on the JVM.
# --------------------------------------------------------------------------
def _coerce_field(raw: Any, attr_type: AttributeType) -> Any:
    """Coerce a JSON-decoded field to the type the schema expects."""
    if raw is None:
        return None
    if attr_type == AttributeType.STRING:
        return str(raw)
    if attr_type == AttributeType.INT:
        return int(raw)
    if attr_type == AttributeType.LONG:
        return int(raw)
    if attr_type == AttributeType.DOUBLE:
        return float(raw)
    if attr_type == AttributeType.BOOL:
        return bool(raw)
    if attr_type == AttributeType.BINARY:
        return base64.b64decode(raw)
    if attr_type == AttributeType.TIMESTAMP:
        # TupleIO writes java.sql.Timestamp.toString ("YYYY-MM-DD HH:MM:SS[.f]");
        # the native path's schema maps TIMESTAMP -> datetime.datetime, and
        # pandas parses the JDBC form robustly.
        return pd.Timestamp(raw).to_pydatetime()
    # LARGE_BINARY: defer until an operator actually exercises it. Failing loud
    # beats silently passing a string through.
    raise NotImplementedError(
        f"py_op_driver: reading attribute type {attr_type!r} from JSONL is "
        f"not implemented yet"
    )


def _read_tuples(data_path: Path, schema: TexeraSchema) -> List[Tuple]:
    rows: List[Tuple] = []
    if not data_path.exists():
        return rows
    with data_path.open("r", encoding="utf-8") as fh:
        for line_num, raw_line in enumerate(fh, 1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"py_op_driver: invalid JSON on line {line_num} of {data_path}: {exc}"
                ) from exc
            field_data: "dict[str, Any]" = {}
            for name, attr_type in schema.as_key_value_pairs():
                field_data[name] = _coerce_field(obj.get(name), attr_type)
            tup = Tuple(field_data)
            tup.finalize(schema)
            rows.append(tup)
    return rows


def _emit_as_dicts(
    emitted: Iterable[Any], schema: TexeraSchema
) -> Iterator["dict[str, Any]"]:
    """
    Flatten whatever the operator yields into per-row dicts keyed by the
    output schema's attribute names. The operator may yield:
        * pandas.DataFrame  (UDFTableOperator's process_table return)
        * pandas.Series      (single row)
        * dict / OrderedDict (e.g. BarChart yields {'html-content': html})
        * Tuple
        * None               (skip — matches the engine's behavior)
    """
    attr_names = schema.get_attr_names()
    for item in emitted:
        if item is None:
            continue
        if isinstance(item, pd.DataFrame):
            for _, row in item.iterrows():
                yield {col: row[col] for col in attr_names if col in row.index}
        elif isinstance(item, pd.Series):
            yield {col: item[col] for col in attr_names if col in item.index}
        elif isinstance(item, Tuple):
            yield {name: item[name] for name in attr_names}
        elif isinstance(item, Mapping):
            yield {name: item.get(name) for name in attr_names}
        else:
            raise TypeError(
                f"py_op_driver: cannot serialize emitted value of type "
                f"{type(item).__name__}: {item!r}"
            )


def _jsonify(value: Any, attr_type: AttributeType) -> Any:
    """Convert a Python value into something json.dumps will accept."""
    if value is None:
        return None
    # pandas often hands us numpy scalars; .item() collapses them to native.
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        try:
            value = value.item()
        except (ValueError, AttributeError):
            pass
    if attr_type == AttributeType.STRING:
        return str(value)
    if attr_type in (AttributeType.INT, AttributeType.LONG):
        return int(value)
    if attr_type == AttributeType.DOUBLE:
        return float(value)
    if attr_type == AttributeType.BOOL:
        return bool(value)
    if attr_type == AttributeType.BINARY:
        # Trained-model / object columns: pickle then base64 so the value
        # survives JSONL round-trip. Mirrors the BINARY read path in
        # _coerce_field. For deterministic estimators the pickle is byte-stable
        # across processes, so the two verification paths compare equal.
        raw = value if isinstance(value, (bytes, bytearray)) else pickle.dumps(value)
        return base64.b64encode(raw).decode("ascii")
    if attr_type == AttributeType.TIMESTAMP:
        # Emit the same JDBC string java.sql.Timestamp.toString produces (>=1
        # fractional digit), so a passed-through timestamp column matches the
        # standalone path, which carries it as that exact string.
        ts = pd.Timestamp(value)
        frac = f"{ts.microsecond:06d}".rstrip("0") or "0"
        return ts.strftime("%Y-%m-%d %H:%M:%S") + "." + frac
    raise NotImplementedError(
        f"py_op_driver: writing attribute type {attr_type!r} to JSONL is "
        f"not implemented yet"
    )


def _write_tuples(
    data_path: Path, rows: Iterable["dict[str, Any]"], schema: TexeraSchema
) -> None:
    _write_schema_sidecar(data_path, schema)
    with data_path.open("w", encoding="utf-8") as fh:
        for row in rows:
            serialized: "dict[str, Any]" = {}
            for name, attr_type in schema.as_key_value_pairs():
                serialized[name] = _jsonify(row.get(name), attr_type)
            fh.write(json.dumps(serialized))
            fh.write("\n")


# --------------------------------------------------------------------------
# Operator discovery + lifecycle.
# --------------------------------------------------------------------------
_OPERATOR_BASES = (
    UDFOperatorV2,
    UDFTableOperator,
    UDFBatchOperator,
    UDFSourceOperator,
)


def _exec_user_code(code: str) -> "dict[str, Any]":
    """
    Execute the operator code in a fresh namespace seeded with the pytexera
    re-exports, the way the real Texera Python worker does it (see
    ``InitializeExecutorHandler``). Returning the namespace lets us pick the
    user's operator class out of it.
    """
    namespace: "dict[str, Any]" = {
        "__name__": "__texera_user_op__",
        "__builtins__": __builtins__,
    }
    # pytexera does `from pyamber import *` itself, so this single import is
    # equivalent to what the generated code's `from pytexera import *` brings
    # into scope.
    exec("from pytexera import *", namespace)
    try:
        exec(code, namespace)
    except Exception:
        sys.stderr.write("py_op_driver: error executing operator code:\n")
        traceback.print_exc()
        raise
    return namespace


def _discover_operator_class(namespace: Mapping[str, Any]) -> type:
    candidates: List[type] = []
    for name, obj in namespace.items():
        if not inspect.isclass(obj):
            continue
        if obj in _OPERATOR_BASES:
            continue  # the base classes themselves come in via the import
        if any(issubclass(obj, base) for base in _OPERATOR_BASES):
            candidates.append(obj)
    if not candidates:
        raise RuntimeError(
            "py_op_driver: operator code did not define a subclass of "
            "UDFOperatorV2 / UDFTableOperator / UDFBatchOperator / UDFSourceOperator"
        )
    if len(candidates) > 1:
        names = ", ".join(c.__name__ for c in candidates)
        raise RuntimeError(
            f"py_op_driver: operator code defined multiple UDF subclasses "
            f"({names}); expected exactly one"
        )
    return candidates[0]


def _run_operator(
    op: Any,
    is_source: bool,
    port_order: Sequence[int],
    inputs_by_port: Mapping[int, Sequence[Tuple]],
) -> List[Any]:
    """
    Drive the operator's lifecycle. Returns the flat list of emitted values
    (anything not-None yielded by process_tuple / on_finish, in emission
    order). UDF operators don't expose multi-output ports today, so we don't
    bucket by output port — same convention as ``OpExecHarness`` when port
    is unset.
    """
    emitted: List[Any] = []

    op.open()
    try:
        if is_source:
            # Source ops: SourceOperator.on_finish iterates produce() and
            # yields Tuples. Single synthetic port 0 — see OpExecHarness.
            for item in op.on_finish(0):
                if item is not None:
                    emitted.append(item)
            return emitted

        for port in port_order:
            for tup in inputs_by_port.get(port, ()):  # type: ignore[arg-type]
                for item in op.process_tuple(tup, port):
                    if item is not None:
                        emitted.append(item)
            for item in op.on_finish(port):
                if item is not None:
                    emitted.append(item)
    finally:
        op.close()

    return emitted


# --------------------------------------------------------------------------
# Entry point.
# --------------------------------------------------------------------------
def main(argv: Sequence[str]) -> int:
    if len(argv) != 2:
        sys.stderr.write(f"usage: {argv[0]} <config.json>\n")
        return 2
    config_path = Path(argv[1])
    with config_path.open("r", encoding="utf-8") as fh:
        config = json.load(fh)

    operator_code: str = config["operatorCode"]
    is_source: bool = bool(config.get("isSource", False))
    port_order: Sequence[int] = list(config.get("portOrder", []))

    inputs_by_port: "dict[int, List[Tuple]]" = {}
    for entry in config.get("inputs", []):
        port = int(entry["portIndex"])
        data_path = Path(entry["dataPath"])
        schema = _read_schema_sidecar(data_path)
        inputs_by_port[port] = _read_tuples(data_path, schema)

    # Default port order: sorted by index. Matches OpExecHarness's fallback
    # when getInputPortDependencyPairs is empty.
    if not port_order:
        port_order = sorted(inputs_by_port.keys())

    namespace = _exec_user_code(operator_code)
    op_class = _discover_operator_class(namespace)
    op_instance = op_class()

    emitted = _run_operator(op_instance, is_source, port_order, inputs_by_port)

    outputs = config.get("outputs", [])
    if len(outputs) > 1:
        raise NotImplementedError(
            "py_op_driver: multi-output Python operators are not supported "
            "yet (no UDF base class exposes per-port emission)"
        )
    if outputs:
        out_entry = outputs[0]
        out_path = Path(out_entry["dataPath"])
        out_schema = _schema_from_dict(out_entry["schema"])
        rows = list(_emit_as_dicts(emitted, out_schema))
        _write_tuples(out_path, rows, out_schema)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
