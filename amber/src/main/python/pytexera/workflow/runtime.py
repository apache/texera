# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Stateless driver execution over local heaps and explicit boundaries."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from pytexera.workflow.codec import (
    BoundaryPayload,
    WorkflowEnvelope,
    decode_boundary,
    encode_boundary,
)

Driver = Callable[["Heap"], "Heap"]


@dataclass(frozen=True)
class InputPort:
    """Boundary IDs delivered together by one physical input port."""

    boundaries: tuple[str, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "boundaries",
            _boundary_ids(self.boundaries, "input-port"),
        )
        if not self.boundaries:
            raise ValueError("input port must own at least one boundary")


class Heap:
    """One process-local namespace whose user values live behind string keys."""

    __slots__ = (
        "_incoming",
        "_outgoing",
        "_values",
        "_field_owners",
    )

    def __init__(self, envelope: WorkflowEnvelope) -> None:
        object.__setattr__(
            self,
            "_incoming",
            {row.boundary_id: row for row in envelope.boundaries},
        )
        object.__setattr__(self, "_outgoing", {})
        object.__setattr__(self, "_values", {})
        object.__setattr__(self, "_field_owners", {})

    def __getattr__(self, name: str) -> object:
        """Read one non-reserved source binding through attribute syntax."""

        return _read_value(self._values, name)

    def __setattr__(self, name: str, value: object) -> None:
        """Write one non-reserved source binding through attribute syntax."""

        _write_value(self._values, name, value)

    def __delattr__(self, name: str) -> None:
        """Delete one non-reserved source binding with Python name semantics."""

        _delete_value(self._values, name)

    def __getitem__(self, name: str) -> object:
        """Read a source binding whose name collides with the Heap API."""

        return _read_value(self._values, name)

    def __setitem__(self, name: str, value: object) -> None:
        """Write a source binding whose name collides with the Heap API."""

        _write_value(self._values, name, value)

    def __delitem__(self, name: str) -> None:
        """Delete a source binding whose name collides with the Heap API."""

        _delete_value(self._values, name)


class Runtime:
    """Configured boundary contract and exactly one generated driver."""

    def __init__(
        self,
        *,
        input_ports: tuple[InputPort, ...] = (),
        outgoing: tuple[str, ...] = (),
    ) -> None:
        if not isinstance(input_ports, tuple) or any(
            not isinstance(port, InputPort) for port in input_ports
        ):
            raise TypeError("input_ports must contain InputPort values")
        incoming = tuple(
            boundary for port in input_ports for boundary in port.boundaries
        )
        if len(incoming) != len(set(incoming)):
            raise ValueError(
                "each incoming boundary must belong to exactly one input port"
            )
        self.input_ports = input_ports
        self.incoming = incoming
        self.outgoing = _boundary_ids(outgoing, "outgoing")
        self._driver: Driver | None = None

    def driver(self, function: Driver, /) -> Driver:
        """Register the generated driver and return it as a decorator."""

        if self._driver is not None:
            raise RuntimeError("workflow Runtime already has a driver")
        self._driver = function
        return function

    def import_boundary(
        self,
        heap: Heap,
        boundary_id: str,
        fields: tuple[str, ...],
    ) -> None:
        """Restore exactly the values selected for one incoming boundary."""

        if boundary_id not in self.incoming:
            raise ValueError(f"boundary {boundary_id!r} is not incoming")
        try:
            boundary = heap._incoming[boundary_id]
        except KeyError as error:
            raise RuntimeError(
                f"incoming boundary {boundary_id!r} is missing"
            ) from error
        values = decode_boundary(boundary, fields)
        _claim_fields(heap, boundary_id, fields)
        for field, value in zip(
            boundary.present,
            values,
            strict=True,
        ):
            _write_value(heap._values, field, value)

    def export_boundary(
        self,
        heap: Heap,
        boundary_id: str,
        fields: tuple[str, ...],
        namespace: Mapping[str, object] | None = None,
    ) -> None:
        """Encode exactly the values selected for one outgoing boundary."""

        if boundary_id not in self.outgoing:
            raise ValueError(f"boundary {boundary_id!r} is not outgoing")
        if boundary_id in heap._outgoing:
            raise RuntimeError(f"outgoing boundary {boundary_id!r} was exported twice")
        _merge_namespace(heap, fields, namespace)
        present = tuple(
            field for field in fields if _contains_value(heap._values, field)
        )
        values = tuple(_read_value(heap._values, field) for field in present)
        heap._outgoing[boundary_id] = encode_boundary(
            boundary_id,
            fields,
            values,
            present=present,
        )

    def execute(self, envelope: WorkflowEnvelope) -> WorkflowEnvelope:
        """Run the driver locally and return only newly exported boundaries."""

        if self._driver is None:
            raise RuntimeError("workflow Runtime has no driver")
        available = frozenset(row.boundary_id for row in envelope.boundaries)
        missing = set(self.incoming) - available
        if missing:
            raise RuntimeError(
                f"incoming boundaries are missing: {tuple(sorted(missing))}"
            )
        heap = Heap(envelope)
        returned = self._driver(heap)
        if returned is not heap:
            raise RuntimeError("workflow driver must return its input Heap")
        produced = frozenset(heap._outgoing)
        if produced != frozenset(self.outgoing):
            raise RuntimeError(
                "outgoing boundaries differ from the Runtime contract: "
                f"expected {self.outgoing!r}, found {tuple(sorted(produced))!r}"
            )
        rows: tuple[BoundaryPayload, ...] = tuple(
            heap._outgoing[boundary_id] for boundary_id in self.outgoing
        )
        return WorkflowEnvelope(envelope.execution_key, rows)


def _boundary_ids(values: tuple[str, ...], label: str) -> tuple[str, ...]:
    if not isinstance(values, tuple) or values != tuple(sorted(set(values))):
        raise ValueError(f"{label} boundary IDs must be canonical and unique")
    if any(not value for value in values):
        raise ValueError(f"{label} boundary IDs must be nonempty")
    return values


def _validate_field_name(name: str) -> None:
    """Reject ambiguous or non-string keys at the Heap API boundary."""

    if not isinstance(name, str):
        raise TypeError("heap field name must be a string")


def _read_value(values: dict[str, object], name: str, /) -> object:
    """Read one source binding with Python's missing-name semantics."""

    _validate_field_name(name)
    try:
        return values[name]
    except KeyError as error:
        raise NameError(f"name {name!r} is not defined") from error


def _write_value(values: dict[str, object], name: str, value: object, /) -> None:
    """Write one exact source binding into private Heap storage."""

    _validate_field_name(name)
    values[name] = value


def _contains_value(values: dict[str, object], name: str, /) -> bool:
    """Return whether one exact source binding exists."""

    _validate_field_name(name)
    return name in values


def _claim_fields(heap: Heap, boundary_id: str, fields: tuple[str, ...], /) -> None:
    """Atomically assign imported fields to one incoming boundary."""

    for field in fields:
        owner = heap._field_owners.get(field)
        if owner is not None and owner != boundary_id:
            raise RuntimeError(
                f"heap field {field!r} is already owned by boundary {owner!r}"
            )
    for field in fields:
        heap._field_owners[field] = boundary_id


def _delete_value(values: dict[str, object], name: str, /) -> None:
    """Delete one exact source binding or reproduce Python's NameError."""

    _validate_field_name(name)
    try:
        del values[name]
    except KeyError as error:
        raise NameError(f"name {name!r} is not defined") from error


def _merge_namespace(
    heap: Heap,
    fields: tuple[str, ...],
    namespace: Mapping[str, object] | None,
    /,
) -> None:
    """Publish selected ordinary locals without generated per-field branches."""

    if namespace is None:
        return
    for field in fields:
        if field in namespace:
            _write_value(heap._values, field, namespace[field])
