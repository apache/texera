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

"""Cloudpickle transport for explicit workflow boundary payloads."""

from __future__ import annotations

import pickle
from dataclasses import dataclass

import cloudpickle


@dataclass(frozen=True, order=True)
class BoundaryPayload:
    """One boundary contract, its path-present fields, and encoded values."""

    boundary_id: str
    fields: tuple[str, ...]
    present: tuple[str, ...]
    payload: bytes

    def __post_init__(self) -> None:
        if not self.boundary_id:
            raise ValueError("boundary ID must be nonempty")
        if self.fields != tuple(sorted(set(self.fields))) or any(
            not field.isidentifier() for field in self.fields
        ):
            raise ValueError("boundary fields must be canonical Python names")
        if self.present != tuple(
            field for field in self.fields if field in frozenset(self.present)
        ):
            raise ValueError("present fields must be a canonical contract subset")
        if not isinstance(self.payload, bytes):
            raise TypeError("boundary payload must be bytes")


@dataclass(frozen=True)
class WorkflowEnvelope:
    """All selected boundary payloads for one independent execution key."""

    execution_key: str
    boundaries: tuple[BoundaryPayload, ...]

    def __post_init__(self) -> None:
        if not self.execution_key:
            raise ValueError("execution key must be nonempty")
        if not isinstance(self.boundaries, tuple) or any(
            not isinstance(row, BoundaryPayload) for row in self.boundaries
        ):
            raise TypeError("workflow boundaries must be a typed tuple")
        ids = tuple(row.boundary_id for row in self.boundaries)
        if ids != tuple(sorted(set(ids))):
            raise ValueError("workflow boundaries must be canonical and unique")


def encode_boundary(
    boundary_id: str,
    fields: tuple[str, ...],
    values: tuple[object, ...],
    *,
    present: tuple[str, ...] | None = None,
) -> BoundaryPayload:
    """Encode values present on this path under one selected field contract."""

    present = fields if present is None else present
    if len(present) != len(values):
        raise ValueError("present boundary fields and values must have equal length")
    payload = cloudpickle.dumps(values, protocol=pickle.HIGHEST_PROTOCOL)
    return BoundaryPayload(boundary_id, fields, present, payload)


def decode_boundary(
    boundary: BoundaryPayload,
    fields: tuple[str, ...],
) -> tuple[object, ...]:
    """Decode a payload only under its exact selected field contract."""

    if boundary.fields != fields:
        raise ValueError("boundary fields do not match the requested contract")
    values = cloudpickle.loads(boundary.payload)
    if not isinstance(values, tuple) or len(values) != len(boundary.present):
        raise ValueError("decoded boundary payload has an invalid shape")
    return values


def dumps_envelope(envelope: WorkflowEnvelope) -> bytes:
    """Validate and encode a workflow envelope."""

    if not isinstance(envelope, WorkflowEnvelope):
        raise TypeError("envelope codec requires WorkflowEnvelope")
    return cloudpickle.dumps(envelope, protocol=pickle.HIGHEST_PROTOCOL)


def loads_envelope(payload: bytes) -> WorkflowEnvelope:
    """Decode and type-check one workflow envelope."""

    envelope = cloudpickle.loads(payload)
    if not isinstance(envelope, WorkflowEnvelope):
        raise TypeError("decoded payload is not WorkflowEnvelope")
    return envelope


def merge_envelopes(
    left: WorkflowEnvelope,
    right: WorkflowEnvelope,
) -> WorkflowEnvelope:
    """Merge independent fan-in payloads for the same execution key."""

    if left.execution_key != right.execution_key:
        raise ValueError("cannot merge envelopes with different execution keys")
    boundaries = (*left.boundaries, *right.boundaries)
    return WorkflowEnvelope(
        left.execution_key,
        tuple(sorted(boundaries, key=lambda row: row.boundary_id)),
    )
