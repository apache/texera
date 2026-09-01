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

"""Generic keyed fan-in adapter between Amber tuples and workflow Runtime."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from pytexera.udf.udf_operator import UDFOperatorV2
from pytexera.workflow.codec import (
    WorkflowEnvelope,
    dumps_envelope,
    loads_envelope,
    merge_envelopes,
)
from pytexera.workflow.runtime import Runtime

ENVELOPE_FIELD = "workflow_envelope"
EXECUTION_KEY_FIELD = "execution_key"


@dataclass(frozen=True)
class _PendingExecution:
    envelope: WorkflowEnvelope
    arrived_ports: frozenset[int]


class TupleOperator(UDFOperatorV2):
    """Execute once after all configured boundaries for one key have arrived."""

    runtime: Runtime

    def __init__(self) -> None:
        super().__init__()
        self._pending: dict[str, _PendingExecution] = {}
        self._finished_ports: set[int] = set()

    def process_tuple(self, tuple_, port: int) -> Iterator[dict[str, object]]:
        """Join one keyed port arrival and execute when all ports are present."""

        if not self.runtime.input_ports:
            key = _field(tuple_, EXECUTION_KEY_FIELD, "default")
            yield _output(self.runtime.execute(WorkflowEnvelope(str(key), ())))
            return
        incoming = self._incoming_envelope(tuple_, port)
        pending = self._merge_pending(incoming, port)
        if len(pending.arrived_ports) != len(self.runtime.input_ports):
            return
        del self._pending[incoming.execution_key]
        yield _output(self.runtime.execute(pending.envelope))

    def _incoming_envelope(self, tuple_, port: int) -> WorkflowEnvelope:
        """Validate and retain only boundaries owned by one physical input port."""

        _validate_port(port, len(self.runtime.input_ports))
        if port in self._finished_ports:
            raise RuntimeError(f"input arrived after port {port} finished")
        raw = _field(tuple_, ENVELOPE_FIELD)
        envelope = loads_envelope(raw)
        relevant = tuple(
            row
            for row in envelope.boundaries
            if row.boundary_id in self.runtime.incoming
        )
        expected = frozenset(self.runtime.input_ports[port].boundaries)
        found = frozenset(row.boundary_id for row in relevant)
        if found != expected:
            raise ValueError(
                f"port {port} boundaries differ from its Runtime contract: "
                f"expected {tuple(sorted(expected))!r}, found {tuple(sorted(found))!r}"
            )
        return WorkflowEnvelope(envelope.execution_key, relevant)

    def _merge_pending(
        self,
        incoming: WorkflowEnvelope,
        port: int,
    ) -> _PendingExecution:
        """Merge one validated arrival into its execution-key accumulator."""

        execution_key = incoming.execution_key
        previous = self._pending.get(execution_key)
        if previous is not None and port in previous.arrived_ports:
            raise RuntimeError(
                f"execution {execution_key!r} arrived twice on port {port}"
            )
        merged = (
            incoming
            if previous is None
            else merge_envelopes(previous.envelope, incoming)
        )
        arrived = (
            frozenset((port,)) if previous is None else previous.arrived_ports | {port}
        )
        pending = _PendingExecution(merged, arrived)
        self._pending[execution_key] = pending
        return pending

    def on_finish(self, port: int) -> Iterator[None]:
        """Close one input port and reject incomplete keyed executions."""

        if not self.runtime.input_ports:
            return
            yield None
        _validate_port(port, len(self.runtime.input_ports))
        if port in self._finished_ports:
            raise RuntimeError(f"input port {port} finished twice")
        self._finished_ports.add(port)
        if len(self._finished_ports) == len(self.runtime.input_ports) and self._pending:
            keys = tuple(sorted(self._pending))
            raise RuntimeError(f"incomplete workflow executions at finish: {keys!r}")
        return
        yield None


def _validate_port(port: int, count: int) -> None:
    if not isinstance(port, int) or isinstance(port, bool) or not 0 <= port < count:
        raise ValueError(f"input port must be an integer in [0, {count})")


def _field(tuple_, name: str, default=...) -> object:
    try:
        return tuple_[name]
    except (KeyError, IndexError):
        if default is ...:
            raise ValueError(f"input tuple is missing field {name!r}") from None
        return default


def _output(envelope: WorkflowEnvelope) -> dict[str, object]:
    return {ENVELOPE_FIELD: dumps_envelope(envelope)}
