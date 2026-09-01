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

from __future__ import annotations

import pytest
from python_to_workflow.mosaic.graph import (
    ActionDependenceGraph,
    InvalidDependenceGraphError,
)
from python_to_workflow.mosaic.model import (
    ActionTransition,
    Carrier,
    ContextualActionId,
    ReachingPair,
)


def _graph_rows() -> tuple[
    tuple[ActionTransition[ContextualActionId], ...],
    tuple[ReachingPair[ContextualActionId], ...],
]:
    producer = ContextualActionId("s1", "module", "module")
    consumer = ContextualActionId("s2", "module", "module")
    carrier = Carrier("value")
    pair = ReachingPair(producer, consumer, carrier)
    transitions = (
        ActionTransition(producer, frozenset(), frozenset({carrier})),
        ActionTransition(consumer, frozenset({carrier}), frozenset()),
    )
    return transitions, (pair,)


def test_dependence_graph_accepts_exact_endpoint_incidence() -> None:
    transitions, pairs = _graph_rows()

    graph = ActionDependenceGraph(transitions, pairs, frozenset())

    assert graph.reaching_pairs == pairs


def test_dependence_graph_rejects_unestablished_pair() -> None:
    transitions, pairs = _graph_rows()
    producer, consumer = transitions
    invalid = (
        ActionTransition(producer.action_id, frozenset(), frozenset()),
        consumer,
    )

    with pytest.raises(InvalidDependenceGraphError):
        ActionDependenceGraph(invalid, pairs, frozenset())


def test_dependence_graph_rejects_requirement_without_supply() -> None:
    """Every requirement is reached or explicitly declared at graph entry."""

    action = ContextualActionId("s1", "module", "module")
    carrier = Carrier("orphan")
    transitions = (ActionTransition(action, frozenset({carrier}), frozenset()),)

    with pytest.raises(InvalidDependenceGraphError, match="requirement"):
        ActionDependenceGraph(transitions, (), frozenset())


def test_dependence_graph_rejects_declared_input_without_consumer() -> None:
    """Entry requests must belong to an exact Action requirement."""

    action = ContextualActionId("s1", "module", "module")
    unused = Carrier("unused")
    transitions = (ActionTransition(action, frozenset(), frozenset()),)

    with pytest.raises(InvalidDependenceGraphError, match="no consumer"):
        ActionDependenceGraph(transitions, (), frozenset({unused}))


def test_carrier_identity_preserves_python_types() -> None:
    assert Carrier(1) != Carrier(True)
    assert Carrier(("value", 1)) == Carrier(("value", 1))


def test_carrier_identity_rejects_mutable_values() -> None:
    with pytest.raises(TypeError, match="hashable"):
        Carrier(["value"])  # type: ignore[arg-type]
