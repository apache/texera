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

"""Contextual dependency graph."""

from __future__ import annotations

from dataclasses import dataclass

from python_to_workflow.mosaic.model import (
    ActionTransition,
    Carrier,
    ContextualActionId,
    ReachingPair,
    reaching_pair_key,
)


class InvalidDependenceGraphError(ValueError):
    """Graph rows are noncanonical or contradict their endpoint transitions."""


@dataclass(frozen=True)
class ActionDependenceGraph:
    """Action dependencies independent of source rendering."""

    transitions: tuple[ActionTransition[ContextualActionId], ...]
    reaching_pairs: tuple[ReachingPair[ContextualActionId], ...]
    declared_inputs: frozenset[Carrier]

    def __post_init__(self) -> None:
        transition_ids = tuple(row.action_id for row in self.transitions)
        canonical_pairs = tuple(sorted(set(self.reaching_pairs), key=_pair_key))
        if (
            any(
                not isinstance(action_id, ContextualActionId)
                for action_id in transition_ids
            )
            or len(transition_ids) != len(set(transition_ids))
            or self.transitions
            != tuple(sorted(self.transitions, key=lambda row: row.action_id))
            or self.reaching_pairs != canonical_pairs
        ):
            raise InvalidDependenceGraphError(
                "dependence graph must contain canonical contextual Action rows"
            )
        _validate_pair_incidence(self.transitions, self.reaching_pairs)
        _validate_requirement_supply(
            self.transitions,
            self.reaching_pairs,
            self.declared_inputs,
        )
        _validate_declared_inputs(self.transitions, self.declared_inputs)


def _validate_pair_incidence(
    transitions: tuple[ActionTransition[ContextualActionId], ...],
    pairs: tuple[ReachingPair[ContextualActionId], ...],
) -> None:
    by_action = {transition.action_id: transition for transition in transitions}
    for pair in pairs:
        if (
            pair.producer_action not in by_action
            or pair.consumer_action not in by_action
            or pair.carrier not in by_action[pair.producer_action].establishes
            or pair.carrier not in by_action[pair.consumer_action].requires
        ):
            raise InvalidDependenceGraphError(
                "dependence pair is incompatible with its endpoint transitions"
            )


def _validate_requirement_supply(
    transitions: tuple[ActionTransition[ContextualActionId], ...],
    pairs: tuple[ReachingPair[ContextualActionId], ...],
    declared_inputs: frozenset[Carrier],
) -> None:
    """Require every contextual input to have one graph or entry supply."""

    reached = {(pair.consumer_action, pair.carrier) for pair in pairs}
    orphan = next(
        (
            (transition.action_id, carrier)
            for transition in transitions
            for carrier in transition.requires
            if carrier not in declared_inputs
            and (transition.action_id, carrier) not in reached
        ),
        None,
    )
    if orphan is not None:
        raise InvalidDependenceGraphError(
            f"dependence requirement has no supply: {orphan!r}"
        )


def _validate_declared_inputs(
    transitions: tuple[ActionTransition[ContextualActionId], ...],
    declared_inputs: frozenset[Carrier],
) -> None:
    """Reject entry declarations that no exact Action actually requires."""

    required = {
        carrier for transition in transitions for carrier in transition.requires
    }
    unused = declared_inputs - required
    if unused:
        raise InvalidDependenceGraphError(
            f"declared input has no consumer: {sorted(map(repr, unused))!r}"
        )


def _pair_key(
    pair: ReachingPair[ContextualActionId],
) -> tuple[object, ...]:
    return reaching_pair_key(pair)
