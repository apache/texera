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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Identity-agnostic, path-sensitive traversal of an ActionForest."""

from __future__ import annotations

import ast
from collections.abc import Callable, Hashable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from python_to_workflow.mosaic.forest import (
    Action,
    ActionForest,
    Expression,
    ForestBuildError,
    Parameter,
    ParameterRole,
    _Builder,
    _Component,
    _component_position,
)

SELF = "SELF"
ENTER = "ENTER"
EXIT = "EXIT"

type Anchor = str | tuple[str, str]
type Identity = Hashable
type Event = tuple[
    Anchor,
    tuple[Identity, ...],
    tuple[Identity, ...],
    tuple[Identity, ...],
]
type Payload = tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]
type ProducerActions = frozenset[str]
type UnresolvedUse = tuple[str, Identity]
type UnresolvedUses = tuple[UnresolvedUse, ...]


def _identity_key(value: object) -> tuple[str, str, str]:
    """Order arbitrary hashable identities without requiring cross-type order."""

    kind = type(value)
    return kind.__module__, kind.__qualname__, repr(value)


def _identity_row_key(row: tuple[object, ...]) -> tuple[tuple[str, str, str], ...]:
    """Order payload rows by their structural identity."""

    return tuple(_identity_key(value) for value in row)


@dataclass(frozen=True)
class ControlWalkResult:
    """Canonical payload plus exact use occurrences that may be unbound."""

    payload: Payload
    unresolved_uses: UnresolvedUses


@dataclass(frozen=True)
class StatementPostState:
    """Writes owned by one complete statement and its normal completions."""

    root: str
    may_write: tuple[Identity, ...]
    must_write: tuple[Identity, ...]
    deletes: tuple[Identity, ...]

    def __post_init__(self) -> None:
        rows = (self.may_write, self.must_write, self.deletes)
        if any(row != tuple(sorted(set(row), key=_identity_key)) for row in rows):
            raise ValueError("statement post-state identities must be canonical")
        if not set(self.must_write) <= set(self.may_write):
            raise ValueError("must-write identities must also be may-write identities")


_WRITE_MARKER = "statement-write"
_DELETE_MARKER = "statement-delete"


@dataclass(frozen=True)
class _WriteEventExtractor:
    """Turn binding definitions and kills into path-visible write markers."""

    source: EventExtractor

    def __call__(self, action: Action, /) -> tuple[Event, ...]:
        rows = []
        for anchor, _uses, defines, kills in self.source(action):
            markers = tuple((_WRITE_MARKER, identity) for identity in defines)
            markers += tuple((_WRITE_MARKER, identity) for identity in kills)
            markers += tuple((_DELETE_MARKER, identity) for identity in kills)
            rows.append((anchor, (), markers, ()))
        return tuple(rows)


@dataclass(frozen=True)
class _DefinitionState:
    """Possible producers plus whether one legal path remains unbound."""

    producers: ProducerActions
    may_be_unbound: bool


type State = dict[Identity, _DefinitionState]
type EntryState = Mapping[Identity, frozenset[str]]
type Outcomes = dict[str, State]
type _DefinitionCounts = dict[Identity, int]
type EndpointRelation = tuple[str, str]
type OwnedEndpointRelation = tuple[str, str, str]
type RepeatedDecisionIndex = Mapping[str, frozenset[str]]
type _ActionNodeIndex = Mapping[str, ast.AST]
type _ActionInventory = frozenset[str]
type _ExtractedActionIds = set[str]
type _DormantParameters = frozenset[str]
type _EvaluationIndices = Mapping[str, int]


class EventExtractor(Protocol):
    """Return canonical anchored uses, definitions, and kills for an Action."""

    def __call__(self, action: Action, /) -> tuple[Event, ...]: ...


@dataclass(frozen=True)
class ControlWalkProgram:
    """Source-exact structural control metadata reusable across flow queries."""

    forest: ActionForest
    _nodes: _ActionNodeIndex
    action_ids: _ActionInventory
    dormant: _DormantParameters
    indices: _EvaluationIndices


class InvalidAnchorError(ValueError):
    """An event anchor does not belong to the Action it labels."""


def controlwalk(forest: ActionForest, events: EventExtractor) -> Payload:
    """Walk legal paths and return canonical private flow facts."""

    return _Walk(compile_controlwalk(forest), events).run()


def compile_controlwalk(forest: ActionForest, /) -> ControlWalkProgram:
    """Compile immutable control structure once for repeated analysis walks."""

    forest.validate()
    builder = _validated_builder(forest)
    _demanded, dormant, indices = _metadata(builder)
    nodes = MappingProxyType(dict(builder.action_nodes))
    return ControlWalkProgram(
        forest,
        nodes,
        frozenset(nodes),
        dormant,
        MappingProxyType(dict(indices)),
    )


def controlwalk_region(
    forest: ActionForest,
    roots: tuple[str, ...],
    incoming: EntryState,
    events: EventExtractor,
    program: ControlWalkProgram | None = None,
) -> Payload:
    """Walk one real lexical region through the same path authority.

    ``incoming`` contains raw analysis identities and their alternative source
    Actions.  It is analysis state only: the walk still emits no Carrier.
    """

    return controlwalk_region_result(forest, roots, incoming, events, program).payload


def controlwalk_region_result(
    forest: ActionForest,
    roots: tuple[str, ...],
    incoming: EntryState,
    events: EventExtractor,
    program: ControlWalkProgram | None = None,
) -> ControlWalkResult:
    """Walk one lexical region and retain path evidence per use occurrence."""

    compiled = _control_program(forest, program)
    if roots != tuple(dict.fromkeys(roots)) or not set(roots) <= compiled.action_ids:
        raise ValueError("control-walk region roots must be unique forest Actions")
    seeded = {
        identity: frozenset(producers) for identity, producers in incoming.items()
    }
    if any(not producers <= compiled.action_ids for producers in seeded.values()):
        raise ValueError("control-walk region seed references an unknown Action")
    return _Walk(compiled, events).run_region_result(roots, seeded)


def statement_post_states(
    forest: ActionForest,
    roots: tuple[str, ...],
    events: EventExtractor,
    program: ControlWalkProgram | None = None,
) -> tuple[StatementPostState, ...]:
    """Derive may/must writes from the same path authority in linear work."""

    compiled = _control_program(forest, program)
    if roots != tuple(dict.fromkeys(roots)) or not set(roots) <= compiled.action_ids:
        raise ValueError("statement post-state roots must be unique forest Actions")
    walk = _Walk(compiled, _WriteEventExtractor(events))
    return tuple(_statement_post_state(walk, root) for root in roots)


def _statement_post_state(walk: _Walk, root: str) -> StatementPostState:
    """Profile one disjoint root without rebuilding its control program."""

    before = set(walk.definitions)
    walk._collect_region_inventory((root,))
    inventory = walk.definitions - before
    normal = walk._action(root, {}).get("normal", {})
    may_write = _marked_identities(inventory, _WRITE_MARKER)
    must_write = tuple(
        sorted(
            (
                marker[1]
                for marker, state in normal.items()
                if _is_marker(marker, _WRITE_MARKER) and not state.may_be_unbound
            ),
            key=_identity_key,
        )
    )
    return StatementPostState(
        root,
        may_write,
        must_write,
        _marked_identities(inventory, _DELETE_MARKER),
    )


def _marked_identities(
    rows: set[tuple[str, Identity]],
    kind: str,
) -> tuple[Identity, ...]:
    """Decode one canonical marker kind from collected definition rows."""

    return tuple(
        sorted(
            {marker[1] for _action, marker in rows if _is_marker(marker, kind)},
            key=_identity_key,
        )
    )


def _is_marker(value: object, kind: str) -> bool:
    return isinstance(value, tuple) and len(value) == 2 and value[0] == kind


def _control_program(
    forest: ActionForest, program: ControlWalkProgram | None
) -> ControlWalkProgram:
    """Accept only a program compiled for this exact immutable forest."""

    if program is None:
        return compile_controlwalk(forest)
    if program.forest is not forest:
        raise ValueError("control-walk program belongs to another ActionForest")
    return program


def region_completion_actions(
    forest: ActionForest,
    roots: tuple[str, ...],
    program: ControlWalkProgram | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return exact terminal Action ids by generic completion kind.

    The same protocol walk owns both order and completion propagation.  A
    private marker records only the last Action reached on each legal path;
    it does not become a fact, Carrier, or second control authority.
    """

    marker = ("terminal",)

    def events(action: Action) -> tuple[Event, ...]:
        return ((SELF, (), (marker,), ()),)

    compiled = _control_program(forest, program)
    walk = _Walk(compiled, events)
    outcomes = walk._sequence(roots, {})
    return tuple(
        sorted(
            (kind, producer)
            for kind, state in outcomes.items()
            for producer in _state_producers(state, marker)
        )
    )


def _state_producers(state: State, identity: Identity) -> ProducerActions:
    """Read possible producers without exposing internal boundness state."""

    row = state.get(identity)
    return frozenset() if row is None else row.producers


def control_region_owners(forest: ActionForest) -> tuple[str, ...]:
    """Derive real Actions that own structured runtime control regions."""

    nodes = _validated_builder(forest).action_nodes
    region_types = (
        ast.BoolOp,
        ast.For,
        ast.If,
        ast.IfExp,
        ast.Match,
        ast.Try,
        ast.While,
        ast.With,
    )
    return tuple(
        sorted(
            action_id
            for action_id, node in nodes.items()
            if isinstance(node, region_types)
        )
    )


def completion_region_owners(forest: ActionForest) -> tuple[str, ...]:
    """Return source owners whose runtime semantics thread completions."""

    nodes = _validated_builder(forest).action_nodes
    return tuple(
        sorted(
            action_id
            for action_id, node in nodes.items()
            if isinstance(node, (ast.Try, ast.With))
        )
    )


def locally_handled_name_error_uses(
    forest: ActionForest,
    uses: UnresolvedUses,
    reaching: tuple[tuple[object, ...], ...],
    /,
) -> UnresolvedUses:
    """Return exact unbound-name occurrences handled by an enclosing try.

    This is control evidence, not a binding heuristic: only occurrences in a
    real try body qualify, and finally/else/handler regions remain outside the
    handler they follow.
    """

    nodes = _validated_builder(forest).action_nodes
    unresolved = frozenset(uses)
    reached = frozenset(
        (consumer, identity) for _producer, consumer, identity in reaching
    )
    evidence = _HandlerEvidence(
        forest,
        nodes,
        {id(node): action_id for action_id, node in nodes.items()},
        unresolved,
        reached,
    )
    handled: dict[str, bool] = {}

    def qualifies(action_id: str) -> bool:
        if action_id not in handled:
            handled[action_id] = _has_enclosing_name_error_handler(evidence, action_id)
        return handled[action_id]

    return tuple(row for row in uses if qualifies(row[0]))


@dataclass(frozen=True)
class _HandlerEvidence:
    """Immutable indices for exact exception-selector certification."""

    forest: ActionForest
    nodes: Mapping[str, ast.AST]
    action_by_node: Mapping[int, str]
    unresolved: frozenset[tuple[str, Hashable]]
    reached: frozenset[tuple[str, Hashable]]


def _has_enclosing_name_error_handler(
    evidence: _HandlerEvidence,
    action_id: str,
) -> bool:
    current = action_id
    while (parameter_id := evidence.forest.parent_parameter(current)) is not None:
        parameter = evidence.forest.parameter(parameter_id)
        owner = parameter.owner
        node = evidence.nodes[owner]
        if (
            isinstance(node, (ast.Try, ast.TryStar))
            and parameter.name == "body"
            and any(
                _catches_name_error(
                    handler.type,
                    evidence.action_by_node,
                    evidence.unresolved,
                    evidence.reached,
                )
                for handler in node.handlers
            )
        ):
            return True
        current = owner
    return False


def _catches_name_error(
    node: ast.expr | None,
    action_by_node: Mapping[int, str],
    unresolved: frozenset[tuple[str, Hashable]],
    reached: frozenset[tuple[str, Hashable]],
) -> bool:
    """Accept a named selector only when binding flow proves builtin fallback."""

    if node is None:
        return True
    if isinstance(node, ast.Name):
        if node.id not in {"NameError", "Exception", "BaseException"}:
            return False
        action_id = action_by_node.get(id(node))
        occurrence = (action_id, ("module", node.id))
        return (
            action_id is not None
            and occurrence in unresolved
            and occurrence not in reached
        )
    if isinstance(node, ast.Tuple):
        return any(
            _catches_name_error(item, action_by_node, unresolved, reached)
            for item in node.elts
        )
    return False


@dataclass(frozen=True)
class CompletionStage:
    """Source Actions plus exact entry and exit frontiers of one SUITE stage."""

    parameter_id: str
    actions: frozenset[str]
    entries: tuple[str, ...]
    exits: tuple[str, ...]


@dataclass(frozen=True)
class ChoiceRegion:
    """One source choice, its complete inventory, and terminal frontier."""

    owner: str
    members: frozenset[str]
    exits: tuple[str, ...]


@dataclass(frozen=True)
class ExceptionHandler:
    """One source-ordered exception selector and its owned handler body."""

    selector: CompletionStage | None
    body: CompletionStage


@dataclass(frozen=True)
class CompletionRegion:
    """The source-exact stages governed by one Python try Action."""

    owner: str
    body: CompletionStage
    handlers: tuple[ExceptionHandler, ...]
    orelse: CompletionStage | None
    finalbody: CompletionStage | None


@dataclass(frozen=True)
class WithRegion:
    """One source-exact context-manager owner and its protected body stage."""

    owner: str
    body: CompletionStage


@dataclass(frozen=True)
class IterationRegion:
    """One loop owner with its repeated body and exhaustion-only else stage."""

    owner: str
    body: CompletionStage
    orelse: CompletionStage | None


@dataclass(frozen=True)
class IterationProtocolIndex:
    """Immutable loop containment shared by every structural projection."""

    regions: tuple[IterationRegion, ...]
    members: Mapping[str, frozenset[str]]
    body_owners: Mapping[str, tuple[str, ...]]
    admission_owner: Mapping[str, str]


@dataclass(frozen=True)
class ControlRelationProjection:
    """Precise control rows and exact baseline world relations they replace."""

    owned_relations: tuple[OwnedEndpointRelation, ...]
    retired_relations: tuple[EndpointRelation, ...]


def control_relations(
    forest: ActionForest, relations: tuple[tuple[str, str], ...]
) -> ControlRelationProjection:
    """Return owner, producer, consumer rows for every control Carrier.

    Ordinary regions mirror exact dependency endpoints.  ``try`` regions also
    emit standalone owner-entry and clause-handoff bases so every ordinary
    continuation is solver-visible before coloring.  Their source-ordered
    spine reaches the first selector, bypasses ``else`` after a handler, and
    reaches ``finally`` without a raising-site × handler expansion.
    """

    if relations != tuple(sorted(set(relations))):
        raise ValueError("control endpoint relations must be canonical")
    builder = _validated_builder(forest)
    entries = _EntryActions(forest, builder)
    tries = completion_regions(forest, builder, entries)
    contexts = with_regions(forest, builder, entries)
    try_owners = frozenset(region.owner for region in tries)
    ordinary = frozenset(control_region_owners(forest)) - try_owners
    rows = _ordinary_control_relations(forest, relations, ordinary)
    rows = {row for row in rows if not _crosses_enclosing_try(row, tries)}
    replaced: set[EndpointRelation] = set()
    for region in tries:
        rows.update(_try_control_relations(forest, relations, region))
        rows.update(
            _try_physical_route_bases(forest, relations, region, tries, entries)
        )
        replaced.update(_try_replaced_relations(relations, region))
    for region in contexts:
        rows.update(_with_control_relations(region))
    return ControlRelationProjection(tuple(sorted(rows)), tuple(sorted(replaced)))


def _crosses_enclosing_try(
    row: OwnedEndpointRelation, regions: tuple[CompletionRegion, ...]
) -> bool:
    """Stop a nested region's control identity at its enclosing try boundary."""

    owner, producer, consumer = row
    for region in regions:
        stages = _try_stages(region)
        actions = _try_region_actions(region, stages)
        if owner in actions and {producer, consumer} <= actions:
            return not _same_stage(producer, consumer, stages)
    return False


def _ordinary_control_relations(
    forest: ActionForest,
    relations: tuple[tuple[str, str], ...],
    owners: frozenset[str],
) -> set[tuple[str, str, str]]:
    """Mirror continuity and admit every independently scheduled SUITE root."""

    result: set[tuple[str, str, str]] = set()
    for producer, consumer in relations:
        ancestors = _endpoint_ancestors(forest, producer, consumer)
        result.update((owner, producer, consumer) for owner in owners & ancestors)
    loop_owners = frozenset(iteration_region_owners(forest))
    for owner in owners - loop_owners:
        result.update(
            (owner, owner, target)
            for target in _suite_admission_roots(forest, owner, relations)
        )
    return result


def _suite_admission_roots(
    forest: ActionForest,
    owner: str,
    relations: tuple[EndpointRelation, ...],
    stage: frozenset[str] | None = None,
) -> tuple[str, ...]:
    """Find physical SUITE roots after analysis has discharged safe ordering.

    Source order alone is insufficient here: eager-safe siblings may no longer
    have an ordinary edge between them, but each still needs the dynamic
    region's admission token.  Nested SUITEs belong to their own nearest owner;
    non-SUITE parameters of a nested owner remain part of the enclosing stage.
    """

    candidates = frozenset(
        action.id
        for action in forest.actions
        if (
            _nearest_suite_owner(forest, action.id) == owner
            or stage is not None
            and forest.parent_action(action.id) == owner
        )
        and (stage is None or action.id in stage)
    )
    inventory = candidates if stage is None else stage
    internal_consumers = frozenset(
        consumer
        for producer, consumer in relations
        if producer in inventory and consumer in inventory
    )
    return tuple(sorted(candidates - internal_consumers))


def _nearest_suite_owner(forest: ActionForest, action_id: str) -> str | None:
    """Return the owner of the nearest source SUITE containing one Action."""

    current = action_id
    while (parameter_id := forest.parent_parameter(current)) is not None:
        parameter = forest.parameter(parameter_id)
        if parameter.role is ParameterRole.SUITE:
            return parameter.owner
        current = parameter.owner
    return None


def _endpoint_ancestors(
    forest: ActionForest, producer: str, consumer: str
) -> frozenset[str]:
    """Return both endpoint ownership spines, including the endpoints."""

    result: set[str] = set()
    for endpoint in (producer, consumer):
        current: str | None = endpoint
        while current is not None:
            result.add(current)
            current = forest.parent_action(current)
    return frozenset(result)


def completion_regions(
    forest: ActionForest,
    builder: _Builder | None = None,
    entries: _EntryActions | None = None,
) -> tuple[CompletionRegion, ...]:
    """Build every try protocol once from source-exact forest Parameters."""

    builder = _validated_builder(forest) if builder is None else builder
    entries = _EntryActions(forest, builder) if entries is None else entries
    owners = tuple(
        sorted(
            action_id
            for action_id, node in builder.action_nodes.items()
            if isinstance(node, ast.Try)
        )
    )
    return tuple(_try_region(forest, owner, entries) for owner in owners)


def completion_resumption_exits(
    forest: ActionForest,
    region: CompletionRegion,
    stage: CompletionStage,
    regions: tuple[CompletionRegion, ...],
) -> tuple[str, ...]:
    """Derive the post-passthrough physical frontier of one try stage."""

    nested = tuple(
        (candidate, _try_region_actions(candidate, _try_stages(candidate)))
        for candidate in regions
        if candidate.owner != region.owner and candidate.owner in stage.actions
    )
    parameter = forest.parameter(stage.parameter_id)
    physical_roots = parameter.actions[-1:]
    semantic_exits = tuple(
        action_id
        for _completion, action_id in region_completion_actions(forest, physical_roots)
    )
    exits = {
        replacement
        for source in semantic_exits
        for replacement in _resumption_exit(source, nested)
    }
    return tuple(sorted(exits))


def _resumption_exit(
    source: str,
    nested: tuple[tuple[CompletionRegion, frozenset[str]], ...],
) -> tuple[str, ...]:
    """Select the immediate nested owner containing one semantic exit."""

    owners = tuple(row for row in nested if source in row[1])
    if not owners:
        return (source,)
    region, _members = max(owners, key=lambda row: len(row[1]))
    return _try_stages(region)[-1].exits


def with_regions(
    forest: ActionForest,
    builder: _Builder | None = None,
    entries: _EntryActions | None = None,
) -> tuple[WithRegion, ...]:
    """Build every context-manager suspension from source-exact Parameters."""

    builder = _validated_builder(forest) if builder is None else builder
    entries = _EntryActions(forest, builder) if entries is None else entries
    owners = tuple(
        sorted(
            action_id
            for action_id, node in builder.action_nodes.items()
            if isinstance(node, ast.With)
        )
    )
    return tuple(_with_region(forest, owner, entries) for owner in owners)


def _with_region(
    forest: ActionForest, owner: str, entries: _EntryActions
) -> WithRegion:
    """Name one protected body without duplicating context-item structure."""

    body = next(
        forest.parameter(parameter_id)
        for parameter_id in forest.action(owner).parameters
        if forest.parameter(parameter_id).name == "body"
    )
    return WithRegion(owner, _completion_stage(forest, body, entries))


def _with_control_relations(region: WithRegion) -> set[OwnedEndpointRelation]:
    """Thread entry forward and body completion back to the suspended owner."""

    entries = {(region.owner, region.owner, target) for target in region.body.entries}
    returns = {(region.owner, source, region.owner) for source in region.body.exits}
    return entries | returns


def _try_region(
    forest: ActionForest, owner: str, entries: _EntryActions
) -> CompletionRegion:
    """Name the ordered clauses owned by one try Action."""

    parameters = tuple(
        forest.parameter(parameter_id)
        for parameter_id in forest.action(owner).parameters
    )
    by_name = {parameter.name: parameter for parameter in parameters}
    handler_prefixes = tuple(
        parameter.name.removesuffix("_body")
        for parameter in parameters
        if parameter.name.startswith("handler") and parameter.name.endswith("_body")
    )
    handlers = tuple(
        ExceptionHandler(
            _optional_stage(forest, by_name.get(f"{prefix}_type"), entries),
            _completion_stage(forest, by_name[f"{prefix}_body"], entries),
        )
        for prefix in handler_prefixes
    )
    return CompletionRegion(
        owner,
        _completion_stage(forest, by_name["body"], entries),
        handlers,
        _optional_stage(forest, by_name.get("orelse"), entries),
        _optional_stage(forest, by_name.get("finalbody"), entries),
    )


def _optional_stage(
    forest: ActionForest,
    parameter: Parameter | None,
    entries: _EntryActions,
) -> CompletionStage | None:
    """Build an optional clause without an empty sentinel record."""

    return None if parameter is None else _completion_stage(forest, parameter, entries)


def _completion_stage(
    forest: ActionForest, parameter: Parameter, entries: _EntryActions
) -> CompletionStage:
    """Derive exact entry and terminal Actions for one real Parameter region."""

    actions = frozenset(_descendants(forest, parameter.actions))
    starts = entries.of(parameter.actions[0]) if parameter.actions else ()
    exits = tuple(
        sorted(
            {
                action_id
                for _completion, action_id in region_completion_actions(
                    forest, parameter.actions
                )
            }
        )
    )
    return CompletionStage(parameter.id, actions, starts, exits)


def _try_control_relations(
    forest: ActionForest,
    relations: tuple[tuple[str, str], ...],
    region: CompletionRegion,
) -> set[tuple[str, str, str]]:
    """Project one try into companions plus an O(clauses) control spine."""

    stages = _try_stages(region)
    region_actions = _try_region_actions(region, stages)
    result = {
        (region.owner, producer, consumer)
        for producer, consumer in relations
        if _try_companion_relation(
            producer, consumer, region.owner, region_actions, stages
        )
    }
    result.update(_try_entry_handoffs(forest, relations, region))
    result.update(_try_body_handoffs(forest, relations, region))
    result.update(_try_handler_handoffs(forest, relations, region))
    return result


def _try_companion_relation(
    producer: str,
    consumer: str,
    owner: str,
    actions: frozenset[str],
    stages: tuple[CompletionStage, ...],
) -> bool:
    """Keep real local continuity without turning data ingress into control."""

    return (
        _same_stage(producer, consumer, stages)
        or consumer == owner
        or producer in actions
        and consumer not in actions
    )


def _try_entry_handoffs(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
) -> set[OwnedEndpointRelation]:
    """Enter the protected body from its owner, never from a data producer."""

    return {
        (region.owner, region.owner, target)
        for target in completion_stage_admission_roots(
            forest, relations, region, region.body
        )
    }


def completion_stage_admission_roots(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
    stage: CompletionStage,
) -> tuple[str, ...]:
    """Return every post-discharge physical root in one try clause."""

    return _suite_admission_roots(forest, region.owner, relations, stage.actions)


def _try_physical_route_bases(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
    regions: tuple[CompletionRegion, ...],
    entries: _EntryActions,
) -> set[OwnedEndpointRelation]:
    """Expose every ordinary completion hop before physical placement."""

    stages = _try_stages(region)
    exits = tuple(
        completion_resumption_exits(forest, region, stage, regions) for stage in stages
    )
    result = {
        (region.owner, source, target)
        for index, stage_exits in enumerate(exits[:-1])
        for source in stage_exits
        for target in completion_stage_admission_roots(
            forest, relations, region, stages[index + 1]
        )
    }
    continuations = _try_continuation_targets(forest, relations, region, entries)
    result.update(
        (region.owner, source, target)
        for source in exits[-1]
        for target in continuations
    )
    result.update(_try_nested_route_bases(forest, relations, region, regions))
    return result


def _try_continuation_targets(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
    entries: _EntryActions,
) -> tuple[str, ...]:
    """Project every external continuation through the try's final frontier."""

    actions = _try_region_actions(region, _try_stages(region))
    external = {
        consumer
        for producer, consumer in relations
        if producer in actions and consumer not in actions
    }
    external.update(_following_parameter_entries(forest, region.owner, entries))
    return tuple(sorted(external))


def _following_parameter_entries(
    forest: ActionForest, action_id: str, entries: _EntryActions
) -> tuple[str, ...]:
    """Return the first lexical sibling after one completed structured Action."""

    parameter_id = forest.parent_parameter(action_id)
    siblings = (
        forest.roots if parameter_id is None else forest.parameter(parameter_id).actions
    )
    following = siblings[siblings.index(action_id) + 1 :]
    return () if not following else entries.of(following[0])


def _try_nested_route_bases(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
    regions: tuple[CompletionRegion, ...],
) -> set[OwnedEndpointRelation]:
    """Expose exact final-frontier hops out of nested completions in one stage."""

    result: set[OwnedEndpointRelation] = set()
    for stage in _try_stages(region):
        nested = tuple(
            sorted(
                (
                    (candidate, _try_region_actions(candidate, _try_stages(candidate)))
                    for candidate in regions
                    if candidate != region and candidate.owner in stage.actions
                ),
                key=lambda row: (len(row[1]), row[0].owner),
            )
        )
        for producer, consumer in relations:
            candidates = tuple(
                candidate
                for candidate, members in nested
                if producer in members and consumer not in members
            )
            if not candidates or consumer not in stage.actions:
                continue
            candidate = candidates[0]
            final = _try_stages(candidate)[-1]
            result.update(
                (region.owner, source, consumer)
                for source in completion_resumption_exits(
                    forest, candidate, final, regions
                )
            )
    return result


def _try_replaced_relations(
    relations: tuple[EndpointRelation, ...], region: CompletionRegion
) -> set[EndpointRelation]:
    """Identify redundant routes inside one try region."""

    stages = _try_stages(region)
    actions = _try_region_actions(region, stages)
    return {
        relation
        for relation in relations
        if set(relation) <= actions and not _same_stage(*relation, stages)
    }


def _try_region_actions(
    region: CompletionRegion, stages: tuple[CompletionStage, ...]
) -> frozenset[str]:
    """Return one owner's complete source Action inventory."""

    return frozenset(
        {region.owner, *(action for stage in stages for action in stage.actions)}
    )


def _try_stages(region: CompletionRegion) -> tuple[CompletionStage, ...]:
    """Flatten one named try region without losing handler order."""

    handlers = tuple(
        stage
        for handler in region.handlers
        for stage in (handler.selector, handler.body)
        if stage is not None
    )
    suffix = tuple(
        stage for stage in (region.orelse, region.finalbody) if stage is not None
    )
    return region.body, *handlers, *suffix


def _same_stage(
    producer: str, consumer: str, stages: tuple[CompletionStage, ...]
) -> bool:
    """Recognize one dependency whose endpoints share a real clause."""

    return any(
        producer in stage.actions and consumer in stage.actions for stage in stages
    )


def _try_body_handoffs(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
) -> set[tuple[str, str, str]]:
    """Connect body terminals to handler search, else, and finalization."""

    first_handler = _handler_admission_roots(
        forest, relations, region, region.handlers[:1]
    )
    orelse = _optional_stage_roots(forest, relations, region, region.orelse)
    finalbody = _optional_stage_roots(forest, relations, region, region.finalbody)
    targets = frozenset((*first_handler, *orelse, *finalbody))
    return {
        (region.owner, source, target)
        for source in region.body.exits
        for target in targets
    }


def _try_handler_handoffs(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
) -> set[tuple[str, str, str]]:
    """Chain nonmatches and send every matched outcome around else."""

    finalbody = _optional_stage_roots(forest, relations, region, region.finalbody)
    result: set[tuple[str, str, str]] = set()
    for index, handler in enumerate(region.handlers):
        following = _handler_admission_roots(
            forest, relations, region, region.handlers[index + 1 : index + 2]
        )
        selector_targets = frozenset(
            (
                *completion_stage_admission_roots(
                    forest, relations, region, handler.body
                ),
                *following,
                *finalbody,
            )
        )
        selector_exits = () if handler.selector is None else handler.selector.exits
        result.update(
            (region.owner, source, target)
            for source in selector_exits
            for target in selector_targets
        )
        result.update(
            (region.owner, source, target)
            for source in handler.body.exits
            for target in finalbody
        )
    if region.orelse is not None:
        result.update(
            (region.owner, source, target)
            for source in region.orelse.exits
            for target in finalbody
        )
    return result


def _optional_stage_roots(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
    stage: CompletionStage | None,
) -> tuple[str, ...]:
    """Return physical roots for one optional real clause."""

    return (
        ()
        if stage is None
        else completion_stage_admission_roots(forest, relations, region, stage)
    )


def _handler_admission_roots(
    forest: ActionForest,
    relations: tuple[EndpointRelation, ...],
    region: CompletionRegion,
    handlers: tuple[ExceptionHandler, ...],
) -> tuple[str, ...]:
    """Return selector roots, or body roots for one bare handler."""

    return tuple(
        root
        for handler in handlers
        for root in completion_stage_admission_roots(
            forest,
            relations,
            region,
            handler.body if handler.selector is None else handler.selector,
        )
    )


def _handler_entries(handlers: tuple[ExceptionHandler, ...]) -> tuple[str, ...]:
    """Return the selector entry, or the body entry for one bare handler."""

    return tuple(
        entry
        for handler in handlers
        for entry in (
            handler.body.entries
            if handler.selector is None
            else handler.selector.entries
        )
    )


def choice_passthrough_entries(
    forest: ActionForest,
    builder: _Builder | None = None,
) -> tuple[tuple[str, str, str], ...]:
    """Return completion-to-continuation entries for split choices.

    These rows are physical source-suppression routes, not dependence facts.
    The control protocol owns the target: it is the first Action evaluated by
    the source statement following the nearest enclosing ``if`` or ``match``.
    """

    builder = _validated_builder(forest) if builder is None else builder
    entries = _EntryActions(forest, builder)
    choices = frozenset(
        action_id
        for action_id, node in builder.action_nodes.items()
        if isinstance(node, (ast.If, ast.Match))
    )
    completions = (
        action_id
        for action_id, node in builder.action_nodes.items()
        if isinstance(node, (ast.Break, ast.Continue, ast.Raise, ast.Return))
    )
    rows = (
        (owner, completion, target)
        for completion in completions
        if (owner := _nearest_owner(forest, completion, choices)) is not None
        for successor in _following_siblings(forest, owner, choices)[:1]
        for target in entries.of(successor)
    )
    return tuple(sorted(set(rows)))


def choice_regions(
    forest: ActionForest, builder: _Builder | None = None
) -> tuple[ChoiceRegion, ...]:
    """Derive exact `if`/`match` inventories through the path authority."""

    builder = _validated_builder(forest) if builder is None else builder
    owners = tuple(
        sorted(
            action_id
            for action_id, node in builder.action_nodes.items()
            if isinstance(node, (ast.If, ast.Match))
        )
    )
    return tuple(
        ChoiceRegion(
            owner,
            frozenset(_descendants(forest, (owner,))),
            tuple(
                sorted(
                    {
                        action_id
                        for _kind, action_id in region_completion_actions(
                            forest, (owner,)
                        )
                    }
                )
            ),
        )
        for owner in owners
    )


def _nearest_owner(
    forest: ActionForest, action_id: str, owners: frozenset[str]
) -> str | None:
    """Find the nearest structurally enclosing owner from one fixed family."""

    current = forest.parent_action(action_id)
    while current is not None and current not in owners:
        current = forest.parent_action(current)
    return current


def _following_siblings(
    forest: ActionForest, action_id: str, choices: frozenset[str]
) -> tuple[str, ...]:
    """Find the next source Action through only enclosing choice continuations."""

    while True:
        parameter_id = forest.parent_parameter(action_id)
        siblings = (
            forest.roots
            if parameter_id is None
            else forest.parameter(parameter_id).actions
        )
        following = siblings[siblings.index(action_id) + 1 :]
        if following:
            return following
        action_id = forest.parent_action(action_id)
        if action_id not in choices:
            return ()


class _EntryActions:
    """Memoized first-evaluation query derived from the control protocol."""

    def __init__(self, forest: ActionForest, builder: _Builder) -> None:
        self.forest = forest
        self.nodes = builder.action_nodes
        _demanded, self.dormant, self.indices = _metadata(builder)
        self.memo: dict[str, tuple[str, ...]] = {}

    def of(self, action_id: str) -> tuple[str, ...]:
        """Return the first executable Actions for one source Action."""

        if action_id not in self.memo:
            self.memo[action_id] = self._derive(action_id)
        return self.memo[action_id]

    def _derive(self, action_id: str) -> tuple[str, ...]:
        node = self.nodes[action_id]
        if isinstance(node, ast.Try):
            return (action_id,)
        parameters = tuple(
            sorted(
                (
                    self.forest.parameter(parameter_id)
                    for parameter_id in self.forest.action(action_id).parameters
                    if parameter_id not in self.dormant
                    and self.forest.parameter(parameter_id).role
                    is not ParameterRole.SUITE
                ),
                key=lambda parameter: self.indices[parameter.id],
            )
        )
        first = next((parameter for parameter in parameters if parameter.actions), None)
        if first is None:
            return (action_id,)
        return self.of(first.actions[0])


def demanded_parameter_entries(
    forest: ActionForest,
) -> tuple[tuple[str, str, str], ...]:
    """Return canonical owner, entry Action, and demanded Parameter ids."""

    builder = _validated_builder(forest)
    demanded, _dormant, _indices = _metadata(builder)
    entries = _EntryActions(forest, builder)
    parameters = (forest.parameter(parameter_id) for parameter_id in demanded)
    return tuple(
        sorted(
            (parameter.owner, entry, parameter.id)
            for parameter in parameters
            if parameter.role is not ParameterRole.SUITE
            for action_id in parameter.actions
            for entry in entries.of(action_id)
        )
    )


def iteration_region_owners(forest: ActionForest) -> tuple[str, ...]:
    """Return canonical loop owners from the path authority."""

    return tuple(region.owner for region in iteration_protocol_index(forest).regions)


def iteration_protocol_index(forest: ActionForest) -> IterationProtocolIndex:
    """Build loop regions and nested membership once per immutable forest."""

    builder = _validated_builder(forest)
    entries = _EntryActions(forest, builder)
    regions = tuple(
        _iteration_region(forest, owner, entries)
        for owner in sorted(
            action_id
            for action_id, node in builder.action_nodes.items()
            if isinstance(node, (ast.For, ast.While))
        )
    )
    members = {
        region.owner: frozenset(_descendants(forest, (region.owner,)))
        for region in regions
    }
    order = {owner: (-len(actions), owner) for owner, actions in members.items()}
    owners = frozenset(members)
    return IterationProtocolIndex(
        regions,
        MappingProxyType(members),
        _iteration_owner_index(
            tuple((region.owner, region.body.actions) for region in regions), order
        ),
        MappingProxyType(
            {
                action.id: owner
                for action in forest.actions
                for owner in (_nearest_suite_owner(forest, action.id),)
                if owner in owners
            }
        ),
    )


def _iteration_owner_index(
    rows: tuple[tuple[str, frozenset[str]], ...],
    order: Mapping[str, tuple[int, str]],
) -> Mapping[str, tuple[str, ...]]:
    """Invert owner membership in deterministic outer-to-inner order."""

    mutable: dict[str, list[str]] = {}
    for owner, actions in rows:
        for action_id in actions:
            mutable.setdefault(action_id, []).append(owner)
    return MappingProxyType(
        {
            action_id: tuple(sorted(owners, key=order.__getitem__))
            for action_id, owners in mutable.items()
        }
    )


def iteration_regions(
    forest: ActionForest,
    builder: _Builder | None = None,
    entries: _EntryActions | None = None,
) -> tuple[IterationRegion, ...]:
    """Build every loop SUITE protocol once from source-exact Parameters."""

    if builder is None and entries is None:
        return iteration_protocol_index(forest).regions
    builder = _validated_builder(forest) if builder is None else builder
    entries = _EntryActions(forest, builder) if entries is None else entries
    owners = tuple(
        sorted(
            action_id
            for action_id, node in builder.action_nodes.items()
            if isinstance(node, (ast.For, ast.While))
        )
    )
    return tuple(_iteration_region(forest, owner, entries) for owner in owners)


def _iteration_region(
    forest: ActionForest, owner: str, entries: _EntryActions
) -> IterationRegion:
    """Name repeated and exhaustion-only stages without encoding an order."""

    parameters = tuple(
        forest.parameter(parameter_id)
        for parameter_id in forest.action(owner).parameters
    )
    by_name = {parameter.name: parameter for parameter in parameters}
    return IterationRegion(
        owner,
        _completion_stage(forest, by_name["body"], entries),
        _optional_stage(forest, by_name.get("orelse"), entries),
    )


def iteration_relations(
    forest: ActionForest,
    relations: tuple[tuple[str, str], ...],
) -> tuple[tuple[tuple[str, str, str], ...], tuple[tuple[str, str, str], ...]]:
    """Partition exact endpoint relations into loop-body entry and return."""

    canonical = tuple(sorted(set(relations)))
    if relations != canonical:
        raise ValueError("iteration endpoint relations must be canonical")
    index = iteration_protocol_index(forest)
    by_owner = {region.owner: region for region in index.regions}
    entries: set[OwnedEndpointRelation] = set()
    returns: set[OwnedEndpointRelation] = set()
    for producer, consumer in relations:
        entries.update(
            (owner, producer, consumer)
            for owner in index.body_owners.get(consumer, ())
            for region in (by_owner[owner],)
            if producer in index.members[owner] and producer not in region.body.actions
        )
        returns.update(
            (owner, producer, consumer)
            for owner in index.body_owners.get(producer, ())
            for region in (by_owner[owner],)
            if consumer in index.members[owner] and consumer not in region.body.actions
        )
    return tuple(sorted(entries)), tuple(sorted(returns))


def iteration_admission_relations(
    forest: ActionForest,
    physical_relations: tuple[EndpointRelation, ...],
    structural_entries: tuple[OwnedEndpointRelation, ...],
) -> tuple[OwnedEndpointRelation, ...]:
    """Admit physical SUITE roots not already entered structurally."""

    if physical_relations != tuple(sorted(set(physical_relations))):
        raise ValueError("physical iteration relations must be canonical")
    index = iteration_protocol_index(forest)
    entered = frozenset(
        (owner, consumer) for owner, _producer, consumer in structural_entries
    )
    internal = frozenset(
        (owner, consumer)
        for producer, consumer in physical_relations
        for owner in (index.admission_owner.get(producer),)
        if owner is not None and index.admission_owner.get(consumer) == owner
    )
    return tuple(
        sorted(
            (owner, owner, target)
            for target, owner in index.admission_owner.items()
            if (owner, target) not in internal and (owner, target) not in entered
        )
    )


def iteration_feedback_relations(
    forest: ActionForest, relations: tuple[tuple[str, str], ...]
) -> tuple[tuple[str, str, str], ...]:
    """Return exact endpoint relations satisfiable only after loop re-entry."""

    if relations != tuple(sorted(set(relations))):
        raise ValueError("iteration endpoint relations must be canonical")
    by_consumer: dict[str, list[tuple[str, str]]] = {}
    by_producer: dict[str, list[tuple[str, str]]] = {}
    for relation in relations:
        by_producer.setdefault(relation[0], []).append(relation)
        by_consumer.setdefault(relation[1], []).append(relation)

    def events(action: Action) -> tuple[Event, ...]:
        uses = tuple(("relation", *row) for row in by_consumer.get(action.id, ()))
        defines = tuple(("relation", *row) for row in by_producer.get(action.id, ()))
        return ((SELF, uses, defines, ()),)

    walk = _Walk(compile_controlwalk(forest), events)
    walk.run()
    repeated = _repeated_decisions(forest)
    exhaustion_only = frozenset(
        action_id
        for region in iteration_regions(forest)
        if region.orelse is not None
        for action_id in region.orelse.actions
    )
    return tuple(
        sorted(
            (owner, producer, consumer)
            for owner, producer, consumer, identity in walk.iteration_feedback
            if identity == ("relation", producer, consumer)
            and consumer not in exhaustion_only
            and not _is_repeated_decision_spine((producer, consumer), repeated)
        )
    )


def _repeated_decisions(forest: ActionForest) -> RepeatedDecisionIndex:
    """Index loop Parameters reevaluated by their persistent owner."""

    return {
        region.owner: frozenset(_descendants(forest, parameter.actions))
        for region in iteration_regions(forest)
        for parameter in (
            next(
                (
                    forest.parameter(parameter_id)
                    for parameter_id in forest.action(region.owner).parameters
                    if forest.parameter(parameter_id).name == "condition"
                ),
                None,
            ),
        )
        if parameter is not None
    }


def _is_repeated_decision_spine(
    relation: EndpointRelation,
    repeated: RepeatedDecisionIndex,
) -> bool:
    """Keep every nested owner/decision spine in its ordinary lazy SCC."""

    producer, consumer = relation
    return any(
        (producer == owner and consumer in actions)
        or (consumer == owner and producer in actions)
        for owner, actions in repeated.items()
    )


def _descendants(forest: ActionForest, roots: tuple[str, ...]) -> set[str]:
    result: set[str] = set()
    pending = list(roots)
    while pending:
        action_id = pending.pop()
        if action_id in result:
            continue
        result.add(action_id)
        pending.extend(
            child
            for parameter_id in forest.action(action_id).parameters
            for child in forest.parameter(parameter_id).actions
        )
    return result


def _iteration_body(forest: ActionForest, owner: str) -> frozenset[str]:
    suites = tuple(
        forest.parameter(parameter_id)
        for parameter_id in forest.action(owner).parameters
        if forest.parameter(parameter_id).role is ParameterRole.SUITE
        and forest.parameter(parameter_id).name == "body"
    )
    if len(suites) != 1:
        raise ForestBuildError(f"loop Action {owner!r} lacks one body SUITE")
    return frozenset(_descendants(forest, suites[0].actions))


class _Walk:
    def __init__(
        self,
        program: ControlWalkProgram,
        events: EventExtractor,
    ) -> None:
        self.forest = program.forest
        self.dormant = program.dormant
        self.indices = program.indices
        self.nodes = program._nodes
        self.events: dict[tuple[str, Anchor], tuple[tuple[Identity, ...], ...]] = {}
        self.definitions: set[tuple[str, Identity]] = set()
        self.uses: set[tuple[str, Identity]] = set()
        self.reaching: set[tuple[str, str, Identity]] = set()
        self.unbound_uses: set[Identity] = set()
        self.unbound_occurrences: set[UnresolvedUse] = set()
        self.iteration_feedback: set[tuple[str, str, str, Identity]] = set()
        self._feedback_context: (
            tuple[str, frozenset[tuple[str, str, Identity]]] | None
        ) = None
        self._extractor = events
        self._extracted: _ExtractedActionIds = set()

    def run(self) -> Payload:
        """Walk the complete module region from an empty entry state."""

        return self.run_region(self.forest.roots, {})

    def run_region(self, roots: tuple[str, ...], incoming: EntryState) -> Payload:
        """Run the collected protocol from an explicit lexical entry state."""

        return self.run_region_result(roots, incoming).payload

    def run_region_result(
        self, roots: tuple[str, ...], incoming: EntryState
    ) -> ControlWalkResult:
        """Run once and preserve private path evidence beside the codec."""

        self._collect_region_inventory(roots)
        seeded = {
            identity: _DefinitionState(producers, False)
            for identity, producers in incoming.items()
        }
        self._sequence(roots, seeded)
        return ControlWalkResult(
            (
                (
                    "definitions",
                    tuple(sorted(self.definitions, key=_identity_row_key)),
                ),
                ("uses", tuple(sorted(self.uses, key=_identity_row_key))),
                ("reaching", tuple(sorted(self.reaching, key=_identity_row_key))),
                (
                    "declared_inputs",
                    tuple(sorted(self.unbound_uses, key=_identity_key)),
                ),
            ),
            tuple(sorted(self.unbound_occurrences, key=_identity_row_key)),
        )

    def _collect_region_inventory(self, roots: tuple[str, ...]) -> None:
        """Index lexical events once without pretending every row executes.

        Definitions and uses describe the source inventory of the queried
        region.  Reaching and unbound evidence are populated separately by
        ``_sequence`` over legal control paths.  The structural traversal
        therefore visits every nested Parameter except a deferred code body;
        it neither orders Actions nor changes flow state.
        """

        pending = list(reversed(roots))
        while pending:
            action_id = pending.pop()
            action = self.forest.action(action_id)
            self._collect_action(action)
            children = tuple(
                child
                for parameter_id in action.parameters
                if parameter_id not in self.dormant
                for child in self.forest.parameter(parameter_id).actions
            )
            pending.extend(reversed(children))

    def _collect_action(self, action: Action) -> None:
        """Extract one Action exactly when the control protocol reaches it."""

        if action.id in self._extracted:
            return
        self._extracted.add(action.id)
        rows = self._extractor(action)
        if not isinstance(rows, tuple):
            raise TypeError("event extractor must return a tuple")
        for row in rows:
            self._collect_row(action, row)

    def _collect_row(self, action: Action, row: object) -> None:
        if not isinstance(row, tuple) or len(row) != 4:
            raise TypeError("each event must be an anchored four-tuple")
        anchor, uses, defines, kills = row
        self._validate_anchor(action, anchor)
        if not all(isinstance(items, tuple) for items in (uses, defines, kills)):
            raise TypeError("event uses, defines, and kills must be tuples")
        key = (action.id, anchor)
        current = self.events.get(key)
        # An anchor usually receives one row, so the merge path exists for the
        # rare repeat rather than being paid on every event. Set construction
        # below is also what validates that every identity is hashable.
        if current is None:
            self.events[key] = (
                tuple(sorted(set(uses), key=_identity_key)),
                tuple(sorted(set(defines), key=_identity_key)),
                tuple(sorted(set(kills), key=_identity_key)),
            )
        else:
            self.events[key] = tuple(
                tuple(sorted(set(before) | set(after), key=_identity_key))
                for before, after in zip(current, (uses, defines, kills), strict=True)
            )
        self.uses.update((action.id, identity) for identity in uses)
        self.definitions.update((action.id, identity) for identity in defines)

    def _validate_anchor(self, action: Action, anchor: Anchor) -> None:
        if anchor == SELF:
            return
        valid = (
            isinstance(anchor, tuple)
            and len(anchor) == 2
            and anchor[0] in action.parameters
            and anchor[1] in {ENTER, EXIT}
        )
        if not valid:
            raise InvalidAnchorError(
                f"invalid anchor {anchor!r} for Action {action.id}"
            )

    def _sequence(self, action_ids: tuple[str, ...], incoming: State) -> Outcomes:
        outcomes: Outcomes = {"normal": dict(incoming)}
        for action_id in action_ids:
            normal = outcomes.pop("normal", None)
            if normal is None:
                break
            outcomes = _union_outcomes((outcomes, self._action(action_id, normal)))
        return outcomes

    def _action(self, action_id: str, incoming: State) -> Outcomes:
        action = self.forest.action(action_id)
        self._collect_action(action)
        node = self.nodes[action_id]
        if isinstance(action, Expression):
            return self._expression(action_id, node, incoming)
        method_name, consumes_node = _ACTION_HANDLERS.get(
            type(node), ("_simple_command", True)
        )
        handler = getattr(self, method_name)
        if consumes_node:
            return handler(action_id, node, incoming)
        return handler(action_id, incoming)

    def _expression(self, action_id: str, node: ast.AST, incoming: State) -> Outcomes:
        if isinstance(node, ast.IfExp):
            after_test = self._named_parameter(action_id, "condition", incoming)
            branches = tuple(
                self._named_parameter(action_id, name, after_test)
                for name in ("then", "otherwise")
            )
            result = _union_states(branches)
        elif isinstance(node, ast.BoolOp):
            result = self._lazy_parameters(action_id, incoming)
        else:
            excluded = (
                frozenset({"body"}) if isinstance(node, ast.Lambda) else frozenset()
            )
            result = self._staged_parameters(action_id, incoming, excluded)
        return {"normal": self._apply(action_id, SELF, result)}

    def _simple_command(
        self, action_id: str, node: ast.AST, incoming: State
    ) -> Outcomes:
        result = self._staged_parameters(action_id, incoming, frozenset())
        result = self._apply(action_id, SELF, result)
        if isinstance(node, ast.Break):
            return {"break": result}
        if isinstance(node, ast.Continue):
            return {"continue": result}
        if isinstance(node, ast.Raise):
            return {"raise": result}
        if isinstance(node, ast.Return):
            return {"return": result}
        return {"normal": result}

    def _if(self, action_id: str, incoming: State) -> Outcomes:
        after_condition = self._named_parameter(action_id, "condition", incoming)
        admitted = self._apply(action_id, SELF, after_condition)
        body = self._suite(action_id, "body", admitted)
        orelse_parameters = self._parameters(action_id, "orelse")
        orelse = (
            self._parameter(orelse_parameters[0], admitted)
            if orelse_parameters
            else {"normal": dict(admitted)}
        )
        return _union_outcomes((body, orelse))

    def _match(self, action_id: str, incoming: State) -> Outcomes:
        subject = self._named_parameter(action_id, "subject", incoming)
        admitted = self._apply(action_id, SELF, subject)
        alternatives: list[Outcomes] = [{"normal": dict(admitted)}]
        for prefix in self._case_prefixes(action_id):
            selected = self._named_parameter(action_id, f"{prefix}_pattern", admitted)
            selected = self._named_parameter(action_id, f"{prefix}_guard", selected)
            alternatives.append(self._suite(action_id, f"{prefix}_body", selected))
        return _union_outcomes(tuple(alternatives))

    def _while(self, action_id: str, incoming: State) -> Outcomes:
        head = dict(incoming)
        baseline: frozenset[tuple[str, str, Identity]] | None = None
        outer_context = self._feedback_context
        while True:
            self._feedback_context = (
                (action_id, baseline) if baseline is not None else outer_context
            )
            condition = self._named_parameter(action_id, "condition", head)
            body = self._suite(action_id, "body", condition)
            self._feedback_context = outer_context
            baseline = frozenset(self.reaching) if baseline is None else baseline
            back = tuple(body[kind] for kind in ("normal", "continue") if kind in body)
            candidate = _union_states((incoming, *back))
            if candidate == head:
                break
            head = candidate
        condition = self._named_parameter(action_id, "condition", head)
        body = self._suite(action_id, "body", condition)
        orelse = self._suite(action_id, "orelse", condition)
        results: list[Outcomes] = [orelse]
        if "break" in body:
            results.append({"normal": body["break"]})
        results.append(
            {kind: state for kind, state in body.items() if kind in {"raise", "return"}}
        )
        return self._finish(action_id, _union_outcomes(tuple(results)), retain=True)

    def _for(self, action_id: str, incoming: State) -> Outcomes:
        before = self._named_parameter(action_id, "iterable", incoming)
        target = self._parameters(action_id, "target")[0]
        head = dict(before)
        baseline: frozenset[tuple[str, str, Identity]] | None = None
        outer_context = self._feedback_context
        while True:
            self._feedback_context = (
                (action_id, baseline) if baseline is not None else outer_context
            )
            iteration = self._normal(self._parameter(target, head))
            body = self._suite(action_id, "body", iteration)
            self._feedback_context = outer_context
            baseline = frozenset(self.reaching) if baseline is None else baseline
            back = tuple(body[kind] for kind in ("normal", "continue") if kind in body)
            candidate = _union_states((before, *back))
            if candidate == head:
                break
            head = candidate
        iteration = self._normal(self._parameter(target, head))
        body = self._suite(action_id, "body", iteration)
        orelse = self._suite(action_id, "orelse", head)
        results: list[Outcomes] = [orelse]
        if "break" in body:
            results.append({"normal": body["break"]})
        results.append(
            {kind: state for kind, state in body.items() if kind in {"raise", "return"}}
        )
        return self._finish(action_id, _union_outcomes(tuple(results)), retain=True)

    def _try(self, action_id: str, incoming: State) -> Outcomes:
        admitted = self._apply(action_id, SELF, incoming)
        body = self._suite(action_id, "body", admitted)
        exception_seed = _union_states((admitted, *body.values()))
        handlers = tuple(
            self._handler(action_id, prefix, exception_seed)
            for prefix in self._handler_prefixes(action_id)
        )
        normal_body = body.get("normal")
        orelse = (
            self._suite(action_id, "orelse", normal_body)
            if normal_body is not None
            else {}
        )
        propagated = {kind: state for kind, state in body.items() if kind != "normal"}
        combined = _union_outcomes((orelse, propagated, *handlers))
        finalbody = self._parameters(action_id, "finalbody")
        if finalbody:
            combined = self._apply_finalbody(combined, finalbody[0])
        return combined

    def _handler(self, action_id: str, prefix: str, incoming: State) -> Outcomes:
        selected = self._named_parameter(action_id, f"{prefix}_type", incoming)
        targets = self._parameters(action_id, f"{prefix}_target")
        target = targets[0] if targets else None
        if target is not None:
            selected = self._normal(self._parameter(target, selected, defer_exit=True))
        result = self._suite(action_id, f"{prefix}_body", selected)
        if target is None:
            return result
        return {
            kind: self._apply(action_id, (target.id, EXIT), state)
            for kind, state in result.items()
        }

    def _with(self, action_id: str, incoming: State) -> Outcomes:
        result = dict(incoming)
        parameters = self._ordered_parameters(action_id)
        body: Parameter | None = None
        for parameter in parameters:
            if parameter.role is ParameterRole.SUITE:
                body = parameter
                continue
            result = self._normal(self._parameter(parameter, result))
        result = self._apply(action_id, SELF, result)
        outcomes = (
            self._parameter(body, result) if body is not None else {"normal": result}
        )
        return outcomes

    def _definition(self, action_id: str, node: ast.AST, incoming: State) -> Outcomes:
        result = dict(incoming)
        for parameter in self._ordered_parameters(action_id):
            if parameter.id in self.dormant:
                continue
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and parameter.role is ParameterRole.SUITE
            ):
                continue
            result = self._normal(self._parameter(parameter, result))
        return {"normal": self._apply(action_id, SELF, result)}

    def _staged_parameters(
        self, action_id: str, incoming: State, excluded: frozenset[str]
    ) -> State:
        result = dict(incoming)
        for parameter in self._ordered_parameters(action_id):
            if parameter.id in self.dormant or parameter.name in excluded:
                continue
            if parameter.role is ParameterRole.SUITE:
                continue
            result = self._normal(self._parameter(parameter, result))
        return result

    def _lazy_parameters(self, action_id: str, incoming: State) -> State:
        result = dict(incoming)
        exits: list[State] = []
        for parameter in self._ordered_parameters(action_id):
            if parameter.role is not ParameterRole.VALUE:
                continue
            result = self._normal(self._parameter(parameter, result))
            exits.append(result)
        return _union_states(tuple(exits)) if exits else result

    def _named_parameter(self, action_id: str, name: str, incoming: State) -> State:
        parameters = self._parameters(action_id, name)
        return (
            self._normal(self._parameter(parameters[0], incoming))
            if parameters
            else dict(incoming)
        )

    def _suite(self, action_id: str, name: str, incoming: State) -> Outcomes:
        parameters = self._parameters(action_id, name)
        return (
            self._parameter(parameters[0], incoming)
            if parameters
            else {"normal": dict(incoming)}
        )

    def _parameter(
        self, parameter: Parameter, incoming: State, defer_exit: bool = False
    ) -> Outcomes:
        entered = self._apply(parameter.owner, (parameter.id, ENTER), incoming)
        outcomes = self._sequence(parameter.actions, entered)
        if defer_exit:
            return outcomes
        return {
            kind: self._apply(parameter.owner, (parameter.id, EXIT), state)
            for kind, state in outcomes.items()
        }

    def _apply(
        self, action_id: str, anchor: Anchor, incoming: State, retain: bool = False
    ) -> State:
        uses, defines, kills = self.events.get((action_id, anchor), ((), (), ()))
        for identity in uses:
            self._record_reaching(action_id, identity, incoming)
        result = dict(incoming)
        for identity in kills:
            result.pop(identity, None)
        for identity in defines:
            previous = result.get(identity) if retain else None
            producers = frozenset() if previous is None else previous.producers
            result[identity] = _DefinitionState(
                producers | frozenset({action_id}), False
            )
        return result

    def _record_reaching(
        self, action_id: str, identity: Identity, incoming: State
    ) -> None:
        state = incoming.get(identity)
        if state is None or state.may_be_unbound:
            self.unbound_uses.add(identity)
            self.unbound_occurrences.add((action_id, identity))
        for producer in () if state is None else state.producers:
            if producer != action_id:
                row = producer, action_id, identity
                self.reaching.add(row)
                self._record_iteration_feedback(row)

    def _record_iteration_feedback(self, row: tuple[str, str, Identity]) -> None:
        if self._feedback_context is None:
            return
        owner, baseline = self._feedback_context
        if row not in baseline:
            self.iteration_feedback.add((owner, *row))

    def _finish(self, action_id: str, outcomes: Outcomes, retain: bool) -> Outcomes:
        return {
            kind: self._apply(action_id, SELF, state, retain)
            for kind, state in outcomes.items()
        }

    def _apply_finalbody(self, outcomes: Outcomes, parameter: Parameter) -> Outcomes:
        results: list[Outcomes] = []
        for original_kind, state in outcomes.items():
            final = self._parameter(parameter, state)
            normal = final.pop("normal", None)
            if normal is not None:
                results.append({original_kind: normal})
            results.append(final)
        return _union_outcomes(tuple(results))

    def _handler_prefixes(self, action_id: str) -> tuple[str, ...]:
        return tuple(
            parameter.name.removesuffix("_body")
            for parameter in self._ordered_parameters(action_id)
            if parameter.role is ParameterRole.SUITE
            and (
                parameter.name == "handler_body"
                or parameter.name.startswith("handler_")
                and parameter.name.endswith("_body")
            )
        )

    def _case_prefixes(self, action_id: str) -> tuple[str, ...]:
        return tuple(
            parameter.name.removesuffix("_body")
            for parameter in self._ordered_parameters(action_id)
            if parameter.role is ParameterRole.SUITE
            and parameter.name.startswith("case")
            and parameter.name.endswith("_body")
        )

    def _ordered_parameters(self, action_id: str) -> tuple[Parameter, ...]:
        action = self.forest.action(action_id)
        indexed = tuple(
            (index, self.forest.parameter(parameter_id))
            for index, parameter_id in enumerate(action.parameters)
        )
        return tuple(
            parameter
            for _, parameter in sorted(
                indexed, key=lambda item: (self.indices[item[1].id], item[0])
            )
        )

    def _parameters(self, action_id: str, name: str) -> tuple[Parameter, ...]:
        return tuple(
            parameter
            for parameter in self._ordered_parameters(action_id)
            if parameter.name == name
        )

    @staticmethod
    def _normal(outcomes: Outcomes) -> State:
        if set(outcomes) != {"normal"}:
            raise ForestBuildError("a parameter completed abruptly")
        return outcomes["normal"]


def _union_outcomes(outcomes: tuple[Outcomes, ...]) -> Outcomes:
    kinds = {kind for outcome in outcomes for kind in outcome}
    return {
        kind: _union_states(
            tuple(outcome[kind] for outcome in outcomes if kind in outcome)
        )
        for kind in kinds
    }


def _union_states(states: tuple[State, ...]) -> State:
    """Merge path states at a join, preserving every alternative producer.

    Producer sets are immutable, so a key carried by exactly one incoming state
    reuses that state's set instead of being rebuilt. Rebuilding every set
    element by element dominated analysis on real programs and made it
    superlinear in program size, which §13.19 forbids.
    """

    if len(states) == 1:
        return dict(states[0])
    merged: State = {}
    counts: _DefinitionCounts = {}
    for state in states:
        _merge_state_into(merged, counts, state)
    return _mark_missing_definitions(merged, counts, len(states))


def _merge_state_into(merged: State, counts: _DefinitionCounts, state: State) -> None:
    """Fold one path state into the accumulator, reusing its immutable sets."""

    for key, producers in state.items():
        existing = merged.get(key)
        counts[key] = counts.get(key, 0) + 1
        merged[key] = (
            producers
            if existing is None or existing is producers
            else _DefinitionState(
                existing.producers | producers.producers,
                existing.may_be_unbound or producers.may_be_unbound,
            )
        )


def _mark_missing_definitions(
    merged: State, counts: _DefinitionCounts, path_count: int
) -> State:
    """Mark identities absent from at least one legal incoming path."""

    return {
        identity: (
            row
            if counts[identity] == path_count or row.may_be_unbound
            else _DefinitionState(row.producers, True)
        )
        for identity, row in merged.items()
    }


@dataclass(frozen=True)
class _DemandRule:
    field: str
    first_index: int = 0

    def matches(self, field: str, item_index: int) -> bool:
        """Return whether one component is selected by this demand rule."""

        return self.field == field and item_index >= self.first_index


type _StageSelector = str | tuple[str, ...] | frozenset[str]


@dataclass(frozen=True)
class _EvaluationProtocol:
    stages: tuple[_StageSelector, ...] = ()
    demands: tuple[_DemandRule, ...] = ()
    orderer: Callable[[tuple[_Component, ...]], tuple[int, ...]] | None = None
    refiner: Callable[[_Component, tuple[_Component, ...]], bool] | None = None

    def evaluation_indices(self, components: tuple[_Component, ...]) -> tuple[int, ...]:
        """Return the protocol's deterministic stage index per component."""

        return (
            self.orderer(components)
            if self.orderer
            else _staged_indices(components, self.stages)
        )

    def is_demanded(
        self, component: _Component, components: tuple[_Component, ...]
    ) -> bool:
        """Return whether evaluating this component may require suspension."""

        base = any(
            rule.matches(component.origin_field, component.origin_index)
            for rule in self.demands
        )
        return self.refiner(component, components) if self.refiner is not None else base


def _protocol_metadata(
    forest: ActionForest,
) -> tuple[frozenset[str], frozenset[str], dict[str, int]]:
    return _metadata(_validated_builder(forest))


def _validated_builder(forest: ActionForest) -> _Builder:
    """Return the forest-owned source-exact structural index."""

    return forest._source_builder


def _metadata(
    builder: _Builder,
) -> tuple[frozenset[str], frozenset[str], dict[str, int]]:
    demanded: set[str] = set()
    dormant: set[str] = set()
    indices: dict[str, int] = {}
    for action in builder.actions:
        node = builder.action_nodes[action.id]
        components = builder.components_by_action[action.id]
        protocol = _PROTOCOLS.get(type(node), _DEFAULT_PROTOCOL)
        action_indices = protocol.evaluation_indices(components)
        for parameter_id, component, index in zip(
            action.parameters, components, action_indices, strict=True
        ):
            indices[parameter_id] = index
            if protocol.is_demanded(component, components):
                demanded.add(parameter_id)
            if component.origin_field in _DORMANT_FIELDS.get(type(node), frozenset()):
                dormant.add(parameter_id)
    return frozenset(demanded), frozenset(dormant), indices


def _staged_indices(
    components: tuple[_Component, ...], plan: tuple[_StageSelector, ...]
) -> tuple[int, ...]:
    if not plan:
        return tuple(range(len(components)))
    remaining = list(range(len(components)))
    result: list[int | None] = [None] * len(components)
    stage = 0
    for item in plan:
        stage = _assign_stage(components, item, remaining, result, stage)
    for index in remaining:
        result[index] = stage
        stage += 1
    return tuple(int(item) for item in result)


def _assign_stage(
    components: tuple[_Component, ...],
    item: _StageSelector,
    remaining: list[int],
    result: list[int | None],
    stage: int,
) -> int:
    selectors = (item,) if isinstance(item, str) else item
    matches = [
        index
        for index in remaining
        if any(_matches(components[index].name, selector) for selector in selectors)
    ]
    if isinstance(item, frozenset):
        for index in matches:
            result[index] = stage
            remaining.remove(index)
        return stage + bool(matches)
    for index in matches:
        result[index] = stage
        stage += 1
        remaining.remove(index)
    return stage


def _comprehension_indices(components: tuple[_Component, ...]) -> tuple[int, ...]:
    prefixes = sorted(
        {prefix for item in components if (prefix := _generator_prefix(item.name))},
        key=lambda prefix: min(
            _component_position(item)
            for item in components
            if _generator_prefix(item.name) == prefix
        ),
    )
    order: list[int] = []
    for prefix in prefixes:
        members = [
            index
            for index, item in enumerate(components)
            if _generator_prefix(item.name) == prefix
        ]
        for suffix in ("_iter", "_target", "_ifs"):
            order.extend(
                index
                for index in members
                if suffix in components[index].name and index not in order
            )
    order.extend(index for index in range(len(components)) if index not in order)
    result = [0] * len(components)
    for stage, index in enumerate(order):
        result[index] = stage
    return tuple(result)


def _generator_prefix(name: str) -> str | None:
    for marker in ("_target", "_iter", "_ifs"):
        if marker in name and name.startswith("generator"):
            return name.split(marker, maxsplit=1)[0]
    return None


def _matches(name: str, selector: str) -> bool:
    return name == selector or name.startswith(selector + "_")


def _comprehension_demand(
    component: _Component, components: tuple[_Component, ...]
) -> bool:
    prefix = _generator_prefix(component.name)
    if prefix is None:
        return True
    prefixes = list(
        dict.fromkeys(
            item
            for item in (_generator_prefix(candidate.name) for candidate in components)
            if item is not None
        )
    )
    return not (prefixes and prefix == prefixes[0] and "_iter" in component.name)


_ACTION_HANDLERS: dict[type[ast.AST], tuple[str, bool]] = {
    ast.If: ("_if", False),
    ast.Match: ("_match", False),
    ast.While: ("_while", False),
    ast.For: ("_for", False),
    ast.Try: ("_try", False),
    ast.With: ("_with", False),
    ast.FunctionDef: ("_definition", True),
    ast.AsyncFunctionDef: ("_definition", True),
    ast.ClassDef: ("_definition", True),
}
_DORMANT_FIELDS: dict[type[ast.AST], frozenset[str]] = {
    ast.FunctionDef: frozenset({"body"}),
    ast.Lambda: frozenset({"body"}),
}
_DEFAULT_PROTOCOL = _EvaluationProtocol()
_COMPREHENSION_PROTOCOL = _EvaluationProtocol(
    demands=(_DemandRule("generators"),),
    orderer=_comprehension_indices,
    refiner=_comprehension_demand,
)
_PROTOCOLS: dict[type[ast.AST], _EvaluationProtocol] = {
    ast.FunctionDef: _EvaluationProtocol(
        stages=(
            "decorator",
            "signature_default",
            "signature_annotation",
            "return_annotation",
            "target",
            "body",
        )
    ),
    ast.ClassDef: _EvaluationProtocol(
        stages=("decorator", ("base", "keyword"), "body", "target")
    ),
    ast.If: _EvaluationProtocol(
        stages=("condition", frozenset({"body", "orelse"})),
        demands=(_DemandRule("body"), _DemandRule("orelse")),
    ),
    ast.While: _EvaluationProtocol(
        stages=("condition", "body", "orelse"),
        demands=(_DemandRule("test"), _DemandRule("body"), _DemandRule("orelse")),
    ),
    ast.For: _EvaluationProtocol(
        stages=("iterable", "target", "body", "orelse"),
        demands=(_DemandRule("body"), _DemandRule("orelse")),
    ),
    ast.Try: _EvaluationProtocol(
        stages=("body", frozenset({"handler", "orelse"}), "finalbody"),
        demands=(
            _DemandRule("body"),
            _DemandRule("handlers"),
            _DemandRule("orelse"),
            _DemandRule("finalbody"),
        ),
    ),
    ast.With: _EvaluationProtocol(
        stages=("item", "body"), demands=(_DemandRule("body"),)
    ),
    ast.Assign: _EvaluationProtocol(stages=("value", "target")),
    ast.AnnAssign: _EvaluationProtocol(stages=("value", "target", "annotation")),
    ast.NamedExpr: _EvaluationProtocol(stages=("value", "target")),
    ast.IfExp: _EvaluationProtocol(
        stages=("condition", frozenset({"then", "otherwise"})),
        demands=(_DemandRule("body"), _DemandRule("orelse")),
    ),
    ast.Assert: _EvaluationProtocol(demands=(_DemandRule("msg"),)),
    ast.BoolOp: _EvaluationProtocol(demands=(_DemandRule("values", 1),)),
    ast.Lambda: _EvaluationProtocol(demands=(_DemandRule("body"),)),
    ast.Compare: _EvaluationProtocol(demands=(_DemandRule("comparators", 1),)),
    ast.ListComp: _COMPREHENSION_PROTOCOL,
    ast.SetComp: _COMPREHENSION_PROTOCOL,
    ast.DictComp: _COMPREHENSION_PROTOCOL,
    ast.GeneratorExp: _COMPREHENSION_PROTOCOL,
}
