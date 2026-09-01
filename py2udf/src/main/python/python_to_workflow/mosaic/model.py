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

"""Named immutable records shared by analysis, checking, and realization."""

from __future__ import annotations

from collections.abc import Hashable
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from functools import total_ordering
from typing import Protocol, TypeVar

from python_to_workflow.mosaic.forest import ActionForest

ActionIdentity = TypeVar("ActionIdentity", bound=Hashable)
type LexicalScopeId = Hashable


@total_ordering
@dataclass(frozen=True)
class ContextualActionId:
    """One source Action in one static context and lexical scope."""

    source_action_id: str
    context_id: str
    scope_id: LexicalScopeId

    def __post_init__(self) -> None:
        if not self.source_action_id or not self.context_id:
            raise ValueError("contextual Action identity components must be nonempty")
        try:
            hash(self.scope_id)
        except TypeError as error:
            raise TypeError("lexical scope identity must be hashable") from error

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ContextualActionId):
            return NotImplemented
        return (
            self.source_action_id,
            self.context_id,
            repr(self.scope_id),
        ) < (
            other.source_action_id,
            other.context_id,
            repr(other.scope_id),
        )


@dataclass(frozen=True, eq=False)
class Carrier:
    """A semantic value with structural identity."""

    identity: Hashable
    _key: CarrierIdentityKey = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_key", carrier_identity_key(self.identity))

    def __hash__(self) -> int:
        return hash(self._key)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Carrier):
            return NotImplemented
        return self._key == other._key

    @property
    def identity_key(self) -> CarrierIdentityKey:
        """Return the canonical identity computed once at construction."""

        return self._key


@dataclass(frozen=True, order=True)
class CarrierIdentityKey:
    """Structural identity used for equality and ordering."""

    encoded: tuple[object, ...]


def carrier_identity_key(identity: Hashable, /) -> CarrierIdentityKey:
    """Return a deterministic key for supported immutable identity values."""

    try:
        hash(identity)
    except RecursionError as error:
        raise TypeError("Carrier identity contains a cyclic structure") from error
    except TypeError as error:
        raise TypeError("Carrier identity must be hashable") from error
    try:
        return CarrierIdentityKey(_identity_node(identity, frozenset()))
    except RecursionError as error:
        raise TypeError("Carrier identity contains a cyclic structure") from error


def _identity_node(value: object, active: frozenset[int]) -> tuple[object, ...]:
    """Encode typed immutable structure without consulting ``repr``."""

    _reject_identity_cycle(value, active)
    return _uncycled_identity_node(value, active | {id(value)})


def _reject_identity_cycle(value: object, active: frozenset[int]) -> None:
    """Fail closed when one structural identity revisits an active object."""

    if id(value) in active:
        raise TypeError("Carrier identity contains a cyclic structure")


def _uncycled_identity_node(
    value: object, active: frozenset[int]
) -> tuple[object, ...]:
    """Encode one value after the active-path cycle check."""

    enum = _enum_identity_node(value)
    if enum is not None:
        return enum
    primitive = _primitive_identity_node(value)
    if primitive is not None:
        return primitive
    collection = _collection_identity_node(value, active)
    if collection is not None:
        return collection
    record = _record_identity_node(value, active)
    if record is not None:
        return record
    raise TypeError(
        "Carrier identity must be an immutable primitive, tuple, frozenset, "
        "dataclass, Enum, or define __mosaic_identity_key__()"
    )


def _enum_identity_node(value: object) -> tuple[object, ...] | None:
    """Encode nominal enum identity before scalar subclass coercions."""

    if isinstance(value, Enum):
        return ("enum", _identity_type_name(value), value.name)
    return None


def _primitive_identity_node(value: object) -> tuple[object, ...] | None:
    """Encode one supported scalar identity or decline structural values."""

    if value is None:
        return ("none",)
    if isinstance(value, bool):
        return ("bool", value)
    numeric = _numeric_identity_node(value)
    if numeric is not None:
        return numeric
    return _text_identity_node(value)


def _numeric_identity_node(value: object) -> tuple[object, ...] | None:
    """Encode a non-boolean numeric identity with exact representation."""

    if isinstance(value, int):
        return ("int", str(value))
    if isinstance(value, float):
        return ("float", value.hex())
    return None


def _text_identity_node(value: object) -> tuple[object, ...] | None:
    """Encode textual and binary scalar identities."""

    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, bytes):
        return ("bytes", value.hex())
    return None


def _collection_identity_node(
    value: object, active: frozenset[int]
) -> tuple[object, ...] | None:
    """Encode supported ordered and unordered immutable collections."""

    if isinstance(value, tuple):
        return ("tuple", *(_identity_node(item, active) for item in value))
    if isinstance(value, frozenset):
        return ("frozenset", *_sorted_identity_nodes(value, active))
    return None


def _record_identity_node(
    value: object, active: frozenset[int]
) -> tuple[object, ...] | None:
    """Encode dataclass or explicit extension identities."""

    type_name = _identity_type_name(value)
    if is_dataclass(value) and not isinstance(value, type):
        return (
            "dataclass",
            type_name,
            *(
                (field.name, _identity_node(getattr(value, field.name), active))
                for field in fields(value)
            ),
        )
    custom = getattr(value, "__mosaic_identity_key__", None)
    if callable(custom):
        return ("custom", type_name, _identity_node(custom(), active))
    return None


def _identity_type_name(value: object) -> str:
    """Return the stable qualified type tag for one structural identity."""

    return f"{type(value).__module__}.{type(value).__qualname__}"


def _sorted_identity_nodes(
    values: frozenset[object], active: frozenset[int]
) -> tuple[tuple[object, ...], ...]:
    """Canonicalize an unordered identity collection without using repr."""

    return tuple(sorted(_identity_node(item, active) for item in values))


@dataclass(frozen=True, order=True)
class ModuleBinding:
    """One Python name in the source module's logical namespace."""

    name: str

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError("module binding name must be a Python identifier")


@dataclass(frozen=True, order=True)
class PythonActivationBinding:
    """A syntax-owned binding whose lifetime stays in one Python activation."""

    name: str
    binder_action_id: str

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError("activation binding name must be a Python identifier")
        if not self.binder_action_id:
            raise ValueError("activation binder Action id must be nonempty")


@dataclass(frozen=True, order=True)
class EffectState:
    """One ambient state domain whose continuity requires a realization."""

    domain: str

    def __post_init__(self) -> None:
        if not self.domain or not self.domain.replace("-", "_").isidentifier():
            raise ValueError("effect-state domain must be a nonempty identifier")


@dataclass(frozen=True, order=True)
class AmbientModuleState:
    """Ambient state owned by one exact imported module path."""

    module_path: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.module_path or any(
            not component.isidentifier() for component in self.module_path
        ):
            raise ValueError("ambient module path must contain Python identifiers")


@dataclass(frozen=True)
class ProcessAmbientState:
    """Unknown process state reachable through an unresolved call alternative."""


@dataclass(frozen=True)
class WorldOrder:
    """Conservative sequencing token removed only by stronger semantics."""


@dataclass(frozen=True)
class ReachingPair[ActionIdentity: Hashable]:
    """Exact dependence from one producing Action to one consumer."""

    producer_action: ActionIdentity
    consumer_action: ActionIdentity
    carrier: Carrier


def reaching_pair_key[PairAction: Hashable](
    pair: ReachingPair[PairAction], /
) -> tuple[PairAction, PairAction, CarrierIdentityKey]:
    """Return the one canonical order key for semantic pair evidence."""

    return pair.producer_action, pair.consumer_action, pair.carrier.identity_key


@dataclass(frozen=True)
class CarrierIncidence:
    """One Carrier and its exact incident contextual dependence pairs."""

    carrier: Carrier
    pairs: tuple[ReachingPair[ContextualActionId], ...]


@dataclass(frozen=True)
class RealizationBatch:
    """An Action cover and its distinct Carrier incidences."""

    actions: tuple[ContextualActionId, ...]
    incidences: tuple[CarrierIncidence, ...]


@dataclass(frozen=True)
class ActionTransition[ActionIdentity: Hashable]:
    """Carriers required and potentially established by one Action."""

    action_id: ActionIdentity
    requires: frozenset[Carrier]
    establishes: frozenset[Carrier]


@dataclass(frozen=True)
class EntryDemand:
    """One external Carrier required by one exact contextual Action."""

    consumer_action: ContextualActionId
    carrier: Carrier


@dataclass(frozen=True)
class ActionProjection:
    """Generated Action forest plus exact semantic coverage receipts."""

    fragment: ActionForest
    covers: frozenset[ContextualActionId]
    materializes: frozenset[Carrier]


@dataclass(frozen=True)
class InternalProjection:
    """One certified local program."""

    program: ActionProjection


@dataclass(frozen=True)
class BoundaryProjection:
    """Certified programs on the export and import sides of a boundary."""

    export: ActionProjection
    import_: ActionProjection


@dataclass(frozen=True)
class UDFActionForest:
    """One complete generated target-runtime module."""

    forest: ActionForest


@dataclass(frozen=True)
class WireForm:
    """Exactly the materialized fields of one boundary realization."""

    fields: tuple[str, ...]


class InternalApplicability(Protocol):
    """Tamper guard for one prepared internal realization application."""

    def __call__(self, batch: RealizationBatch, /) -> bool: ...


class BoundaryApplicability(Protocol):
    """Tamper guard for one prepared boundary realization application."""

    def __call__(self, batch: RealizationBatch, /) -> bool: ...


class InternalRenderer(Protocol):
    """Render one certified local batch against its source forest."""

    def __call__(
        self, forest: ActionForest, batch: RealizationBatch, /
    ) -> InternalProjection: ...


class BoundaryRenderer(Protocol):
    """Render one certified crossing under its exact selected WireForm."""

    def __call__(
        self, batch: RealizationBatch, wire: WireForm, /
    ) -> BoundaryProjection: ...


@dataclass(frozen=True)
class InternalRealization:
    """One legal local projection family for an exact Action batch."""

    realization_id: str
    applicability: InternalApplicability
    realize: InternalRenderer


@dataclass(frozen=True)
class BoundaryRealization:
    """One legal crossing family with inert wire and executable projection."""

    realization_id: str
    applicability: BoundaryApplicability
    wire: WireForm
    realize: BoundaryRenderer


@dataclass(frozen=True)
class InternalApplication:
    """One provider-certified local realization for one exact batch."""

    realization: InternalRealization
    batch: RealizationBatch
    cost: int

    def __post_init__(self) -> None:
        if not isinstance(self.cost, int) or self.cost < 0:
            raise ValueError("internal application cost must be nonnegative")


@dataclass(frozen=True)
class BoundaryApplication:
    """One provider-certified crossing realization for one exact batch."""

    realization: BoundaryRealization
    batch: RealizationBatch
    cost: int

    def __post_init__(self) -> None:
        if not isinstance(self.cost, int) or self.cost < 0:
            raise ValueError("boundary application cost must be nonnegative")
