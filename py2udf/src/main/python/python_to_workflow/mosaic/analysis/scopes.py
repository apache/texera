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

"""Source-exact lexical scope identities for modular Mosaic analysis.

The index is an ephemeral view of the ActionForest and CPython's symbol-table
classification.  It is neither a provider fact nor a second source tree.  Its
named records keep scope meaning out of anonymous dict/tuple positions.
"""

from __future__ import annotations

import ast
import symtable
from collections.abc import Mapping
from dataclasses import dataclass
from functools import total_ordering
from types import MappingProxyType
from typing import Literal

from python_to_workflow.mosaic.forest import (
    ActionForest,
    ForestBuildError,
    Parameter,
    ParameterRole,
    _Builder,
)

type ActionId = str
type ActionIds = tuple[ActionId, ...]
type SourceSignature = str
type SymbolTable = symtable.SymbolTable


@total_ordering
@dataclass(frozen=True)
class ScopeId:
    """Stable semantic identity of one real source callable/class namespace."""

    path: str
    signature: SourceSignature
    captures: tuple[str, ...]

    def __lt__(self, other: object) -> bool:
        """Provide one deterministic order for canonical mixed-scope rows."""

        if isinstance(other, str):
            return False
        if isinstance(other, ScopeId):
            return (self.path, self.signature, self.captures) < (
                other.path,
                other.signature,
                other.captures,
            )
        return NotImplemented


type ModuleScope = Literal["module"]
type LexicalScopeId = ModuleScope | ScopeId
type BindingIdentity = tuple[LexicalScopeId, str]


@dataclass(frozen=True)
class LexicalScope:
    """One real source namespace and its directly executed forest Actions."""

    parent: LexicalScopeId | None
    roots: ActionIds
    actions: frozenset[ActionId]
    symbols: SymbolTable


type ScopeTable = Mapping[LexicalScopeId, LexicalScope]
type ActionScopeTable = Mapping[ActionId, LexicalScopeId]


@dataclass(frozen=True)
class LexicalScopeIndex:
    """Immutable joins from source Actions and definitions to lexical scopes."""

    builder: _Builder
    scopes: ScopeTable
    action_scopes: ActionScopeTable
    owner_scopes: ActionScopeTable


@dataclass
class _ScopeDraft:
    """Transient construction state; frozen before it leaves the indexer."""

    parent: LexicalScopeId | None
    roots: ActionIds
    actions: set[ActionId]
    symbols: SymbolTable


class _ScopeIndexer:
    """Build the lexical view once from the source-exact forest."""

    def __init__(self, forest: ActionForest) -> None:
        self.forest = forest
        self.builder = forest._source_builder
        module = symtable.symtable(forest.source, "<mosaic>", "exec")
        self.scopes: dict[LexicalScopeId, _ScopeDraft] = {
            "module": _ScopeDraft(None, forest.roots, set(), module)
        }
        self.action_scopes: dict[ActionId, LexicalScopeId] = {}
        self.owner_scopes: dict[ActionId, LexicalScopeId] = {}

    def build(self) -> LexicalScopeIndex:
        """Assign every Action exactly once and freeze every index view."""

        self._assign_actions(self.forest.roots, "module")
        scopes = {
            scope_id: LexicalScope(
                draft.parent,
                draft.roots,
                frozenset(draft.actions),
                draft.symbols,
            )
            for scope_id, draft in self.scopes.items()
        }
        return LexicalScopeIndex(
            self.builder,
            MappingProxyType(scopes),
            MappingProxyType(dict(self.action_scopes)),
            MappingProxyType(dict(self.owner_scopes)),
        )

    def _assign_actions(self, action_ids: ActionIds, scope_id: LexicalScopeId) -> None:
        """Assign a source-ordered Action sequence to one execution scope."""

        for action_id in action_ids:
            self._assign_action(action_id, scope_id)

    def _assign_action(self, action_id: ActionId, scope_id: LexicalScopeId) -> None:
        """Assign an Action and recursively divert only real body scopes."""

        self.scopes[scope_id].actions.add(action_id)
        self.action_scopes[action_id] = scope_id
        node = self.builder.action_nodes[action_id]
        child_scope = self._definition_scope(action_id, node, scope_id)
        for parameter_id in self.forest.action(action_id).parameters:
            parameter = self.forest.parameter(parameter_id)
            destination = _body_destination(parameter, child_scope, scope_id)
            if destination == child_scope:
                self.scopes[child_scope].roots = parameter.actions
            self._assign_actions(parameter.actions, destination)

    def _definition_scope(
        self, owner: ActionId, node: ast.AST, parent_scope: LexicalScopeId
    ) -> ScopeId | None:
        """Register a function/class body scope; headers remain in the parent."""

        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return None
        table = self._matching_table(parent_scope, node)
        parent_path = _scope_path(parent_scope)
        path = node.name if parent_path == "module" else f"{parent_path}.{node.name}"
        captures = tuple(
            sorted(
                symbol.get_name()
                for symbol in table.get_symbols()
                if symbol.is_free() or symbol.is_nonlocal()
            )
        )
        scope_id = ScopeId(path, _source_signature(node), captures)
        self.scopes[scope_id] = _ScopeDraft(parent_scope, (), set(), table)
        self.owner_scopes[owner] = scope_id
        return scope_id

    def _matching_table(
        self,
        parent_scope: LexicalScopeId,
        node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    ) -> SymbolTable:
        """Join one forest definition to CPython's corresponding symbol table."""

        kind = "class" if isinstance(node, ast.ClassDef) else "function"
        used = {id(scope.symbols) for scope in self.scopes.values()}
        candidates = tuple(
            table
            for table in self.scopes[parent_scope].symbols.get_children()
            if table.get_name() == node.name
            and table.get_lineno() == node.lineno
            and table.get_type() == kind
            and id(table) not in used
        )
        if len(candidates) != 1:
            raise ForestBuildError(
                f"cannot identify lexical scope for {node.name!r} at line {node.lineno}"
            )
        return candidates[0]


def build_lexical_scope_index(forest: ActionForest) -> LexicalScopeIndex:
    """Return the lexical view for the source forest."""

    return _ScopeIndexer(forest).build()


def binding_identity(
    index: LexicalScopeIndex, scope_id: LexicalScopeId, name: str
) -> BindingIdentity:
    """Name a binding according to CPython's lexical symbol classification."""

    if scope_id == "module":
        return "module", name
    try:
        symbol = index.scopes[scope_id].symbols.lookup(name)
    except KeyError:
        return "module", name
    if symbol.is_global():
        return "module", name
    if symbol.is_free() or symbol.is_nonlocal():
        return _free_binding_scope(index, scope_id, name), name
    return scope_id, name


def _free_binding_scope(
    index: LexicalScopeIndex, scope_id: LexicalScopeId, name: str
) -> LexicalScopeId:
    """Find the real defining namespace of one free/nonlocal cell."""

    current = index.scopes[scope_id].parent
    while current not in {None, "module"}:
        try:
            symbol = index.scopes[current].symbols.lookup(name)
        except KeyError:
            current = index.scopes[current].parent
            continue
        if symbol.is_local() or symbol.is_parameter():
            return current
        current = index.scopes[current].parent
    return "module"


def _body_destination(
    parameter: Parameter,
    child_scope: ScopeId | None,
    parent_scope: LexicalScopeId,
) -> LexicalScopeId:
    """Keep headers in the parent and move only the definition body."""

    is_body = (
        child_scope is not None
        and parameter.role is ParameterRole.SUITE
        and parameter.name == "body"
    )
    return child_scope if is_body else parent_scope


def _scope_path(scope_id: LexicalScopeId) -> str:
    """Project the readable lexical path without parsing semantic behavior."""

    return scope_id if isinstance(scope_id, str) else scope_id.path


def _source_signature(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
) -> SourceSignature:
    """Produce a deterministic signature component from source AST fields."""

    if isinstance(node, ast.ClassDef):
        bases = ", ".join(ast.unparse(base) for base in node.bases)
        keywords = ", ".join(
            f"{keyword.arg}={ast.unparse(keyword.value)}" for keyword in node.keywords
        )
        separator = ", " if bases and keywords else ""
        return f"({bases}{separator}{keywords})"
    parameters = ast.unparse(node.args)
    returns = "" if node.returns is None else f" -> {ast.unparse(node.returns)}"
    return f"({parameters}){returns}"
