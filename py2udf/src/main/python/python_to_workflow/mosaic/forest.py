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

"""Source-exact Action/Parameter forests for Python statements.

The public graph is deliberately small: executable ``Command`` and
``Expression`` actions alternate with non-executable ``Parameter`` source
containers.  Python's AST is used only to discover that structure; AST helper
nodes, operators, and contexts never leak into the graph as fake actions.
Evaluation order is deliberately absent; ``linearize.py`` derives semantics
from this source structure in a separate stage.
"""

from __future__ import annotations

import ast
import bisect
import tokenize
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, replace
from enum import Enum
from functools import cached_property, lru_cache
from io import StringIO


class ForestBuildError(ValueError):
    """The source cannot be represented by the current linear forest rung."""


class ParameterRole(Enum):
    """How an Action interprets the exact source carried by a Parameter."""

    VALUE = "value"
    TARGET = "target"
    SUITE = "suite"


class AstDisposition(Enum):
    """Total classification of Python AST classes, not a graph-node kind."""

    ACTION = "action"
    STRUCTURE = "structure"
    METADATA = "metadata"
    COMPOUND = "compound"


class NameAccess(Enum):
    """Syntactic access performed by one exact Python ``Name`` occurrence."""

    LOAD = "load"
    STORE = "store"
    DELETE = "delete"


@dataclass(frozen=True)
class SourceSpan:
    """Absolute offsets and one-based line/column endpoints in source."""

    start_offset: int
    end_offset: int
    start: tuple[int, int]
    end: tuple[int, int]

    @property
    def start_line(self) -> int:
        """Return the one-based first source line."""

        return self.start[0]

    @property
    def start_column(self) -> int:
        """Return the zero-based first source column."""

        return self.start[1]

    @property
    def end_line(self) -> int:
        """Return the one-based final source line."""

        return self.end[0]

    @property
    def end_column(self) -> int:
        """Return the zero-based exclusive final source column."""

        return self.end[1]


@dataclass(frozen=True)
class NameOccurrence:
    """One source-owned name span and its Python access context."""

    name: str
    access: NameAccess
    span: SourceSpan


def _name_access(context: ast.expr_context, /) -> NameAccess:
    """Map Python's closed name-context family to the public syntax record."""

    if isinstance(context, ast.Load):
        return NameAccess.LOAD
    if isinstance(context, ast.Store):
        return NameAccess.STORE
    if isinstance(context, ast.Del):
        return NameAccess.DELETE
    raise ForestBuildError(f"unsupported Name context: {type(context).__name__}")


class _ModuleNameOccurrenceVisitor(ast.NodeVisitor):
    """Select module-name occurrences without capturing comprehension locals."""

    def __init__(
        self,
        index: _SourceIndex,
        names: frozenset[str],
    ) -> None:
        self._index = index
        self._names = names
        self._bound: frozenset[str] = frozenset()
        self.rows: list[NameOccurrence] = []

    def visit_Name(self, node: ast.Name) -> None:
        """Record one unshadowed module-name occurrence."""

        if node.id in self._names and node.id not in self._bound:
            self.rows.append(
                NameOccurrence(node.id, _name_access(node.ctx), self._index.span(node))
            )

    def visit_ListComp(self, node: ast.ListComp) -> None:
        """Visit a list comprehension with Python's nested binding scope."""

        self._visit_comprehension(node.generators, (node.elt,))

    def visit_SetComp(self, node: ast.SetComp) -> None:
        """Visit a set comprehension with Python's nested binding scope."""

        self._visit_comprehension(node.generators, (node.elt,))

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        """Visit a generator expression with Python's nested binding scope."""

        self._visit_comprehension(node.generators, (node.elt,))

    def visit_DictComp(self, node: ast.DictComp) -> None:
        """Visit a dictionary comprehension with Python's nested binding scope."""

        self._visit_comprehension(node.generators, (node.key, node.value))

    def _visit_comprehension(
        self,
        generators: list[ast.comprehension],
        outputs: tuple[ast.expr, ...],
    ) -> None:
        inherited = self._bound
        current = set(inherited)
        try:
            for generator in generators:
                self._bound = frozenset(current)
                self.visit(generator.iter)
                self._visit_target(generator.target)
                current.update(_stored_target_names(generator.target))
                self._bound = frozenset(current)
                for condition in generator.ifs:
                    self.visit(condition)
            for output in outputs:
                self.visit(output)
        finally:
            self._bound = inherited

    def _visit_target(self, node: ast.AST) -> None:
        """Skip local Name stores but retain evaluated target subexpressions."""

        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            return
        if isinstance(node, (ast.Tuple, ast.List, ast.Starred)):
            for child in ast.iter_child_nodes(node):
                self._visit_target(child)
            return
        self.visit(node)


class _PythonActivationBindingVisitor(ast.NodeVisitor):
    """Collect binders encoded outside ordinary ``Name`` target nodes."""

    def __init__(self) -> None:
        self.names: set[str] = set()

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        """Record an exception alias and inspect its executable expressions."""

        if node.name is not None:
            self.names.add(node.name)
        if node.type is not None:
            self.visit(node.type)
        for statement in node.body:
            self.visit(statement)

    def visit_MatchAs(self, node: ast.MatchAs) -> None:
        """Record an ``as`` or bare capture pattern."""

        if node.name is not None:
            self.names.add(node.name)
        if node.pattern is not None:
            self.visit(node.pattern)

    def visit_MatchStar(self, node: ast.MatchStar) -> None:
        """Record a starred sequence-pattern capture."""

        if node.name is not None:
            self.names.add(node.name)

    def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
        """Record a mapping-rest capture and nested patterns."""

        if node.rest is not None:
            self.names.add(node.rest)
        for key in node.keys:
            self.visit(key)
        for pattern in node.patterns:
            self.visit(pattern)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Do not leak binders from an unexecuted nested function body."""

        del node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Do not leak binders from an unexecuted async function body."""

        del node

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Do not leak binders from a nested class namespace."""

        del node

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Do not leak binders from a nested lambda scope."""

        del node


def _stored_target_names(target: ast.AST, /) -> frozenset[str]:
    """Return names bound in one comprehension target pattern."""

    return frozenset(
        node.id
        for node in ast.walk(target)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store)
    )


@dataclass(frozen=True)
class AliasValueFlow:
    """Assignment targets receive exactly one existing binding value."""

    targets: tuple[str, ...]
    source: str


@dataclass(frozen=True)
class FreshValueFlow:
    """Assignment targets share one fresh allocation and its initial contents."""

    targets: tuple[str, ...]
    type_name: str
    contents: tuple[str, ...]


@dataclass(frozen=True)
class ContentLoadFlow:
    """Assignment targets receive an object reachable through a container."""

    targets: tuple[str, ...]
    container: str


@dataclass(frozen=True)
class MethodCallFlow:
    """Assignment targets receive the result of a receiver method call."""

    targets: tuple[str, ...]
    receiver: str
    method: str
    attributes: tuple[str, ...]
    inputs: CallInputs


@dataclass(frozen=True)
class CallInput:
    """Source-visible bindings that may contribute one call operand."""

    bindings: tuple[str, ...]


@dataclass(frozen=True)
class CallInputs:
    """Named call inputs without positional/keyword tuple conventions."""

    positional: tuple[CallInput, ...]
    keywords: tuple[CallInput, ...]

    def bindings(self) -> tuple[str, ...]:
        """Return every visible binding once in canonical order."""

        return tuple(
            sorted(
                {
                    binding
                    for item in (*self.positional, *self.keywords)
                    for binding in item.bindings
                }
            )
        )


@dataclass(frozen=True)
class MethodEffect:
    """One executed receiver call and its source-visible argument bindings."""

    receiver: str
    method: str
    inputs: CallInputs


@dataclass(frozen=True)
class NamedCall:
    """One direct name call whose builtin fallback can be certified."""

    action_id: str
    name: str
    inputs: CallInputs


@dataclass(frozen=True)
class ReceiverCall:
    """One direct binding receiver call eligible for exact type evidence."""

    action_id: str
    receiver: str
    method: str
    attributes: tuple[str, ...]
    inputs: CallInputs


@dataclass(frozen=True)
class DynamicCall:
    """A call whose callable expression has no exact statement-level shape."""

    action_id: str
    callable: CallInput
    inputs: CallInputs


type SourceCall = NamedCall | ReceiverCall | DynamicCall


@dataclass(frozen=True, order=True)
class ImportBinding:
    """One local import name and its exact qualified source target."""

    name: str
    qualified: str


@dataclass(frozen=True, order=True)
class ImportOccurrence:
    """Exact import bindings established by one source Action."""

    action_id: str
    bindings: tuple[ImportBinding, ...]


@dataclass(frozen=True)
class UnknownValueFlow:
    """Assignment result may be fresh or alias any source-visible input."""

    targets: tuple[str, ...]
    sources: tuple[str, ...]


type BindingValueFlow = (
    AliasValueFlow
    | FreshValueFlow
    | ContentLoadFlow
    | MethodCallFlow
    | UnknownValueFlow
)


@dataclass(frozen=True)
class SourceStatement:
    """One top-level statement shape retained from the parsed source."""

    root_id: str
    kind: str
    span: SourceSpan
    nested_kinds: tuple[str, ...]
    restrictions: tuple[str, ...]
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    in_place_writes: tuple[str, ...]
    mutates: tuple[str, ...]
    imports: tuple[str, ...]
    import_bindings: tuple[ImportBinding, ...]
    value_flows: tuple[BindingValueFlow, ...]
    calls: tuple[SourceCall, ...]
    method_effects: tuple[MethodEffect, ...]


@dataclass(frozen=True)
class SourceInventory:
    """Typed source-admission input, independent of solver and renderer policy."""

    statements: tuple[SourceStatement, ...]
    import_occurrences: tuple[ImportOccurrence, ...]


@dataclass(frozen=True, order=True)
class TemplateHole:
    """Where one child's source was removed from its owner's template.

    A hole is a **position**, never a byte pattern. The template around it is
    arbitrary Python source in which `{0}`, `{value}`, and `{left}` are all
    legal text — set displays, format strings, dict keys, comments — so a
    filler that searched for those bytes would splice a child into text that
    merely looked like a hole, and could even accept an ambiguous template
    whose reconstruction happened to match. Offsets are in template
    coordinates and never overlap.
    """

    start: int
    end: int
    name: str


#: One token of the source paired with the exact span it occupies.
type _TokenSpan = tuple[tokenize.TokenInfo, SourceSpan]

#: Every token lying inside the indexed lines, in source order.
type _TokenSpans = tuple[_TokenSpan, ...]

#: One child to cut out of its owner: where it sits and what names it.
type _ChildCut = tuple[SourceSpan, str]

#: The holes of one template, ordered by position.
type TemplateHoles = tuple[TemplateHole, ...]


#: The source line each template line began on, one entry per template line.
type SourceLineNumbers = tuple[int, ...]


@dataclass(frozen=True)
class TemplateSource:
    """One template, the positions its children were cut from, and its origin.

    The three travel together because none is usable alone: filling needs the
    positions, the positions mean nothing without the text they index, and
    relocating the text needs to know which source line each of its lines came
    from. Any record that carries a template carries this instead of a bare
    string, so a template can never reach a filler without the holes that belong
    to it, nor a relocation without the origins it has to consult.

    `source_lines` exists because the obvious arithmetic is wrong. A template
    line's source line is not its index plus the owner's first line: a multi-line
    child is cut down to a one-line sentinel, so every line after the first such
    hole sits earlier in the template than in the source.
    """

    template: str
    holes: TemplateHoles
    source_lines: SourceLineNumbers


@dataclass(frozen=True)
class Command:
    """Executable statement-like Action with source-exact child holes."""

    id: str
    source_span: SourceSpan
    source_text: str
    source_template: TemplateSource
    parameters: tuple[str, ...]

    @property
    def template(self) -> str:
        """The owner's source with each child replaced by a written sentinel."""

        return self.source_template.template

    @property
    def holes(self) -> TemplateHoles:
        """Where each child was removed, in template coordinates."""

        return self.source_template.holes


@dataclass(frozen=True)
class Expression:
    """Executable expression Action with source-exact child holes."""

    id: str
    source_span: SourceSpan
    source_text: str
    source_template: TemplateSource
    parameters: tuple[str, ...]

    @property
    def template(self) -> str:
        """The owner's source with each child replaced by a written sentinel."""

        return self.source_template.template

    @property
    def holes(self) -> TemplateHoles:
        """Where each child was removed, in template coordinates."""

        return self.source_template.holes


type Action = Command | Expression

_PARAMETER_CHILD_TYPES: dict[ParameterRole, tuple[type[Action], ...]] = {
    ParameterRole.VALUE: (Expression,),
    ParameterRole.TARGET: (Expression,),
    ParameterRole.SUITE: (Command, Expression),
}


@dataclass(frozen=True)
class _ParameterSource:
    source_span: SourceSpan
    source_text: str
    source_template: TemplateSource
    actions: tuple[str, ...]

    @property
    def template(self) -> str:
        """The owner's source with each child replaced by a written sentinel."""

        return self.source_template.template

    @property
    def holes(self) -> TemplateHoles:
        """Where each child was removed, in template coordinates."""

        return self.source_template.holes


@dataclass(frozen=True)
class Parameter:
    """Non-executable source container owned by exactly one Action."""

    id: str
    owner: str
    name: str
    role: ParameterRole
    source: _ParameterSource

    @property
    def source_span(self) -> SourceSpan:
        """Return the exact source extent represented by this Parameter."""

        return self.source.source_span

    @property
    def source_text(self) -> str:
        """Return the original source represented by this Parameter."""

        return self.source.source_text

    @property
    def template(self) -> str:
        """Return Parameter source with child Actions replaced by holes."""

        return self.source.template

    @property
    def actions(self) -> tuple[str, ...]:
        """Return child Action IDs in source order."""

        return self.source.actions


@dataclass(frozen=True)
class ActionForest:
    """Immutable source-exact alternating graph of Actions and Parameters."""

    source: str
    actions: tuple[Action, ...]
    parameters: tuple[Parameter, ...]

    @cached_property
    def _actions_by_id(self) -> dict[str, Action]:
        return {action.id: action for action in self.actions}

    @cached_property
    def _source_builder(self) -> _Builder:
        """Store the source-exact structural view for this immutable forest."""

        builder = _Builder(self.source)
        if builder.build() != self:
            raise ForestBuildError("ActionForest cannot rebuild its exact source")
        return builder

    @cached_property
    def _parameters_by_id(self) -> dict[str, Parameter]:
        return {parameter.id: parameter for parameter in self.parameters}

    @cached_property
    def _parent_parameter_by_action(self) -> dict[str, str]:
        return {
            child_action: parameter.id
            for parameter in self.parameters
            for child_action in parameter.actions
        }

    def action(self, action_id: str) -> Action:
        """Resolve one source Action; execution contexts live outside the forest."""

        return self._actions_by_id[action_id]

    def parameter(self, parameter_id: str) -> Parameter:
        """Resolve one structural Parameter by its canonical ID."""

        return self._parameters_by_id[parameter_id]

    def parent_action(self, action_id: str) -> str | None:
        """Return the Action owning the Parameter that contains an Action."""

        parameter_id = self.parent_parameter(action_id)
        if parameter_id is None:
            return None
        owner = self.parameter(parameter_id).owner
        return owner

    def parent_parameter(self, action_id: str) -> str | None:
        """Return the unique structural Parameter containing this Action."""

        return self._parent_parameter_by_action.get(action_id)

    def root_of(self, action_id: str) -> str:
        """Return the cached outer source root owning one exact Action."""

        try:
            return self._root_by_action[action_id]
        except KeyError as error:
            raise KeyError(f"unknown Action {action_id!r}") from error

    @cached_property
    def _root_by_action(self) -> dict[str, str]:
        """Index structural ancestry once for all analysis consumers."""

        result: dict[str, str] = {}
        for action in self.actions:
            path = []
            current = action.id
            while current not in result:
                path.append(current)
                parent = self.parent_action(current)
                if parent is None:
                    result[current] = current
                    break
                current = parent
            root = result[current]
            result.update((item, root) for item in path)
        return result

    @cached_property
    def roots(self) -> tuple[str, ...]:
        """Derive source roots from ownership; no StatementTree is stored."""

        children = set(self._parent_parameter_by_action)
        return tuple(action.id for action in self.actions if action.id not in children)

    def reconstruct_action(self, action_id: str) -> str:
        """Rebuild one Action from its exact template and descendants."""

        action = self.action(action_id)
        values = {
            self.parameter(parameter_id).name: self.reconstruct_parameter(parameter_id)
            for parameter_id in action.parameters
        }
        return _fill(action.template, action.holes, values)

    def reconstruct_parameter(self, parameter_id: str) -> str:
        """Rebuild one Parameter from its exact template and child Actions."""

        parameter = self.parameter(parameter_id)
        values = {
            str(index): self.reconstruct_action(action_id)
            for index, action_id in enumerate(parameter.actions)
        }
        return _fill(parameter.template, parameter.source.holes, values)

    def name_occurrences(
        self,
        action_id: str,
        names: frozenset[str] | None = None,
        /,
    ) -> tuple[NameOccurrence, ...]:
        """Return exact ``Name`` spans from the parsed AST."""

        owner = self._source_builder.action_nodes[action_id]
        rows = tuple(
            NameOccurrence(
                node.id, _name_access(node.ctx), self._source_builder.index.span(node)
            )
            for node in ast.walk(owner)
            if isinstance(node, ast.Name) and (names is None or node.id in names)
        )
        return tuple(sorted(rows, key=lambda row: row.span.start_offset))

    def module_name_occurrences(
        self,
        action_id: str,
        names: frozenset[str],
        /,
    ) -> tuple[NameOccurrence, ...]:
        """Return occurrences resolved to the surrounding module namespace."""

        visitor = _ModuleNameOccurrenceVisitor(self._source_builder.index, names)
        visitor.visit(self._source_builder.action_nodes[action_id])
        return tuple(sorted(visitor.rows, key=lambda row: row.span.start_offset))

    def python_activation_bindings(self, action_id: str, /) -> frozenset[str]:
        """Return syntax-owned binding names that cannot cross an activation."""

        visitor = _PythonActivationBindingVisitor()
        visitor.visit(self._source_builder.action_nodes[action_id])
        return frozenset(visitor.names)

    def project_action(
        self,
        action_id: str,
        replacements: Mapping[SourceSpan, str],
        /,
    ) -> str:
        """Project one Action by replacing validated, non-overlapping spans."""

        action = self.action(action_id)
        spans = tuple(sorted(replacements, key=lambda span: span.start_offset))
        if any(
            span.start_offset < action.source_span.start_offset
            or span.end_offset > action.source_span.end_offset
            or span.start_offset >= span.end_offset
            for span in spans
        ):
            raise ValueError("projected span lies outside its Action")
        if any(
            left.end_offset > right.start_offset
            for left, right in zip(spans, spans[1:], strict=False)
        ):
            raise ValueError("projected Action spans overlap")
        result = action.source_text
        base = action.source_span.start_offset
        for span in reversed(spans):
            start = span.start_offset - base
            end = span.end_offset - base
            result = result[:start] + replacements[span] + result[end:]
        return result

    def validate(self) -> None:
        """Validate this immutable Forest once, then reuse its receipt."""

        _ = self._validation_receipt

    @cached_property
    def _validation_receipt(self) -> bool:
        self._validate_uncached()
        return True

    def _validate_uncached(self) -> None:
        action_ids = _validate_forest_identifiers(self)
        parent_count = dict.fromkeys(action_ids, 0)
        _validate_action_parameters(self)
        _validate_parameter_children(self, parent_count)
        _validate_forest_roots(self, parent_count)
        _validate_reconstruction(self)


@dataclass(frozen=True)
class BuiltSource:
    """The forest and admission inventory produced by one shared AST parse."""

    forest: ActionForest
    inventory: SourceInventory

    def statement(self, root_id: str) -> SourceStatement:
        """Resolve one top-level statement through the source-owned index."""

        try:
            return self._statements_by_root[root_id]
        except KeyError as error:
            raise KeyError(f"unknown statement root {root_id!r}") from error

    @cached_property
    def _statements_by_root(self) -> dict[str, SourceStatement]:
        """Index immutable statement summaries once for all consumers."""

        return {statement.root_id: statement for statement in self.inventory.statements}


def _validate_forest_identifiers(forest: ActionForest) -> tuple[str, ...]:
    action_ids = tuple(action.id for action in forest.actions)
    parameter_ids = tuple(parameter.id for parameter in forest.parameters)
    checks = (
        (len(action_ids) != len(set(action_ids)), "Action identifiers are not unique"),
        (
            len(parameter_ids) != len(set(parameter_ids)),
            "Parameter identifiers are not unique",
        ),
        (
            bool(set(action_ids) & set(parameter_ids)),
            "Action and Parameter identifiers overlap",
        ),
    )
    failure = next((message for failed, message in checks if failed), None)
    if failure is not None:
        raise ForestBuildError(failure)
    return action_ids


def _validate_action_parameters(forest: ActionForest) -> None:
    for action in forest.actions:
        _validate_placeholders(
            action.holes,
            {forest.parameter(item).name for item in action.parameters},
        )
        for parameter_id in action.parameters:
            if forest.parameter(parameter_id).owner != action.id:
                raise ForestBuildError("Parameter owner does not match its Action")


def _validate_parameter_children(
    forest: ActionForest, parent_count: dict[str, int]
) -> None:
    for parameter in forest.parameters:
        if not isinstance(parameter.role, ParameterRole):
            raise ForestBuildError("Parameter role is invalid")
        _validate_placeholders(
            parameter.source.holes,
            {str(index) for index in range(len(parameter.actions))},
        )
        expected_child_types = _PARAMETER_CHILD_TYPES[parameter.role]
        for action_id in parameter.actions:
            if not isinstance(forest.action(action_id), expected_child_types):
                raise ForestBuildError("Parameter child type does not match its role")
            parent_count[action_id] += 1


def _validate_forest_roots(
    forest: ActionForest, parent_count: Mapping[str, int]
) -> None:
    roots = set(forest.roots)
    for action_id, count in parent_count.items():
        expected = 0 if action_id in roots else 1
        if count != expected:
            raise ForestBuildError("Action/Parameter alternation is not a forest")


def _validate_reconstruction(forest: ActionForest) -> None:
    reconstructed_actions: dict[str, str] = {}
    reconstructed_parameters: dict[str, str] = {}

    def reconstruct_action(action_id: str) -> str:
        cached = reconstructed_actions.get(action_id)
        if cached is not None:
            return cached
        action = forest.action(action_id)
        result = _fill(
            action.template,
            action.holes,
            {
                forest.parameter(parameter_id).name: reconstruct_parameter(parameter_id)
                for parameter_id in action.parameters
            },
        )
        reconstructed_actions[action_id] = result
        return result

    def reconstruct_parameter(parameter_id: str) -> str:
        cached = reconstructed_parameters.get(parameter_id)
        if cached is not None:
            return cached
        parameter = forest.parameter(parameter_id)
        result = _fill(
            parameter.template,
            parameter.source.holes,
            {
                str(index): reconstruct_action(action_id)
                for index, action_id in enumerate(parameter.actions)
            },
        )
        reconstructed_parameters[parameter_id] = result
        return result

    if any(
        reconstruct_action(action.id) != action.source_text for action in forest.actions
    ):
        raise ForestBuildError("Action template is not source-exact")
    if any(
        reconstruct_parameter(parameter.id) != parameter.source_text
        for parameter in forest.parameters
    ):
        raise ForestBuildError("Parameter template is not source-exact")


_COMPOUND_STATEMENTS = tuple(
    item
    for item in (
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.ClassDef,
        ast.For,
        ast.AsyncFor,
        ast.While,
        ast.If,
        ast.With,
        ast.AsyncWith,
        ast.Match,
        ast.Try,
        getattr(ast, "TryStar", None),
    )
    if item is not None
)

_FIELD_NAMES: dict[type[ast.AST], dict[str, str]] = {
    ast.FunctionDef: {
        "name": "target",
        "args": "signature",
        "body": "body",
        "decorator_list": "decorator",
        "returns": "return_annotation",
    },
    ast.ClassDef: {
        "name": "target",
        "bases": "base",
        "keywords": "keyword",
        "body": "body",
        "decorator_list": "decorator",
    },
    ast.arguments: {
        "posonlyargs": "annotation",
        "args": "annotation",
        "vararg": "annotation",
        "kwonlyargs": "annotation",
        "kw_defaults": "default",
        "kwarg": "annotation",
        "defaults": "default",
    },
    ast.arg: {"annotation": "value"},
    ast.If: {"test": "condition", "body": "body", "orelse": "orelse"},
    ast.While: {"test": "condition", "body": "body", "orelse": "orelse"},
    ast.For: {
        "target": "target",
        "iter": "iterable",
        "body": "body",
        "orelse": "orelse",
    },
    ast.With: {"items": "item", "body": "body"},
    ast.Try: {
        "body": "body",
        "handlers": "handler",
        "orelse": "orelse",
        "finalbody": "finalbody",
    },
    ast.Match: {"subject": "subject", "cases": "case"},
    ast.match_case: {
        "pattern": "pattern",
        "guard": "guard",
        "body": "body",
    },
    ast.withitem: {"context_expr": "context", "optional_vars": "target"},
    ast.ExceptHandler: {"type": "type", "name": "target", "body": "body"},
    ast.Assign: {"targets": "target", "value": "value"},
    ast.AnnAssign: {
        "target": "target",
        "annotation": "annotation",
        "value": "value",
    },
    ast.AugAssign: {"target": "target", "value": "value"},
    ast.NamedExpr: {"target": "target", "value": "value"},
    ast.BinOp: {"left": "left", "right": "right"},
    ast.BoolOp: {"values": "operand"},
    ast.UnaryOp: {"operand": "operand"},
    ast.IfExp: {"body": "then", "test": "condition", "orelse": "otherwise"},
    ast.Call: {"func": "callable", "args": "positional", "keywords": "keyword"},
    ast.Attribute: {"value": "base"},
    ast.Subscript: {"value": "base", "slice": "index"},
    ast.Compare: {"left": "left", "comparators": "comparator"},
    ast.Lambda: {"args": "parameters", "body": "body"},
    ast.Return: {"value": "value"},
    ast.Delete: {"targets": "target"},
    ast.Raise: {"exc": "error", "cause": "cause"},
    ast.Assert: {"test": "condition", "msg": "message"},
    ast.Import: {"names": "module"},
    ast.ImportFrom: {"names": "name"},
    ast.List: {"elts": "element"},
    ast.Tuple: {"elts": "element"},
    ast.Set: {"elts": "element"},
    ast.Dict: {"keys": "key", "values": "value"},
    ast.ListComp: {"elt": "element", "generators": "generator"},
    ast.SetComp: {"elt": "element", "generators": "generator"},
    ast.GeneratorExp: {"elt": "element", "generators": "generator"},
    ast.DictComp: {
        "key": "key",
        "value": "value",
        "generators": "generator",
    },
    ast.Yield: {"value": "value"},
    ast.YieldFrom: {"value": "value"},
    ast.Await: {"value": "value"},
    ast.JoinedStr: {"values": "part"},
    ast.Slice: {"lower": "lower", "upper": "upper", "step": "step"},
}

_ATOMIC_ACTIONS = (
    ast.Constant,
    ast.Name,
    ast.Pass,
    ast.Break,
    ast.Continue,
    ast.Global,
    ast.Nonlocal,
)


def ast_disposition(node_type: type[ast.AST]) -> AstDisposition:
    """Classify every concrete Python AST class without silently dropping one."""

    if issubclass(node_type, _COMPOUND_STATEMENTS):
        return AstDisposition.COMPOUND
    if issubclass(node_type, ast.expr):
        return AstDisposition.ACTION
    if issubclass(node_type, ast.stmt):
        return AstDisposition.ACTION
    if issubclass(
        node_type,
        (ast.operator, ast.unaryop, ast.boolop, ast.cmpop, ast.expr_context),
    ):
        return AstDisposition.METADATA
    return AstDisposition.STRUCTURE


def concrete_ast_classes() -> tuple[type[ast.AST], ...]:
    """Return all concrete AST classes shipped by the active Python runtime."""

    pending = [ast.AST]
    found: set[type[ast.AST]] = set()
    while pending:
        parent = pending.pop()
        for child in parent.__subclasses__():
            if child not in found:
                found.add(child)
                pending.append(child)
    return tuple(sorted(found, key=lambda item: item.__name__))


@lru_cache(maxsize=1)
def assert_total_ast_classification() -> None:
    """Fail if the running Python exposes an unclassified concrete AST type."""

    for node_type in concrete_ast_classes():
        disposition = ast_disposition(node_type)
        if not isinstance(disposition, AstDisposition):
            raise ForestBuildError(f"unclassified AST class: {node_type.__name__}")


@dataclass(frozen=True)
class _SourceOccurrence:
    source_span: SourceSpan
    role: ParameterRole


@dataclass(frozen=True)
class _Component:
    name: str
    node: ast.AST | _SourceOccurrence
    origin_field: str
    origin_index: int
    suite: tuple[ast.stmt, ...] = ()

    @property
    def role(self) -> ParameterRole:
        """Return the structural role contributed by this component."""

        if self.suite:
            return ParameterRole.SUITE
        if isinstance(self.node, _SourceOccurrence):
            return self.node.role
        return _parameter_role(self.node)

    @property
    def child_nodes(self) -> tuple[ast.AST, ...]:
        """Return executable child nodes owned by this component."""

        if self.suite:
            return tuple(_statement_action_root(statement) for statement in self.suite)
        if isinstance(self.node, _SourceOccurrence):
            return ()
        return _parameter_action_roots(self.node)


@dataclass(frozen=True)
class _CompoundProtocol:
    fields: tuple[str, ...]
    suites: frozenset[str]


_IF_OR_WHILE_PROTOCOL = _CompoundProtocol(
    fields=("test", "body", "orelse"),
    suites=frozenset({"body", "orelse"}),
)
_FOR_PROTOCOL = _CompoundProtocol(
    fields=("target", "iter", "body", "orelse"),
    suites=frozenset({"body", "orelse"}),
)
_WITH_PROTOCOL = _CompoundProtocol(
    fields=("items", "body"),
    suites=frozenset({"body"}),
)
_TRY_PROTOCOL = _CompoundProtocol(
    fields=("body", "handlers", "orelse", "finalbody"),
    suites=frozenset({"body", "orelse", "finalbody"}),
)
_HANDLER_PROTOCOL = _CompoundProtocol(
    fields=("type", "name", "body"),
    suites=frozenset({"body"}),
)
_FUNCTION_PROTOCOL = _CompoundProtocol(
    fields=("name", "args", "body", "decorator_list", "returns"),
    suites=frozenset({"body"}),
)
_CLASS_PROTOCOL = _CompoundProtocol(
    fields=("name", "bases", "keywords", "body", "decorator_list"),
    suites=frozenset({"body"}),
)
_MATCH_PROTOCOL = _CompoundProtocol(
    fields=("subject", "cases"),
    suites=frozenset(),
)
_MATCH_CASE_PROTOCOL = _CompoundProtocol(
    fields=("pattern", "guard", "body"),
    suites=frozenset({"body"}),
)
_ARGUMENT_PROTOCOL = _CompoundProtocol(
    fields=("annotation",),
    suites=frozenset(),
)
_COMPOUND_PROTOCOLS: dict[type[ast.AST], _CompoundProtocol] = {
    ast.If: _IF_OR_WHILE_PROTOCOL,
    ast.While: _IF_OR_WHILE_PROTOCOL,
    ast.For: _FOR_PROTOCOL,
    ast.With: _WITH_PROTOCOL,
    ast.Try: _TRY_PROTOCOL,
    ast.TryStar: _TRY_PROTOCOL,
    ast.Match: _MATCH_PROTOCOL,
    ast.FunctionDef: _FUNCTION_PROTOCOL,
    ast.ClassDef: _CLASS_PROTOCOL,
}
_STRUCTURE_PROTOCOLS: dict[type[ast.AST], _CompoundProtocol] = {
    ast.ExceptHandler: _HANDLER_PROTOCOL,
    ast.match_case: _MATCH_CASE_PROTOCOL,
    ast.arg: _ARGUMENT_PROTOCOL,
}

_OWNER_PREFIX_FIELDS: dict[type[ast.AST], tuple[str, ...]] = {
    ast.FunctionDef: ("decorator_list",),
    ast.ClassDef: ("decorator_list",),
}
_UNSUPPORTED_NONEMPTY_FIELDS = {
    (ast.FunctionDef, "type_params"),
    (ast.ClassDef, "type_params"),
}


class _SourceIndex:
    def __init__(self, source: str) -> None:
        self.source = source
        self._lines = source.splitlines(keepends=True)
        self._line_is_ascii = tuple(line.isascii() for line in self._lines)
        self._line_offsets: list[int] = []
        self._spans: dict[ast.AST, SourceSpan] = {}
        self._tokens = tuple(tokenize.generate_tokens(StringIO(source).readline))
        offset = 0
        for line in self._lines:
            self._line_offsets.append(offset)
            offset += len(line)
        self._indexed_tokens = self._index_tokens()
        self._token_starts = tuple(
            span.start_offset for _token, span in self._indexed_tokens
        )

    def _index_tokens(self) -> _TokenSpans:
        """Span every token once, so no later lookup recomputes one.

        Tokens positioned past the last indexed line are the terminators
        `tokenize` appends; they have no source extent, so they are dropped here
        rather than guarded against at each use.
        """

        return tuple(
            (token, self._token_span(token))
            for token in self._tokens
            if token.start[0] <= len(self._lines) and token.end[0] <= len(self._lines)
        )

    def span(self, node: ast.AST) -> SourceSpan:
        """The one source extent of a node, decorators included.

        A decorated `def` or `class` owns its decorators: they are part of the
        statement's text, not of whatever contains it. This is the
        span authority. A second, decorator-excluding one used to exist for
        holes and suite extents, so a decorated definition inside any suite had
        its decorator both left in the template and returned by the child,
        duplicating it in the reconstruction.
        """

        cached = self._spans.get(node)
        if cached is not None:
            return cached
        if not _has_span(node):
            raise ForestBuildError(f"AST node {type(node).__name__} has no source span")
        result = self._prefixed_span(node, self._bare_span(node))
        self._spans[node] = result
        return result

    def _bare_span(self, node: ast.AST) -> SourceSpan:
        """The node's own extent, before its owned prefixes are folded in."""

        start = (node.lineno, node.col_offset)
        end = (node.end_lineno, node.end_col_offset)
        return SourceSpan(
            start_offset=self._offset(*start),
            end_offset=self._offset(*end),
            start=start,
            end=end,
        )

    def _prefixed_span(self, node: ast.AST, span: SourceSpan) -> SourceSpan:
        """Extend an extent backwards over the prefixes the node owns."""

        prefixes = tuple(
            item
            for field_name in _OWNER_PREFIX_FIELDS.get(type(node), ())
            for item in getattr(node, field_name, ())
            if isinstance(item, ast.AST)
        )
        if not prefixes:
            return span
        first = min(prefixes, key=_position)
        start = (first.lineno, node.col_offset)
        return SourceSpan(
            start_offset=self._offset(*start),
            end_offset=span.end_offset,
            start=start,
            end=span.end,
        )

    def text(self, span: SourceSpan) -> str:
        """Return the exact source text covered by one canonical span."""

        return self.source[span.start_offset : span.end_offset]

    def covering_span(self, nodes: tuple[ast.AST, ...]) -> SourceSpan:
        """Return the minimal span covering a nonempty ordered node sequence."""

        first = self.span(nodes[0])
        last = self.span(nodes[-1])
        return SourceSpan(
            start_offset=first.start_offset,
            end_offset=last.end_offset,
            start=first.start,
            end=last.end,
        )

    def marked_name(
        self,
        owner: ast.AST,
        marker: str,
        name: str,
        role: ParameterRole,
    ) -> _SourceOccurrence:
        """Locate the name a marker keyword introduces inside one owner.

        Reads pre-computed token spans over the owner's own window. The earlier
        version spanned every token in the file on each call, which made a forest
        build quadratic in its token count and, since the renderer builds a
        forest of each generated driver, dominated rendering entirely.
        """

        window = self._tokens_inside(self.span(owner))
        for (marker_token, _span), (target, target_span) in zip(
            window, window[1:], strict=False
        ):
            if (
                marker_token.type == tokenize.NAME
                and marker_token.string == marker
                and target.type == tokenize.NAME
                and target.string == name
            ):
                return _SourceOccurrence(source_span=target_span, role=role)
        raise ForestBuildError(f"{marker} target has no source token")

    def _tokens_inside(self, span: SourceSpan) -> _TokenSpans:
        """The indexed tokens contained in one span, located rather than scanned.

        Token starts ascend, so the candidate window is found by bisection. The
        containment test still runs, because a token starting inside the span may
        end outside it.
        """

        first = bisect.bisect_left(self._token_starts, span.start_offset)
        last = bisect.bisect_right(self._token_starts, span.end_offset)
        return tuple(
            row
            for row in self._indexed_tokens[first:last]
            if _span_is_inside(row[1], span)
        )

    def _token_span(self, token: tokenize.TokenInfo) -> SourceSpan:
        start_line, start_column = token.start
        end_line, end_column = token.end
        start_byte_column = len(
            self._lines[start_line - 1][:start_column].encode("utf-8")
        )
        end_byte_column = len(self._lines[end_line - 1][:end_column].encode("utf-8"))
        return SourceSpan(
            start_offset=self._offset(start_line, start_byte_column),
            end_offset=self._offset(end_line, end_byte_column),
            start=(start_line, start_byte_column),
            end=(end_line, end_byte_column),
        )

    def _offset(self, line_number: int, utf8_column: int) -> int:
        if self._line_is_ascii[line_number - 1]:
            return self._line_offsets[line_number - 1] + utf8_column
        line = self._lines[line_number - 1]
        prefix = line.encode("utf-8")[:utf8_column].decode("utf-8")
        return self._line_offsets[line_number - 1] + len(prefix)


class _Builder:
    def __init__(self, source: str) -> None:
        self.source = source
        self.index = _SourceIndex(source)
        self.module = ast.parse(source, type_comments=True)
        self.actions: list[Action] = []
        self.parameters: list[Parameter] = []
        self.action_nodes: dict[str, ast.AST] = {}
        self.components_by_action: dict[str, tuple[_Component, ...]] = {}

    def build(self) -> ActionForest:
        """Build and validate the forest from the module parsed at construction."""

        assert_total_ast_classification()
        for statement_number, statement in enumerate(self.module.body, start=1):
            root_node = _statement_action_root(statement)
            root_id = f"s{statement_number}"
            self._build_action(root_node, root_id, root=True)
        forest = ActionForest(
            source=self.source,
            actions=tuple(self.actions),
            parameters=tuple(self.parameters),
        )
        forest.validate()
        object.__setattr__(forest, "_source_builder", self)
        return forest

    def inventory(self) -> SourceInventory:
        """Project admission facts from the already parsed module."""

        action_by_node = {
            id(node): action_id for action_id, node in self.action_nodes.items()
        }
        statements = tuple(
            _source_statement(self.index, statement, index, action_by_node)
            for index, statement in enumerate(self.module.body, start=1)
        )
        occurrences = tuple(
            ImportOccurrence(action_id, bindings)
            for _offset, action_id, bindings in sorted(
                (
                    self.index.span(node).start_offset,
                    action_id,
                    import_bindings(node),
                )
                for action_id, node in self.action_nodes.items()
                if isinstance(node, (ast.Import, ast.ImportFrom))
            )
            if bindings
        )
        return SourceInventory(statements, occurrences)

    def _build_action(self, node: ast.AST, action_id: str, *, root: bool) -> str:
        disposition = ast_disposition(type(node))
        if (
            disposition is AstDisposition.COMPOUND
            and type(node) not in _COMPOUND_PROTOCOLS
        ):
            raise ForestBuildError(
                f"unsupported compound statement: {type(node).__name__}"
            )
        if disposition not in {AstDisposition.ACTION, AstDisposition.COMPOUND}:
            raise ForestBuildError(
                f"{type(node).__name__} cannot be an executable Action"
            )
        span = self.index.span(node)
        components = _components(node, self.index)
        self.action_nodes[action_id] = node
        self.components_by_action[action_id] = components
        parameter_ids: list[str] = []
        replacements: list[_ChildCut] = []
        for component_number, component in enumerate(components):
            name = _unique_component_name(components, component_number)
            parameter_id = f"{action_id}.{name}"
            parameter = self._build_parameter(
                replace(component, name=name),
                parameter_id,
                owner=action_id,
            )
            self.parameters.append(parameter)
            parameter_ids.append(parameter_id)
            replacements.append((parameter.source_span, name))
        source_text = self.index.text(span)
        cut = _replace_source(source_text, span, replacements)
        common_fields = {
            "id": action_id,
            "source_span": span,
            "source_text": source_text,
            "source_template": cut,
            "parameters": tuple(parameter_ids),
        }
        action: Action
        if root and isinstance(node, ast.stmt):
            action = Command(**common_fields)
        else:
            action = Expression(**common_fields)
        # Parents are stored before descendants for deterministic readable order.
        insertion = len(self.actions)
        self.actions.insert(insertion, action)
        return action_id

    def _build_parameter(
        self,
        component: _Component,
        parameter_id: str,
        *,
        owner: str,
    ) -> Parameter:
        span = _component_span(component, self.index)
        action_ids: list[str] = []
        replacements: list[_ChildCut] = []
        owner_node = self.action_nodes[owner]
        child_nodes = _parameter_action_children(component, owner_node)
        for child_number, child in enumerate(child_nodes):
            child_id = f"{parameter_id}.{child_number}"
            self._build_action(child, child_id, root=bool(component.suite))
            action_ids.append(child_id)
            replacements.append((self.index.span(child), str(child_number)))
        source_text = self.index.text(span)
        cut = _replace_source(source_text, span, replacements)
        return Parameter(
            id=parameter_id,
            owner=owner,
            name=component.name,
            role=component.role,
            source=_ParameterSource(
                source_span=span,
                source_text=source_text,
                source_template=cut,
                actions=tuple(action_ids),
            ),
        )


def build_forest(source: str) -> ActionForest:
    """Build and validate a source-exact ActionForest from one source string."""

    return _Builder(source).build()


def build_forest_with_inventory(source: str) -> BuiltSource:
    """Build source structure and admission facts without parsing twice."""

    builder = _Builder(source)
    forest = builder.build()
    return BuiltSource(forest, builder.inventory())


def _source_statement(
    index: _SourceIndex,
    statement: ast.stmt,
    number: int,
    action_by_node: Mapping[int, str],
) -> SourceStatement:
    reads, writes, in_place_writes, mutates = _statement_bindings(statement)
    calls, method_effects = _call_facts(statement, action_by_node)
    return SourceStatement(
        root_id=f"s{number}",
        kind=type(statement).__name__,
        span=index.span(statement),
        nested_kinds=tuple(
            sorted(
                {
                    type(node).__name__
                    for node in ast.walk(statement)
                    if node is not statement
                }
            )
        ),
        restrictions=_statement_restrictions(statement),
        reads=reads,
        writes=writes,
        in_place_writes=in_place_writes,
        mutates=mutates,
        imports=_imported_bindings(statement),
        import_bindings=import_bindings(statement),
        value_flows=_binding_value_flows(statement),
        calls=calls,
        method_effects=method_effects,
    )


def _statement_bindings(
    statement: ast.stmt,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    visitor = _BindingVisitor()
    visitor.visit(statement)
    reads = (visitor.reads - visitor.writes) | visitor.read_before_write
    return (
        tuple(sorted(reads | visitor.mutates)),
        tuple(sorted(visitor.writes)),
        tuple(sorted(visitor.read_before_write & visitor.writes)),
        tuple(sorted(visitor.mutates)),
    )


class _BindingVisitor(ast.NodeVisitor):
    """Project one opaque statement to conservative module binding facts."""

    def __init__(self) -> None:
        self.reads: set[str] = set()
        self.writes: set[str] = set()
        self.read_before_write: set[str] = set()
        self.mutates: set[str] = set()

    def visit_Name(self, node: ast.Name) -> None:
        """Record module-level reads, stores, and deletions."""

        if isinstance(node.ctx, ast.Load):
            self.reads.add(node.id)
        elif isinstance(node.ctx, (ast.Store, ast.Del)):
            self.writes.add(node.id)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        """Record augmented-assignment read-before-write and mutation semantics."""

        if isinstance(node.target, ast.Name):
            self.read_before_write.add(node.target.id)
            self.writes.add(node.target.id)
        else:
            self.visit(node.target)
            if (name := _target_base_name(node.target)) is not None:
                self.mutates.add(name)
        self.visit(node.value)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Record assignment value reads, writes, and non-name mutations."""

        self.visit(node.value)
        for target in node.targets:
            self.visit(target)
            if (
                not isinstance(target, (ast.Name, ast.Tuple, ast.List))
                and (name := _target_base_name(target)) is not None
            ):
                self.mutates.add(name)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Record annotated assignment dependencies and target mutation."""

        self.visit(node.target)
        self.visit(node.annotation)
        if node.value is not None:
            self.visit(node.value)
        if (
            not isinstance(node.target, ast.Name)
            and (name := _target_base_name(node.target)) is not None
        ):
            self.mutates.add(name)

    def visit_Call(self, node: ast.Call) -> None:
        """Record call reads; aliasing owns certified versus unknown effects."""

        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        """Record names established by an import statement."""

        self.writes.update(
            alias.asname or alias.name.partition(".")[0] for alias in node.names
        )

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Record non-star names established by a from-import statement."""

        self.writes.update(
            alias.asname or alias.name for alias in node.names if alias.name != "*"
        )

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        """Treat the conditionally bound and cleared exception alias as a write."""

        if node.name is not None:
            self.writes.add(node.name)
        if node.type is not None:
            self.visit(node.type)
        for statement in node.body:
            self.visit(statement)

    def visit_MatchAs(self, node: ast.MatchAs) -> None:
        """Record an ``as`` or bare pattern capture as a conditional write."""

        if node.name is not None:
            self.writes.add(node.name)
        if node.pattern is not None:
            self.visit(node.pattern)

    def visit_MatchStar(self, node: ast.MatchStar) -> None:
        """Record a starred sequence-pattern capture as a conditional write."""

        if node.name is not None:
            self.writes.add(node.name)

    def visit_MatchMapping(self, node: ast.MatchMapping) -> None:
        """Record a mapping-rest capture and visit nested expressions."""

        if node.rest is not None:
            self.writes.add(node.rest)
        for key in node.keys:
            self.visit(key)
        for pattern in node.patterns:
            self.visit(pattern)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Record a synchronous definition without executing its body."""

        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Record an asynchronous definition without executing its body."""

        self._visit_function(node)

    def _visit_function(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        self.writes.add(node.name)
        for expression in (
            *node.decorator_list,
            *node.args.defaults,
            *(value for value in node.args.kw_defaults if value is not None),
        ):
            self.visit(expression)
        if node.returns is not None:
            self.visit(node.returns)
        scope = _BindingVisitor()
        scope.writes.update(argument.arg for argument in _arguments(node.args))
        for statement in node.body:
            scope.visit(statement)
        self.reads.update((scope.reads - scope.writes) | scope.read_before_write)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Project only free reads from a dormant lambda body."""

        scope = _BindingVisitor()
        scope.writes.update(argument.arg for argument in _arguments(node.args))
        scope.visit(node.body)
        self.reads.update((scope.reads - scope.writes) | scope.read_before_write)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Record class creation and module reads from its executed definition."""

        self.writes.add(node.name)
        for expression in (*node.decorator_list, *node.bases):
            self.visit(expression)
        for keyword in node.keywords:
            self.visit(keyword.value)
        scope = _BindingVisitor()
        for statement in node.body:
            scope.visit(statement)
        self.reads.update((scope.reads - scope.writes) | scope.read_before_write)


def _arguments(arguments: ast.arguments) -> tuple[ast.arg, ...]:
    positional = (*arguments.posonlyargs, *arguments.args, *arguments.kwonlyargs)
    optional = tuple(
        argument
        for argument in (arguments.vararg, arguments.kwarg)
        if argument is not None
    )
    return (*positional, *optional)


def _target_base_name(node: ast.AST) -> str | None:
    current = node
    while isinstance(current, (ast.Attribute, ast.Subscript)):
        current = current.value
    return current.id if isinstance(current, ast.Name) else None


def _binding_value_flows(statement: ast.stmt) -> tuple[BindingValueFlow, ...]:
    """Project executed binding flows without changing statement granularity."""

    assignment = _outer_assignment(statement)
    if assignment is not None:
        return _assignment_value_flow(*assignment)
    if not isinstance(
        statement,
        (
            ast.AsyncFor,
            ast.AsyncWith,
            ast.For,
            ast.If,
            ast.Match,
            ast.Try,
            ast.TryStar,
            ast.While,
            ast.With,
        ),
    ):
        return ()
    visitor = _OpaqueStatementFlowVisitor()
    visitor.visit(statement)
    return tuple(visitor.flows)


def _assignment_value_flow(
    targets: tuple[ast.expr, ...],
    value: ast.expr,
) -> tuple[BindingValueFlow, ...]:
    """Describe one assignment using the closed points-to vocabulary."""

    names = tuple(target.id for target in targets if isinstance(target, ast.Name))
    if not names:
        return ()
    match value:
        case ast.Name(id=source):
            return (AliasValueFlow(names, source),)
        case ast.List() | ast.Tuple() | ast.Set() | ast.Dict():
            return (
                FreshValueFlow(
                    names,
                    f"builtins:{type(value).__name__.lower()}",
                    _loaded_names(value),
                ),
            )
        case ast.Subscript(value=container_node) if (
            container := _target_base_name(container_node)
        ) is not None:
            return (ContentLoadFlow(names, container),)
        case ast.Call(func=ast.Attribute(value=receiver_node, attr=method)) if (
            receiver := _target_base_name(receiver_node)
        ) is not None:
            attribute_call = _attribute_chain(value.func)
            if attribute_call is None:
                return (UnknownValueFlow(names, _loaded_names(value)),)
            receiver, attributes = attribute_call
            return (
                MethodCallFlow(
                    names,
                    receiver,
                    method,
                    attributes,
                    _call_inputs(value),
                ),
            )
        case _:
            return (UnknownValueFlow(names, _loaded_names(value)),)


class _OpaqueStatementFlowVisitor(ast.NodeVisitor):
    """Collect may-flows executed inside one opaque top-level statement."""

    def __init__(self) -> None:
        self.flows: list[BindingValueFlow] = []

    def visit_Assign(self, node: ast.Assign) -> None:
        """Collect a may-flow for an assignment executed by opaque control."""

        self.flows.extend(_assignment_value_flow(tuple(node.targets), node.value))
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Collect a may-flow for an executed annotated assignment."""

        if node.value is not None:
            self.flows.extend(_assignment_value_flow((node.target,), node.value))
            self.visit(node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        """Collect a may-flow for an executed assignment expression."""

        self.flows.extend(_assignment_value_flow((node.target,), node.value))
        self.visit(node.value)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Visit definition-time expressions while keeping the body dormant."""

        self._visit_definition_expressions(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Visit async definition-time expressions while keeping the body dormant."""

        self._visit_definition_expressions(node)

    def _visit_definition_expressions(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        for expression in (
            *node.decorator_list,
            *node.args.defaults,
            *(value for value in node.args.kw_defaults if value is not None),
        ):
            self.visit(expression)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Visit class bases and decorators without entering its local namespace."""

        for expression in (*node.decorator_list, *node.bases):
            self.visit(expression)
        for keyword in node.keywords:
            self.visit(keyword.value)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Visit lambda defaults without treating its dormant body as executed."""

        for expression in (
            *node.args.defaults,
            *(value for value in node.args.kw_defaults if value is not None),
        ):
            self.visit(expression)


def _outer_assignment(
    statement: ast.stmt,
) -> tuple[tuple[ast.expr, ...], ast.expr] | None:
    if isinstance(statement, ast.Assign):
        return tuple(statement.targets), statement.value
    if isinstance(statement, ast.AnnAssign) and statement.value is not None:
        return (statement.target,), statement.value
    return None


def _loaded_names(node: ast.AST) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                child.id
                for child in ast.walk(node)
                if isinstance(child, ast.Name) and isinstance(child.ctx, ast.Load)
            }
        )
    )


def _call_facts(
    statement: ast.stmt,
    action_by_node: Mapping[int, str],
) -> tuple[tuple[SourceCall, ...], tuple[MethodEffect, ...]]:
    """Collect call shape and method transfer facts in one source walk."""

    visitor = _CallFactVisitor(action_by_node)
    visitor.visit(statement)
    return tuple(visitor.calls), tuple(visitor.effects)


class _CallFactVisitor(ast.NodeVisitor):
    """Collect executed call facts while leaving function bodies dormant."""

    def __init__(self, action_by_node: Mapping[int, str]) -> None:
        self.action_by_node = action_by_node
        self.calls: list[SourceCall] = []
        self.effects: list[MethodEffect] = []

    def visit_Call(self, node: ast.Call) -> None:
        """Record one call using closed statement-level shape variants."""

        inputs = _call_inputs(node)
        try:
            action_id = self.action_by_node[id(node)]
        except KeyError as error:
            raise ForestBuildError("call fact has no source Action") from error
        call = _source_call(action_id, node.func, inputs)
        self.calls.append(call)
        if isinstance(call, ReceiverCall) and len(call.attributes) == 1:
            self.effects.append(MethodEffect(call.receiver, call.method, inputs))
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Inspect synchronous definition-time calls but not its body."""

        self._visit_function_definition(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Inspect asynchronous definition-time calls but not its body."""

        self._visit_function_definition(node)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Inspect lambda defaults while leaving its deferred body dormant."""

        for expression in (
            *node.args.defaults,
            *(value for value in node.args.kw_defaults if value is not None),
        ):
            self.visit(expression)

    def _visit_function_definition(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        for expression in (
            *node.decorator_list,
            *node.args.defaults,
            *(value for value in node.args.kw_defaults if value is not None),
        ):
            self.visit(expression)


def _call_inputs(node: ast.Call) -> CallInputs:
    """Project positional and keyword operands without tuple role conventions."""

    return CallInputs(
        tuple(CallInput(_loaded_names(argument)) for argument in node.args),
        tuple(CallInput(_loaded_names(keyword.value)) for keyword in node.keywords),
    )


def _source_call(
    action_id: str,
    function: ast.expr,
    inputs: CallInputs,
) -> SourceCall:
    """Classify one callable expression without resolving runtime identity."""

    if isinstance(function, ast.Name):
        return NamedCall(action_id, function.id, inputs)
    attribute_call = _attribute_chain(function)
    if attribute_call is not None:
        receiver, attributes = attribute_call
        return ReceiverCall(
            action_id,
            receiver,
            attributes[-1],
            attributes,
            inputs,
        )
    return DynamicCall(action_id, CallInput(_loaded_names(function)), inputs)


def _attribute_chain(node: ast.expr) -> tuple[str, tuple[str, ...]] | None:
    """Return a name-rooted attribute path without resolving its identity."""

    attributes = []
    current = node
    while isinstance(current, ast.Attribute):
        attributes.append(current.attr)
        current = current.value
    if not isinstance(current, ast.Name) or not attributes:
        return None
    return current.id, tuple(reversed(attributes))


def _imported_bindings(statement: ast.stmt) -> tuple[str, ...]:
    if isinstance(statement, ast.Import):
        return tuple(
            sorted(
                alias.asname or alias.name.partition(".")[0]
                for alias in statement.names
            )
        )
    if isinstance(statement, ast.ImportFrom):
        return tuple(
            sorted(
                alias.asname or alias.name
                for alias in statement.names
                if alias.name != "*"
            )
        )
    return ()


def import_bindings(statement: ast.stmt) -> tuple[ImportBinding, ...]:
    """Retain qualified import targets without resolving calls during parsing."""

    if isinstance(statement, ast.Import):
        return _plain_import_bindings(statement)
    if isinstance(statement, ast.ImportFrom) and statement.module is not None:
        return _from_import_bindings(statement)
    return ()


def _plain_import_bindings(statement: ast.Import) -> tuple[ImportBinding, ...]:
    bindings = {}
    for alias in statement.names:
        name = alias.asname or alias.name.partition(".")[0]
        qualified = alias.name if alias.asname else alias.name.partition(".")[0]
        bindings[name] = ImportBinding(name, qualified)
    return tuple(sorted(bindings.values()))


def _from_import_bindings(statement: ast.ImportFrom) -> tuple[ImportBinding, ...]:
    assert statement.module is not None
    bindings = {}
    for alias in statement.names:
        if alias.name != "*":
            name = alias.asname or alias.name
            bindings[name] = ImportBinding(name, f"{statement.module}.{alias.name}")
    return tuple(sorted(bindings.values()))


def _statement_restrictions(statement: ast.stmt) -> tuple[str, ...]:
    restrictions = set()
    if isinstance(statement, ast.ImportFrom) and statement.module == "__future__":
        restrictions.add("future-import")
    if isinstance(statement, ast.ImportFrom) and any(
        alias.name == "*" for alias in statement.names
    ):
        restrictions.add("wildcard-import")
    restricted_calls = {"eval", "exec", "globals", "locals"}
    restrictions.update(
        node.func.id
        for node in ast.walk(statement)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in restricted_calls
    )
    return tuple(sorted(restrictions))


def _statement_action_root(statement: ast.stmt) -> ast.AST:
    return statement.value if isinstance(statement, ast.Expr) else statement


def _component_span(component: _Component, source_index: _SourceIndex) -> SourceSpan:
    if component.suite:
        return source_index.covering_span(component.suite)
    if isinstance(component.node, _SourceOccurrence):
        return component.node.source_span
    return source_index.span(component.node)


def _component_position(component: _Component) -> tuple[int, int]:
    if isinstance(component.node, _SourceOccurrence):
        return component.node.source_span.start
    return _position(component.node)


type _FieldNode = ast.AST | _SourceOccurrence
_FIELD_NAME_MARKERS: dict[tuple[type[ast.AST], str], str] = {
    (ast.ExceptHandler, "name"): "as",
    (ast.FunctionDef, "name"): "def",
    (ast.ClassDef, "name"): "class",
}
_FIELD_ITEM_ATTRIBUTES: dict[tuple[type[ast.AST], str], str] = {
    (ast.ClassDef, "keywords"): "value",
}
_IMPLICIT_LOAD_FIELDS = frozenset({(ast.AugAssign, "target")})
type _DirectFieldContext = tuple[
    _SourceIndex | None,
    _CompoundProtocol | None,
    Mapping[str, str],
]
type _NestedFieldContext = tuple[
    _SourceIndex | None,
    str,
    int,
    _CompoundProtocol | None,
    Mapping[str, str],
]


def _field_nodes(
    node: ast.AST,
    field_name: str,
    value: object,
    source_index: _SourceIndex | None,
) -> tuple[_FieldNode, ...]:
    field_key = (type(node), field_name)
    marker = _FIELD_NAME_MARKERS.get(field_key)
    if marker is not None:
        return _marked_field_node(node, marker, value, source_index)
    item_attribute = _FIELD_ITEM_ATTRIBUTES.get(field_key)
    if item_attribute is not None and isinstance(value, list):
        return tuple(
            projected
            for item in value
            if isinstance(item, ast.AST)
            if isinstance((projected := getattr(item, item_attribute, None)), ast.AST)
        )
    if isinstance(value, list):
        return tuple(item for item in value if isinstance(item, ast.AST))
    return (value,) if isinstance(value, ast.AST) else ()


def _marked_field_node(
    node: ast.AST,
    marker: str,
    value: object,
    source_index: _SourceIndex | None,
) -> tuple[_FieldNode, ...]:
    if value is None:
        return ()
    if not isinstance(value, str):
        raise ForestBuildError("marked name field contains invalid data")
    if source_index is None:
        raise ForestBuildError("marked name field requires a source index")
    return (source_index.marked_name(node, marker, value, ParameterRole.TARGET),)


def _components(
    node: ast.AST,
    source_index: _SourceIndex | None = None,
) -> tuple[_Component, ...]:
    if isinstance(node, _ATOMIC_ACTIONS):
        return ()
    compound_protocol = _COMPOUND_PROTOCOLS.get(type(node))
    aliases = _FIELD_NAMES.get(type(node), {})
    components: list[_Component] = []
    for field_name, value in ast.iter_fields(node):
        components.extend(
            _direct_field_components(
                node,
                field_name,
                value,
                (source_index, compound_protocol, aliases),
            )
        )
    return tuple(sorted(components, key=_component_position))


def _direct_field_components(
    node: ast.AST,
    field_name: str,
    value: object,
    context: _DirectFieldContext,
) -> list[_Component]:
    source_index, protocol, aliases = context
    if field_name in {"ctx", "ops", "op", "type_comment", "simple", "level"}:
        return []
    if (type(node), field_name) in _UNSUPPORTED_NONEMPTY_FIELDS and value:
        raise ForestBuildError(
            f"unsupported non-empty field: {type(node).__name__}.{field_name}"
        )
    if protocol is not None and field_name not in protocol.fields:
        return []
    if protocol is not None and field_name in protocol.suites:
        statements = tuple(item for item in value if isinstance(item, ast.stmt))
        return (
            [
                _Component(
                    aliases.get(field_name, field_name),
                    statements[0],
                    field_name,
                    0,
                    suite=statements,
                )
            ]
            if statements
            else []
        )
    return _direct_field_items(node, field_name, value, source_index, aliases)


def _direct_field_items(
    node: ast.AST,
    field_name: str,
    value: object,
    source_index: _SourceIndex | None,
    aliases: Mapping[str, str],
) -> list[_Component]:
    components = []
    field_nodes = _field_nodes(node, field_name, value, source_index)
    for item_index, item in enumerate(field_nodes):
        base_name = aliases.get(field_name, field_name)
        component_name = _component_name(node, field_name, base_name, item_index, value)
        if isinstance(item, _SourceOccurrence) or (
            type(item) not in _STRUCTURE_PROTOCOLS and _has_span(item)
        ):
            components.append(_Component(component_name, item, field_name, item_index))
        else:
            components.extend(
                _span_components(
                    item,
                    prefix=component_name,
                    source_index=source_index,
                    origin_field=field_name,
                    origin_index=item_index,
                )
            )
    return components


def _component_name(
    node: ast.AST,
    field_name: str,
    base_name: str,
    item_index: int,
    field_value: object,
) -> str:
    is_list = isinstance(field_value, list)
    item_count = len(field_value) if is_list else 1
    if isinstance(node, ast.BoolOp) and field_name == "values" and item_count == 2:
        return ("left", "right")[item_index]
    if isinstance(node, ast.Call) and field_name == "args":
        return f"positional_{item_index}"
    if isinstance(node, ast.Call) and field_name == "keywords":
        return f"keyword_{item_index}"
    always_indexed = isinstance(
        node,
        (ast.List, ast.Tuple, ast.Set, ast.Dict, ast.Compare),
    )
    if is_list and (item_count > 1 or always_indexed):
        return f"{base_name}_{item_index}"
    return base_name


def _span_components(
    node: ast.AST,
    *,
    prefix: str,
    source_index: _SourceIndex | None,
    origin_field: str,
    origin_index: int,
) -> list[_Component]:
    node_protocol = _STRUCTURE_PROTOCOLS.get(type(node))
    aliases = _FIELD_NAMES.get(type(node), {})
    found: list[_Component] = []
    for field_name, value in ast.iter_fields(node):
        found.extend(
            _nested_field_components(
                node,
                field_name,
                value,
                prefix,
                (source_index, origin_field, origin_index, node_protocol, aliases),
            )
        )
    return found


def _nested_field_components(
    node: ast.AST,
    field_name: str,
    value: object,
    prefix: str,
    context: _NestedFieldContext,
) -> list[_Component]:
    source_index, origin_field, origin_index, protocol, aliases = context
    if field_name in {"ctx", "ops", "op", "type_comment"}:
        return []
    if protocol is not None and field_name not in protocol.fields:
        return []
    if protocol is not None and field_name in protocol.suites:
        statements = tuple(item for item in value if isinstance(item, ast.stmt))
        return (
            [
                _Component(
                    f"{prefix}_{aliases.get(field_name, field_name)}",
                    statements[0],
                    origin_field,
                    origin_index,
                    suite=statements,
                )
            ]
            if statements
            else []
        )
    return _nested_field_items(node, field_name, value, prefix, context)


def _nested_field_items(
    node: ast.AST,
    field_name: str,
    value: object,
    prefix: str,
    context: _NestedFieldContext,
) -> list[_Component]:
    source_index, origin_field, origin_index, _protocol, aliases = context
    found = []
    field_nodes = _field_nodes(node, field_name, value, source_index)
    for item_index, item in enumerate(field_nodes):
        base_name = aliases.get(field_name, field_name)
        suffix = f"{base_name}_{item_index}" if isinstance(value, list) else base_name
        name = f"{prefix}_{suffix}"
        if isinstance(item, _SourceOccurrence) or (
            type(item) not in _STRUCTURE_PROTOCOLS and _has_span(item)
        ):
            found.append(_Component(name, item, origin_field, origin_index))
        else:
            found.extend(
                _span_components(
                    item,
                    prefix=name,
                    source_index=source_index,
                    origin_field=origin_field,
                    origin_index=origin_index,
                )
            )
    return found


def _parameter_action_roots(node: _FieldNode) -> tuple[ast.AST, ...]:
    if isinstance(node, _SourceOccurrence):
        return ()
    if isinstance(node, ast.expr):
        if not isinstance(getattr(node, "ctx", ast.Load()), (ast.Store, ast.Del)):
            return (node,)
    roots: list[ast.AST] = []
    for component in _components(node):
        roots.extend(_parameter_action_roots(component.node))
    return tuple(_outermost_nonoverlapping(roots))


def _parameter_action_children(
    component: _Component, owner: ast.AST
) -> tuple[ast.AST, ...]:
    roots = component.child_nodes
    if not _component_has_implicit_load(component, owner):
        return roots
    implicit = tuple(
        ast.copy_location(ast.Name(id=item.id, ctx=ast.Load()), item)
        for item in ast.walk(component.node)
        if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Store)
    )
    return tuple(_outermost_nonoverlapping((*roots, *implicit)))


def _component_has_implicit_load(component: _Component, owner: ast.AST) -> bool:
    return (
        isinstance(component.node, ast.AST)
        and (type(owner), component.origin_field) in _IMPLICIT_LOAD_FIELDS
    )


def _parameter_role(node: ast.AST) -> ParameterRole:
    """Classify a source hole from Python's own expression context."""

    if isinstance(node, ast.pattern):
        return ParameterRole.TARGET
    context = getattr(node, "ctx", None)
    if isinstance(context, (ast.Store, ast.Del)):
        return ParameterRole.TARGET
    return ParameterRole.VALUE


def _outermost_nonoverlapping(nodes: Iterable[ast.AST]) -> list[ast.AST]:
    ordered = sorted(nodes, key=lambda item: (_position(item), -_span_size(item)))
    selected: list[ast.AST] = []
    for node in ordered:
        if any(_contains(existing, node) for existing in selected):
            continue
        selected.append(node)
    return selected


def _unique_component_name(components: tuple[_Component, ...], index: int) -> str:
    name = components[index].name
    duplicates = [item for item in components if item.name == name]
    if len(duplicates) == 1:
        return name
    occurrence = sum(1 for item in components[:index] if item.name == name)
    return f"{name}_{occurrence}"


def _sentinel(name: str) -> str:
    """The written stand-in for one removed child.

    This helper owns the sentinel's shape, and it is write-only: a
    hole is found by its recorded position, never by searching for this text.
    Its whole purpose is to keep a template readable, and to leave it
    byte-identical to its source when that source contains no braces.
    """

    return "{" + name + "}"


def _replace_source(
    source_text: str,
    container: SourceSpan,
    replacements: Iterable[_ChildCut],
) -> TemplateSource:
    """Cut every child span out of its owner, recording where each hole sits.

    The template is assembled left to right, so each recorded position is
    already a position in the finished template. Each child's name arrives with
    its span rather than being read back out of the sentinel: recovering a name
    by slicing the written text would leave the sentinel's shape load-bearing,
    which is the coupling positional holes exist to remove.
    """

    ordered = sorted(replacements, key=lambda item: item[0].start_offset)
    pieces: list[str] = []
    holes: list[TemplateHole] = []
    origins = _OriginTrail(container.start_line)
    written = 0
    cursor = container.start_offset
    for span, name in ordered:
        _require_contained(span, container, cursor)
        literal = source_text[
            cursor - container.start_offset : span.start_offset - container.start_offset
        ]
        written += len(literal)
        sentinel = _sentinel(name)
        pieces.extend((literal, sentinel))
        holes.append(TemplateHole(written, written + len(sentinel), name))
        origins.advance(literal, span.end_line - span.start_line)
        written += len(sentinel)
        cursor = span.end_offset
    trailing = source_text[cursor - container.start_offset :]
    pieces.append(trailing)
    origins.advance(trailing, 0)
    return TemplateSource("".join(pieces), tuple(holes), origins.recorded())


class _OriginTrail:
    """Track which source line each template line begins on, as one is built.

    A literal stretch copied from the owner advances the source line once per
    newline it carries, and each of those newlines also starts a template line.
    A removed child advances the source line by its own height while starting no
    template line at all, because its sentinel is one line. Keeping both in one
    place is what stops the two from being derived from each other later.
    """

    def __init__(self, first_line: int) -> None:
        self._source_line = first_line
        self._lines = [first_line]

    def advance(self, literal: str, child_height: int) -> None:
        """Consume one copied stretch, then the child that was cut out after it."""

        for _newline in range(literal.count("\n")):
            self._source_line += 1
            self._lines.append(self._source_line)
        self._source_line += child_height

    def recorded(self) -> SourceLineNumbers:
        """The source line every template line began on, in template order."""

        return tuple(self._lines)


def _require_contained(span: SourceSpan, container: SourceSpan, cursor: int) -> None:
    """A child must lie inside its owner and after every earlier sibling."""

    if (
        span.start_offset < container.start_offset
        or span.end_offset > container.end_offset
    ):
        raise ForestBuildError("child source span escapes its owner")
    if span.start_offset < cursor:
        raise ForestBuildError("overlapping source spans cannot form Parameters")


def _fill(template: str, holes: TemplateHoles, values: dict[str, str]) -> str:
    """Splice each child's source into its recorded hole, right to left.

    Filling by position rather than by pattern is what makes exact
    reconstruction sound: text a child contributes is never re-scanned, so a
    child whose own source contains `{0}` cannot capture a later fill, and no
    ordering heuristic is needed.
    """

    if not holes:
        return template
    result = template
    for hole in sorted(holes, reverse=True):
        result = result[: hole.start] + values[hole.name] + result[hole.end :]
    return result


def _validate_placeholders(
    holes: TemplateHoles,
    expected: set[str],
) -> None:
    """Every child owns exactly one hole, and no hole is unclaimed."""

    declared = [hole.name for hole in holes]
    if sorted(declared) != sorted(expected):
        raise ForestBuildError("template does not declare every child")


def _has_span(node: ast.AST) -> bool:
    return all(
        getattr(node, field, None) is not None
        for field in ("lineno", "col_offset", "end_lineno", "end_col_offset")
    )


def _span_is_inside(inner: SourceSpan, outer: SourceSpan) -> bool:
    return (
        inner.start_offset >= outer.start_offset
        and inner.end_offset <= outer.end_offset
    )


def _position(node: ast.AST) -> tuple[int, int, int, int]:
    return (
        getattr(node, "lineno", -1),
        getattr(node, "col_offset", -1),
        getattr(node, "end_lineno", -1),
        getattr(node, "end_col_offset", -1),
    )


def _span_size(node: ast.AST) -> int:
    return (
        (getattr(node, "end_lineno", 0) - getattr(node, "lineno", 0)) * 1_000_000
        + getattr(node, "end_col_offset", 0)
        - getattr(node, "col_offset", 0)
    )


def _contains(outer: ast.AST, inner: ast.AST) -> bool:
    return (
        _position(outer)[:2] <= _position(inner)[:2]
        and _position(outer)[2:] >= _position(inner)[2:]
    )
