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

"""Single-parse source boundary for the modular compiler."""

from __future__ import annotations

from dataclasses import dataclass

from python_to_workflow.mosaic.forest import (
    AliasValueFlow,
    BindingValueFlow,
    BuiltSource,
    CallInput,
    CallInputs,
    ContentLoadFlow,
    DynamicCall,
    FreshValueFlow,
    ImportBinding,
    ImportOccurrence,
    MethodCallFlow,
    MethodEffect,
    NamedCall,
    ReceiverCall,
    SourceCall,
    SourceInventory,
    SourceStatement,
    UnknownValueFlow,
    build_forest_with_inventory,
)


@dataclass(frozen=True)
class GeneratedSymbols:
    """Fresh Python identifiers owned by one generated workflow program."""

    workflow_module: str
    runtime: str
    heap: str
    boundary: str
    driver: str
    operator: str

    def __post_init__(self) -> None:
        names = (
            self.workflow_module,
            self.runtime,
            self.heap,
            self.boundary,
            self.driver,
            self.operator,
        )
        if any(not name.isidentifier() for name in names):
            raise ValueError("generated symbols must be Python identifiers")
        if len(set(names)) != len(names):
            raise ValueError("generated symbols must be distinct")


@dataclass(frozen=True)
class ParsedSource:
    """One source parse plus the generated names fresh for that source."""

    built: BuiltSource
    symbols: GeneratedSymbols

    @property
    def forest(self):
        """Return the source-exact ActionForest from the parsed source."""

        return self.built.forest

    @property
    def inventory(self) -> SourceInventory:
        """Return the source inventory from the parsed source."""

        return self.built.inventory

    def statement(self, root_id: str) -> SourceStatement:
        """Resolve one top-level statement through the source-owned index."""

        return self.built.statement(root_id)


def parse_source(source: str, /) -> ParsedSource:
    """Build the source-exact forest and admission inventory in one parse."""

    built = build_forest_with_inventory(source)
    return ParsedSource(built, _generated_symbols(built.inventory))


def _generated_symbols(inventory: SourceInventory) -> GeneratedSymbols:
    """Allocate deterministic generated identifiers outside the source namespace."""

    reserved = {
        name
        for statement in inventory.statements
        for name in (
            *statement.reads,
            *statement.writes,
            *statement.in_place_writes,
            *statement.mutates,
            *statement.imports,
            *(binding.name for binding in statement.import_bindings),
        )
    }
    allocated: set[str] = set()

    def fresh(role: str) -> str:
        index = 0
        while True:
            candidate = f"_mosaic_{role}_{index}"
            if candidate not in reserved and candidate not in allocated:
                allocated.add(candidate)
                return candidate
            index += 1

    return GeneratedSymbols(
        fresh("workflow_module"),
        fresh("runtime"),
        fresh("heap"),
        fresh("boundary"),
        fresh("driver"),
        fresh("operator"),
    )


__all__ = [
    "AliasValueFlow",
    "BindingValueFlow",
    "CallInput",
    "CallInputs",
    "ContentLoadFlow",
    "DynamicCall",
    "FreshValueFlow",
    "ImportBinding",
    "ImportOccurrence",
    "MethodCallFlow",
    "MethodEffect",
    "NamedCall",
    "GeneratedSymbols",
    "ParsedSource",
    "SourceInventory",
    "ReceiverCall",
    "SourceCall",
    "SourceStatement",
    "UnknownValueFlow",
    "parse_source",
]
