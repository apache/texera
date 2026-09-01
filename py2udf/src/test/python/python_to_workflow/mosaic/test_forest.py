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

import ast

import pytest
from python_to_workflow.mosaic.forest import NameAccess
from python_to_workflow.mosaic.source import (
    DynamicCall,
    ImportBinding,
    ImportOccurrence,
    NamedCall,
    ReceiverCall,
    parse_source,
)


def test_source_builds_forest_and_inventory_from_one_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    original = ast.parse

    def counted_parse(*args: object, **kwargs: object) -> ast.Module:
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ast, "parse", counted_parse)

    parsed = parse_source("left = 1\nright = left + 1\n")
    parsed.forest.validate()

    assert calls == 1
    assert len(parsed.forest.roots) == len(parsed.inventory.statements) == 2


def test_module_occurrences_exclude_comprehension_locals_but_keep_first_iter() -> None:
    parsed = parse_source("values = [x for x in x]\n")

    occurrences = parsed.forest.module_name_occurrences(
        parsed.forest.roots[0],
        frozenset({"x"}),
    )

    assert tuple(row.access for row in occurrences) == (NameAccess.LOAD,)
    assert tuple(
        parsed.forest.source[row.span.start_offset : row.span.end_offset]
        for row in occurrences
    ) == ("x",)


def test_forest_reports_only_module_activation_binders() -> None:
    parsed = parse_source(
        "try:\n"
        "    pass\n"
        "except RuntimeError as error:\n"
        "    pass\n"
        "match value:\n"
        "    case {'item': captured, **rest}:\n"
        "        pass\n"
    )

    first, second = parsed.forest.roots

    assert parsed.forest.python_activation_bindings(first) == frozenset({"error"})
    assert parsed.forest.python_activation_bindings(second) == frozenset(
        {"captured", "rest"}
    )


def test_generated_scaffold_symbols_are_all_fresh_from_the_source_inventory() -> None:
    """No composer or executor identifier may live outside symbol authority."""

    source = "\n".join(
        f"_mosaic_{role}_0 = {index}"
        for index, role in enumerate(
            (
                "workflow_module",
                "runtime",
                "heap",
                "boundary",
                "driver",
                "operator",
            )
        )
    )

    symbols = parse_source(source + "\n").symbols

    assert (
        symbols.workflow_module,
        symbols.runtime,
        symbols.heap,
        symbols.boundary,
        symbols.driver,
        symbols.operator,
    ) == tuple(
        f"_mosaic_{role}_1"
        for role in (
            "workflow_module",
            "runtime",
            "heap",
            "boundary",
            "driver",
            "operator",
        )
    )


def test_inventory_exposes_closed_call_shapes_from_the_same_parse() -> None:
    """Call evidence is typed analysis input, never renderer inference."""

    parsed = parse_source(
        "print(value)\nitems.copy()\nregistry['runner'](value, mode=option)\n"
    )

    direct, receiver, dynamic = (
        statement.calls[0] for statement in parsed.inventory.statements
    )

    assert isinstance(direct, NamedCall)
    assert direct.action_id == "s1"
    assert direct.name == "print"
    assert direct.inputs.bindings() == ("value",)
    assert isinstance(receiver, ReceiverCall)
    assert receiver.action_id == "s2"
    assert (receiver.receiver, receiver.method) == ("items", "copy")
    assert isinstance(dynamic, DynamicCall)
    assert dynamic.action_id == "s3"
    assert dynamic.callable.bindings == ("registry",)
    assert dynamic.inputs.bindings() == ("option", "value")


def test_inventory_retains_import_targets_and_attribute_call_paths() -> None:
    """Effect resolution uses typed import and call facts from parsing."""

    parsed = parse_source(
        "import random as rng\nimport numpy as np\nrng.seed(7)\nnp.random.seed(8)\n"
    )

    first, second, seed, numpy_seed = parsed.inventory.statements
    assert first.import_bindings == (ImportBinding("rng", "random"),)
    assert second.import_bindings == (ImportBinding("np", "numpy"),)
    assert isinstance(seed.calls[0], ReceiverCall)
    assert seed.calls[0].attributes == ("seed",)
    assert isinstance(numpy_seed.calls[0], ReceiverCall)
    assert numpy_seed.calls[0].attributes == ("random", "seed")


def test_forest_indexes_outer_roots_once_for_all_consumers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated root lookups consume the forest index, not ancestry walks."""

    forest = parse_source(
        "if condition:\n"
        "    result = transform(source)\n"
        "else:\n"
        "    result = fallback(source)\n"
    ).forest
    expected = {action.id: forest.root_of(action.id) for action in forest.actions}

    def unexpected_walk(_forest: object, _action_id: str) -> str | None:
        raise AssertionError("root lookup repeated the structural ancestry walk")

    monkeypatch.setattr(type(forest), "parent_action", unexpected_walk)

    actual = {action.id: forest.root_of(action.id) for action in forest.actions}
    assert actual == expected


def test_parsed_source_indexes_statement_summaries_once() -> None:
    """Realizations resolve source facts without rescanning all statements."""

    parsed = parse_source("left = 1\nright = left + 1\n")
    expected = tuple(parsed.statement(root) for root in parsed.forest.roots)

    class _NoIteration(tuple):
        def __iter__(self):
            raise AssertionError("statement lookup rescanned the inventory")

    object.__setattr__(
        parsed.inventory,
        "statements",
        _NoIteration(parsed.inventory.statements),
    )

    assert tuple(parsed.statement(root) for root in parsed.forest.roots) == expected


def test_inventory_owns_import_occurrences_for_nested_and_from_imports() -> None:
    """Resolution consumes typed per-Action imports, never forest internals."""

    parsed = parse_source(
        "if condition:\n    import random as rng\nfrom numpy import random as nr\n"
    )

    assert parsed.inventory.import_occurrences == (
        ImportOccurrence("s1.body.0", (ImportBinding("rng", "random"),)),
        ImportOccurrence("s2", (ImportBinding("nr", "numpy.random"),)),
    )


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        (
            "import random as rng, numpy.random as rng\n",
            ImportBinding("rng", "numpy.random"),
        ),
        (
            "import numpy.random as rng, random as rng\n",
            ImportBinding("rng", "random"),
        ),
    ),
)
def test_duplicate_import_aliases_preserve_python_last_wins(
    source: str,
    expected: ImportBinding,
) -> None:
    """Canonicalization happens after source-order binding semantics."""

    parsed = parse_source(source)

    assert parsed.inventory.statements[0].import_bindings == (expected,)
    assert parsed.inventory.import_occurrences == (ImportOccurrence("s1", (expected,)),)


def test_lambda_body_calls_are_dormant_but_default_calls_execute() -> None:
    """Call facts follow Python definition-time execution, not raw AST walk."""

    parsed = parse_source("callback = lambda value=seed(): hidden(value)\n")
    calls = parsed.inventory.statements[0].calls

    assert len(calls) == 1
    assert isinstance(calls[0], NamedCall)
    assert calls[0].name == "seed"


def test_parse_source_rejects_malformed_python() -> None:
    with pytest.raises(SyntaxError):
        parse_source("value =\n")
