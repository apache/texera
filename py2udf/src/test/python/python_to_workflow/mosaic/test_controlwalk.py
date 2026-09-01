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

"""Tests for control-flow traversal."""

import ast

from python_to_workflow.mosaic.analysis.controlwalk import (
    SELF,
    compile_controlwalk,
    controlwalk,
    controlwalk_region_result,
)
from python_to_workflow.mosaic.analysis.scopes import build_lexical_scope_index
from python_to_workflow.mosaic.forest import build_forest


def _section(payload, name):
    return dict(payload)[name]


def test_unreachable_definition_is_in_inventory_but_not_reaching() -> None:
    """Inventory and executable paths must remain separate authorities."""

    forest = build_forest(
        "try:\n"
        "    raise RuntimeError()\n"
        "    value = 1\n"
        "except RuntimeError:\n"
        "    print(value)\n"
    )

    def events(action):
        if action.id == "s1.body.1":
            return ((SELF, (), (("module", "value"),), ()),)
        if action.id == "s1.handler_body.0.positional_0.0":
            return ((SELF, (("module", "value"),), (), ()),)
        return ((SELF, (), (), ()),)

    payload = controlwalk(forest, events)

    assert ("s1.body.1", ("module", "value")) in _section(payload, "definitions")
    assert not any(
        producer == "s1.body.1" and identity == ("module", "value")
        for producer, _consumer, identity in _section(payload, "reaching")
    )


def test_region_inventory_excludes_deferred_lambda_body() -> None:
    """A containing lexical query must not inspect deferred lambda code."""

    forest = build_forest(
        "outside = 1\n"
        "def target(value):\n"
        "    delayed = lambda hidden: hidden + outside\n"
        "    return delayed\n"
        "result = target(outside)\n"
    )
    scopes = build_lexical_scope_index(forest)
    owner = next(
        action_id
        for action_id, node in forest._source_builder.action_nodes.items()
        if isinstance(node, ast.FunctionDef) and node.name == "target"
    )
    lambda_owner = next(
        action_id
        for action_id, node in forest._source_builder.action_nodes.items()
        if isinstance(node, ast.Lambda)
    )
    body = next(
        forest.parameter(parameter_id)
        for parameter_id in forest.action(lambda_owner).parameters
        if forest.parameter(parameter_id).name == "body"
    )
    deferred = frozenset(body.actions)
    scope = scopes.scopes[scopes.owner_scopes[owner]]
    visited = set()

    def events(action):
        visited.add(action.id)
        return ((SELF, (), (), ()),)

    controlwalk_region_result(
        forest,
        scope.roots,
        {},
        events,
        compile_controlwalk(forest),
    )

    assert deferred
    assert deferred.isdisjoint(visited)
