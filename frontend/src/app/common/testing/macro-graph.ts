/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { macroGraph } from "../../../../../common/workflow-core/src/test/typescript/macro-test-graph";
import {
  isMacroTerminal,
  MacroInterface,
  MacroExpansion,
  MacroDefinition,
  MacroPort,
  macroRuntimeID,
  macroSchema,
} from "../util/workflow-macro";
import type { WorkflowActionService } from "../../workspace/service/workflow-graph/model/workflow-action.service";
import { WorkflowContent } from "../type/workflow";
import { testField, testOperator, testWorkflow } from "./workflow-graph";

import { OperatorLink, OperatorPredicate } from "../../workspace/types/workflow-common.interface";
import { OperatorSchema } from "../../workspace/types/operator-schema.interface";

export const testPort = (
  portID: string,
  operatorID: string,
  internalPort = portID,
  displayName?: string
): MacroPort => ({
  portID,
  ...(displayName === undefined ? {} : { displayName }),
  targets: [{ operatorID, portID: internalPort }],
});

type MacroFixtureOptions = Partial<WorkflowContent> &
  Partial<Pick<MacroDefinition, "version" | "name" | "wid">> &
  Partial<Pick<MacroInterface, "inputs" | "outputs">>;
export function testMacro<C extends MacroFixtureOptions = {}>(
  id: string,
  operators: OperatorPredicate[],
  options?: C
): MacroDefinition & { content: WorkflowContent & Omit<C, "version" | "name" | "wid" | "inputs" | "outputs"> };
export function testMacro(
  id: string,
  operators: OperatorPredicate[],
  options: MacroFixtureOptions = {}
): MacroDefinition {
  const { version = 1, name = id, wid, inputs = [], outputs = [], ...body } = options;
  return {
    id,
    version,
    name,
    ...(wid === undefined ? {} : { wid }),
    content: macroGraph(
      testWorkflow(operators, body.links ?? [], { ...body, operators, macroInterface: { id, inputs, outputs } })
    ),
  };
}

/** Expose every native field through two saved macro levels for rendered Formly tests. */
export function testNestedMacroSchema(native: OperatorSchema, values = {}): OperatorSchema {
  let schema = native;
  for (const depth of [1, 2]) {
    const previous = schema;
    const inner = testOperator("inner", {
      operatorType: previous.operatorType,
      operatorProperties: depth === 1 ? values : {},
    });
    const fields = Object.keys(previous.jsonSchema.properties ?? {}).map((key, index) =>
      testField(inner.operatorID, key, `field-${depth}-${index}`, key)
    );
    const definition = testMacro(`macro-${depth}`, [inner], { version: depth, formBinding: { fields } });
    schema = macroSchema(definition, () => previous);
  }
  return schema;
}

/** Construct a backend response from explicit native leaves; never resolve or validate a macro graph. */
export function expansionFixture(
  content: WorkflowContent,
  leaves: [string[], OperatorPredicate][],
  links: OperatorLink[] = [],
  ports: MacroExpansion["ports"] = {}
): MacroExpansion & { inspectors: NonNullable<MacroExpansion["inspectors"]> } {
  const entries = leaves.map(([path, op]) => [macroRuntimeID(path.slice(0, -1), path.at(-1)!), path, op] as const);
  const inspectors: NonNullable<MacroExpansion["inspectors"]> = { "[]": { ids: {}, inputs: {}, terminals: [] } };
  for (const [runtime, path] of entries) {
    for (let depth = 0; depth < path.length; depth++) {
      const scope = path.slice(0, depth);
      const view = (inspectors[JSON.stringify(scope)] ??= { ids: {}, inputs: {}, terminals: [] });
      view.ids[path[depth]] = macroRuntimeID(scope, path[depth]);
      view.ids[macroRuntimeID(path.slice(depth, -1), path.at(-1)!)] = runtime;
    }
  }
  return structuredClone({
    inspectors,
    content: {
      ...content,
      operators: entries.map(([operatorID, , op]) => ({ ...op, operatorID })),
      links,
      macroInterface: undefined,
      macroDependencies: undefined,
    },
    paths: Object.fromEntries(entries.map(([id, path]) => [id, path])),
    ports,
  });
}

/** Build a test interface using the same normal graph operations as the editor. */
export function setMacroGraph(actions: WorkflowActionService, boundary: MacroInterface): void {
  const old = actions.getWorkflowContent();
  const terminalIDs = new Set(old.operators.filter(isMacroTerminal).map(op => op.operatorID));
  const graph = macroGraph({
    ...old,
    macroInterface: boundary,
  });
  const terminals = graph.operators.filter(isMacroTerminal);
  actions.bundleMacroActions(() => {
    actions.deleteOperatorsAndLinks([...terminalIDs]);
    actions.setMacroInterface({ id: boundary.id });
    actions.addOperatorsAndLinks(
      terminals.map(op => ({ op, pos: graph.operatorPositions[op.operatorID] })),
      graph.links.filter(link =>
        terminals.some(op => op.operatorID === link.source.operatorID || op.operatorID === link.target.operatorID)
      )
    );
  });
}
