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

import { cloneDeep, groupBy, isEqual, memoize, sumBy } from "lodash-es";
import { WorkflowContent, FormFieldBinding } from "../type/workflow";
import { OperatorPredicate } from "../../workspace/types/workflow-common.interface";
import { OperatorSchema } from "../../workspace/types/operator-schema.interface";
import { CustomJSONSchema7 } from "../../workspace/types/custom-json-schema.interface";
import { OperatorRuntimeStatus, OperatorState } from "../../workspace/types/execute-workflow.interface";
import { assert } from "./assert";

export { isMacro, macroType, portKey, macroInstance } from "@texera/workflow-macros";
export type { MacroInterface, MacroPort } from "@texera/workflow-macros";
import { isMacro, macroType, portKey, macroInstance, createMacroRuntime } from "@texera/workflow-macros";
import { v5 as uuid } from "uuid";
import type { MacroDefinition as Definition, MacroExpansion as Expansion, MacroPort } from "@texera/workflow-macros";
const macros = createMacroRuntime(uuid, isEqual);
export const { expandMacros, macroRuntimeID, macroResultAliases, mapMacroCompilation } = macros;
export type MacroDefinition = Definition<WorkflowContent>;
export type MacroExpansion = Expansion<WorkflowContent>;
export const mergeMacroDefinitions = macros.mergeMacroDefinitions<WorkflowContent>;
const fields = (content?: WorkflowContent): FormFieldBinding[] => content?.formBinding?.fields ?? [];

/** Capture a convex selection; re-entering it through unselected nodes would create a macro cycle. */
export function extractMacro(content: WorkflowContent, selected: readonly string[], id: string): WorkflowContent {
  assert(selected.length > 0, "Select at least one operator");
  const ids = new Set(selected);
  assert(
    selected.every(id => content.operators.some(o => o.operatorID === id)),
    "Missing selected operator"
  );
  const escaped = new Set<string>();
  const follow = (from: string): void => {
    assert(!ids.has(from), "Selection must not leave and re-enter the macro");
    if (escaped.has(from)) return;
    escaped.add(from);
    content.links.filter(l => l.source.operatorID === from).forEach(l => follow(l.target.operatorID));
  };
  content.links
    .filter(l => ids.has(l.source.operatorID) && !ids.has(l.target.operatorID))
    .forEach(l => follow(l.target.operatorID));
  const operators = content.operators.filter(o => ids.has(o.operatorID));
  const links = content.links.filter(l => ids.has(l.source.operatorID) && ids.has(l.target.operatorID));
  const boundary = (direction: "inputs" | "outputs"): MacroPort[] => {
    const connections = groupBy(content.links, link => {
      const end = direction === "inputs" ? link.target : link.source;
      return portKey(end.operatorID, direction, end.portID);
    });
    return operators
      .flatMap(op =>
        (direction === "inputs" ? op.inputPorts : op.outputPorts)
          .filter(p => {
            const edges = connections[portKey(op.operatorID, direction, p.portID)] ?? [];
            return (
              !edges.length || edges.some(l => !ids.has((direction === "inputs" ? l.source : l.target).operatorID))
            );
          })
          .map(p => ({ ...p, targets: [{ operatorID: op.operatorID, portID: p.portID }] }))
      )
      .map((p, i) => ({ ...p, portID: `${direction === "inputs" ? "input" : "output"}-${i}` }));
  };
  return cloneDeep({
    ...content,
    ...(content.macroDependencies ? { macroDependencies: mergeMacroDefinitions(content.macroDependencies) } : {}),
    operators,
    links,
    operatorPositions: Object.fromEntries(operators.map(o => [o.operatorID, content.operatorPositions[o.operatorID]])),
    commentBoxes: [],
    macroInterface: { id, inputs: boundary("inputs"), outputs: boundary("outputs") },
    formBinding: { fields: fields(content).filter(f => ids.has(f.operatorID)) },
  });
}

export function adoptMacroVersion(
  op: OperatorPredicate,
  previous: MacroDefinition,
  next: MacroDefinition
): OperatorPredicate {
  assert(previous.id === next.id && op.operatorType === macroType(previous), "Different macro definition");
  const signature = (d: MacroDefinition) => [
    d.content.macroInterface!.inputs.map(p => p.portID).sort(),
    d.content.macroInterface!.outputs.map(p => p.portID).sort(),
    fields(d.content)
      .map(f => f.id)
      .sort(),
  ];
  assert(
    isEqual(signature(previous), signature(next)),
    "Macro interface changed; insert the new version and reconnect explicitly"
  );
  return {
    ...macroInstance(next, op.operatorID),
    operatorProperties: cloneDeep(op.operatorProperties),
    customDisplayName: op.customDisplayName,
    isDisabled: op.isDisabled,
    viewResult: op.viewResult,
    markedForReuse: op.markedForReuse,
  };
}

/** Exposed Form View fields become the macro's ordinary operator properties, including nested macros. */
export function macroSchema(d: MacroDefinition, getSchema: (type: string) => OperatorSchema): OperatorSchema {
  const schemaFor = memoize((type: string) => getSchema(type).jsonSchema);
  const properties: Record<string, CustomJSONSchema7> = Object.create(null);
  const resolve = (value: any, root: CustomJSONSchema7, op: OperatorPredicate, refs: string[] = []): any => {
    if (Array.isArray(value)) return value.map(v => resolve(v, root, op, refs));
    if (!value || typeof value !== "object") return value;
    if (value.$ref) {
      const { $ref, ...rest } = value;
      assert($ref.startsWith("#/") && !refs.includes($ref), "Missing or recursive exposed property schema reference");
      const target = $ref
        .slice(2)
        .split("/")
        .reduce((node: any, key: string) => node?.[key.replace(/~1/g, "/").replace(/~0/g, "~")], root);
      assert(target, "Missing exposed property schema reference");
      return resolve({ ...target, ...rest }, root, op, [...refs, $ref]);
    }
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => key !== "$id")
        .map(([key, v]) => [
          key,
          key === "autofillAttributeOnPort" && Number.isInteger(v)
            ? d.content.macroInterface!.inputs.findIndex(p =>
                p.targets.some(t => t.operatorID === op.operatorID && t.portID === op.inputPorts[v as number]?.portID)
              )
            : resolve(v, root, op, refs),
        ])
    );
  };
  for (const f of fields(d.content)) {
    const op = d.content.operators.find(o => o.operatorID === f.operatorID);
    assert(op, "Missing exposed property operator");
    const root = schemaFor(op.operatorType);
    const schema = root.properties?.[f.propertyKey];
    assert(schema && typeof schema !== "boolean", "Missing exposed property schema");
    properties[f.id] = {
      ...resolve(schema, root, op),
      title: f.displayName,
      description: f.helpText,
      default: cloneDeep(
        Object.hasOwn(op.operatorProperties, f.propertyKey) ? op.operatorProperties[f.propertyKey] : schema.default
      ),
    };
  }
  return {
    operatorType: macroType(d),
    operatorVersion: String(d.version),
    jsonSchema: { type: "object", properties, additionalProperties: false },
    additionalMetadata: {
      userFriendlyName: `${d.name} (v${d.version})`,
      operatorGroupName: "Macros",
      operatorDescription: "Reusable macro. Double-click to inspect its pinned definition.",
      inputPorts: d.content.macroInterface!.inputs.map(p => ({ displayName: p.displayName })),
      outputPorts: d.content.macroInterface!.outputs.map(p => ({ displayName: p.displayName })),
    },
  };
}

/** Count the external boundaries; summing every internal edge would multiply the row counts. */
export function rollupMacroStatus(
  content: WorkflowContent,
  status: Record<string, OperatorRuntimeStatus>,
  expanded = expandMacros(content)
): Record<string, OperatorRuntimeStatus> {
  const result = { ...status };
  for (const op of content.operators.filter(isMacro)) {
    const leaves = expanded.content.operators.filter(o => expanded.paths[o.operatorID]?.[0] === op.operatorID);
    const updates = leaves.map(o => status[o.operatorID]).filter(Boolean);
    if (!updates.length) continue;
    const boundary = (direction: "inputs" | "outputs") => {
      const values = (direction === "inputs" ? op.inputPorts : op.outputPorts).map(p => {
        const targets = (expanded.ports[portKey(op.operatorID, direction, p.portID)] ?? []).map(target => {
          const leaf = leaves.find(o => o.operatorID === target.operatorID)!;
          const ports = direction === "inputs" ? leaf.inputPorts : leaf.outputPorts;
          const update = status[target.operatorID];
          const metrics = direction === "inputs" ? update?.inputPortMetrics : update?.outputPortMetrics;
          return {
            rows:
              metrics !== undefined
                ? metrics[String(ports.findIndex(p => p.portID === target.portID))] ?? 0
                : ports.length === 1
                  ? (direction === "inputs" ? update?.aggregatedInputRowCount : update?.aggregatedOutputRowCount) ?? 0
                  : 0,
            // The wire format cannot split a leaf's aggregate bytes across multiple ports.
            size:
              ports.length === 1
                ? direction === "inputs"
                  ? update?.aggregatedInputSize
                  : update?.aggregatedOutputSize
                : undefined,
          };
        });
        return {
          rows: Math.max(0, ...targets.map(t => t.rows)),
          size: targets.length === 1 ? targets[0].size : undefined,
        };
      });
      return {
        ports: Object.fromEntries(values.map((value, index) => [String(index), value.rows])),
        rows: sumBy(values, value => value.rows),
        size: values.every(value => value.size !== undefined) ? sumBy(values, value => value.size ?? 0) : undefined,
      };
    };
    const inputs = boundary("inputs"),
      outputs = boundary("outputs");
    const priority = [
      OperatorState.Recovering,
      OperatorState.Pausing,
      OperatorState.Paused,
      OperatorState.Resuming,
      OperatorState.Running,
      OperatorState.Initializing,
      OperatorState.Ready,
      OperatorState.Uninitialized,
    ];
    result[op.operatorID] = {
      operatorState:
        updates.length === leaves.length && updates.every(s => s.operatorState === OperatorState.Completed)
          ? OperatorState.Completed
          : priority.find(state => updates.some(s => s.operatorState === state)) ?? OperatorState.Uninitialized,
      aggregatedInputRowCount: inputs.rows,
      aggregatedInputSize: inputs.size,
      inputPortMetrics: inputs.ports,
      aggregatedOutputRowCount: outputs.rows,
      aggregatedOutputSize: outputs.size,
      outputPortMetrics: outputs.ports,
      numWorkers: sumBy(updates, s => s.numWorkers ?? 0),
      aggregatedDataProcessingTime: sumBy(updates, s => s.aggregatedDataProcessingTime ?? 0),
      aggregatedControlProcessingTime: sumBy(updates, s => s.aggregatedControlProcessingTime ?? 0),
      aggregatedIdleTime: sumBy(updates, s => s.aggregatedIdleTime ?? 0),
      reusedFromCache: updates.length === leaves.length && updates.every(s => s.reusedFromCache === true),
    };
  }
  return result;
}
