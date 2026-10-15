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

import { v5 as uuid } from "uuid";
import { cloneDeep, groupBy, isEqual, sumBy } from "lodash-es";
import { WorkflowContent, FormFieldBinding } from "../type/workflow";
import { LogicalPort, OperatorPredicate, PortDescription } from "../../workspace/types/workflow-common.interface";
import { OperatorSchema } from "../../workspace/types/operator-schema.interface";
import { CustomJSONSchema7 } from "../../workspace/types/custom-json-schema.interface";
import { OperatorRuntimeStatus, OperatorState } from "../../workspace/types/execute-workflow.interface";

export interface MacroPort extends PortDescription {
  targets: LogicalPort[];
}
export interface MacroInterface {
  /** Stable across definition versions and deployments. */
  id: string;
  inputs: MacroPort[];
  outputs: MacroPort[];
}
export interface MacroDefinition {
  id: string;
  version: number;
  name: string;
  /** Source workflow, used only when this deployment's library has the same definition id. */
  wid?: number;
  content: WorkflowContent;
}
export const isMacro = (op: OperatorPredicate): boolean => op.operatorType.startsWith("WorkflowMacro:");
export const macroType = (d: MacroDefinition): string => `WorkflowMacro:${d.id}:${d.version}`;
export const portKey = (id: string, direction: "inputs" | "outputs", port: string): string =>
  JSON.stringify([id, direction, port]);
const require = (condition: unknown, message: string): void => {
  if (!condition) throw new Error(message);
};
const fields = (content?: WorkflowContent): FormFieldBinding[] => content?.formBinding?.fields ?? [];
const namespace = "33269d11-7448-5d6d-9aeb-68826068e94d";
export const macroRuntimeID = (chain: string[], id: string): string =>
  chain.length ? uuid(JSON.stringify([...chain, id]), namespace) : id;

export function macroInstance(d: MacroDefinition, operatorID: string): OperatorPredicate {
  require(d.content?.macroInterface, "Missing macro interface");
  return {
    operatorID,
    operatorType: macroType(d),
    operatorVersion: String(d.version),
    operatorProperties: {},
    inputPorts: d.content.macroInterface!.inputs.map(({ targets, ...p }) => p),
    outputPorts: d.content.macroInterface!.outputs.map(({ targets, ...p }) => p),
    showAdvanced: false,
    customDisplayName: d.name,
  };
}

export interface MacroExpansion {
  content: WorkflowContent;
  /** Runtime id -> instance chain followed by the definition's leaf id. */
  paths: Record<string, string[]>;
  /** Canvas boundary -> runtime ports, for result and schema lookup. */
  ports: Record<string, LogicalPort[]>;
}

/** One immutable snapshot per identity/version, shared by imports, libraries and clipboard operations. */
export function mergeMacroDefinitions(...bundles: MacroDefinition[][]): MacroDefinition[] {
  const definitions = new Map<string, MacroDefinition>();
  for (const d of bundles.flat()) {
    require(d &&
      typeof d.id === "string" &&
      typeof d.name === "string" &&
      d.id.length &&
      Number.isSafeInteger(d.version) &&
      d.version > 0 &&
      d.content?.macroInterface?.id === d.id, "Malformed macro definition");
    const previous = definitions.get(macroType(d));
    require(!previous || isEqual(previous.content, d.content), "Conflicting macro version");
    definitions.set(macroType(d), d);
  }
  return [...definitions.values()];
}

/** A pure, deterministic expansion used by both compilers and executions. Definitions stay immutable. */
export function expandMacros(content: WorkflowContent): MacroExpansion {
  const definitions = new Map(mergeMacroDefinitions(content.macroDependencies ?? []).map(d => [macroType(d), d]));
  const paths: Record<string, string[]> = Object.create(null);
  const ports: Record<string, LogicalPort[]> = Object.create(null);
  const operators: OperatorPredicate[] = [];
  const links: WorkflowContent["links"] = [];

  const visit = (body: WorkflowContent, chain: string[], stack: string[]): Map<string, LogicalPort[]> => {
    require(body && Array.isArray(body.operators) && Array.isArray(body.links), "Malformed macro graph");
    require(chain.length <= 32, "Macro nesting exceeds 32 levels");
    const ops = new Map<string, OperatorPredicate>();
    for (const op of body.operators) {
      require(op &&
        typeof op.operatorID === "string" &&
        op.operatorID.length &&
        !ops.has(op.operatorID), "Duplicate or invalid operator ID");
      require(typeof op.operatorType === "string" &&
        Array.isArray(op.inputPorts) &&
        Array.isArray(op.outputPorts) &&
        op.operatorProperties &&
        typeof op.operatorProperties === "object" &&
        !Array.isArray(op.operatorProperties), "Malformed operator");
      for (const group of [op.inputPorts, op.outputPorts]) {
        require(group.every(p => p && typeof p.portID === "string" && p.portID.length) &&
          new Set(group.map(p => p.portID)).size === group.length, "Duplicate or invalid operator port");
      }
      ops.set(op.operatorID, op);
    }
    const resolved = new Map<string, LogicalPort[]>();
    const endpoint = (p: LogicalPort, direction: "inputs" | "outputs"): LogicalPort[] => {
      const op = ops.get(p?.operatorID);
      require(op &&
        (direction === "inputs" ? op.inputPorts : op.outputPorts).some(
          port => port.portID === p.portID
        ), "Missing operator or port in macro graph");
      if (op!.isDisabled) return [];
      const result = resolved.get(portKey(p.operatorID, direction, p.portID));
      require(result, "Missing exposed macro port");
      return result!;
    };
    for (const op of body.operators.filter(op => !op.isDisabled)) {
      if (!isMacro(op)) {
        const id = macroRuntimeID(chain, op.operatorID);
        require(!Object.hasOwn(paths, id), "Duplicate runtime operator ID");
        operators.push({ ...cloneDeep(op), operatorID: id });
        paths[id] = [...chain, op.operatorID];
        for (const direction of ["inputs", "outputs"] as const) {
          for (const p of direction === "inputs" ? op.inputPorts : op.outputPorts) {
            resolved.set(portKey(op.operatorID, direction, p.portID), [{ operatorID: id, portID: p.portID }]);
          }
        }
        continue;
      }
      const d = definitions.get(op.operatorType);
      require(d, `Missing macro version: ${op.operatorType}`);
      require(op.operatorVersion === String(d!.version), "Macro version does not match its pinned definition");
      require(!stack.includes(op.operatorType), "Recursive macro reference");
      const inner = cloneDeep(d!.content);
      const bindings = fields(inner);
      for (const [key, value] of Object.entries(op.operatorProperties)) {
        const binding = bindings.find(f => f.id === key);
        require(binding, `Unknown macro property: ${key}`);
        const target = inner.operators.find(o => o.operatorID === binding!.operatorID);
        require(target &&
          (!isMacro(target) ||
            fields(definitions.get(target.operatorType)?.content).some(
              f => f.id === binding!.propertyKey
            )), "Missing exposed macro property");
        inner.operators[inner.operators.indexOf(target!)] = {
          ...target!,
          operatorProperties: { ...target!.operatorProperties, [binding!.propertyKey]: cloneDeep(value) },
        };
      }
      const innerPorts = visit(inner, [...chain, op.operatorID], [...stack, op.operatorType]);
      for (const direction of ["inputs", "outputs"] as const) {
        const boundary = inner.macroInterface![direction];
        const declared = direction === "inputs" ? op.inputPorts : op.outputPorts;
        require(declared.length === boundary!.length &&
          declared.every(p =>
            boundary!.some(b => b.portID === p.portID)
          ), "Macro instance interface differs from its pinned version");
        for (const p of boundary!) {
          // visit already validated each boundary target; disabled operators have no runtime port.
          const targets = p.targets.flatMap(t => innerPorts.get(portKey(t.operatorID, direction, t.portID)) ?? []);
          resolved.set(portKey(op.operatorID, direction, p.portID), targets);
          if (!chain.length) ports[portKey(op.operatorID, direction, p.portID)] = targets;
        }
      }
    }
    if (body.macroInterface) {
      require(typeof body.macroInterface.id === "string" &&
        body.macroInterface.id.length, "Invalid macro interface ID");
      for (const direction of ["inputs", "outputs"] as const) {
        const boundary = body.macroInterface[direction];
        require(Array.isArray(boundary) &&
          boundary.every(p => p && typeof p.portID === "string" && p.portID.length) &&
          new Set(boundary.map(p => p.portID)).size === boundary.length, "Duplicate or invalid macro port");
        boundary.forEach(p => {
          require(Array.isArray(p.targets) &&
            p.targets.length &&
            (direction === "inputs" ||
              p.targets.length === 1), "An output must map to one output port; inputs must have targets");
          require(new Set(p.targets.map(t => portKey(t?.operatorID, direction, t?.portID))).size ===
            p.targets.length, "Duplicate macro target");
          p.targets.forEach(t => endpoint(t, direction));
        });
      }
      require(new Set(fields(body).map(f => f.id)).size === fields(body).length, "Duplicate exposed macro property");
      fields(body).forEach(f =>
        require(f &&
          typeof f.id === "string" &&
          f.id.length &&
          typeof f.propertyKey === "string" &&
          ops.has(f.operatorID), "Invalid exposed macro property")
      );
    }
    require(new Set(body.links.map(l => l?.linkID)).size === body.links.length, "Duplicate link ID");
    for (const l of body.links) {
      require(l && typeof l.linkID === "string" && l.linkID.length, "Invalid link ID");
      const sources = endpoint(l.source, "outputs");
      const targets = endpoint(l.target, "inputs");
      for (const source of sources)
        for (const target of targets) {
          links.push({
            linkID:
              chain.length || sources.length * targets.length > 1
                ? uuid(JSON.stringify([chain, l.linkID, source, target]), namespace)
                : l.linkID,
            source,
            target,
          });
        }
    }
    return resolved;
  };
  visit(content, [], []);
  const children = groupBy(links, l => l.source.operatorID);
  const visiting = new Set<string>(),
    visited = new Set<string>();
  const checkCycle = (id: string): void => {
    require(!visiting.has(id), "Workflow contains a cycle");
    if (visited.has(id)) return;
    visiting.add(id);
    if (Object.hasOwn(children, id)) children[id].forEach(link => checkCycle(link.target.operatorID));
    visiting.delete(id);
    visited.add(id);
  };
  operators.forEach(op => checkCycle(op.operatorID));
  return {
    content: { ...content, operators, links, macroInterface: undefined, macroDependencies: undefined },
    paths,
    ports,
  };
}

/** Capture a convex selection; re-entering it through unselected nodes would create a macro cycle. */
export function extractMacro(content: WorkflowContent, selected: readonly string[], id: string): WorkflowContent {
  require(selected.length > 0, "Select at least one operator");
  const ids = new Set(selected);
  require(selected.every(id => content.operators.some(o => o.operatorID === id)), "Missing selected operator");
  const escaped = new Set<string>();
  const follow = (from: string): void => {
    require(!ids.has(from), "Selection must not leave and re-enter the macro");
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
  require(previous.id === next.id && op.operatorType === macroType(previous), "Different macro definition");
  const signature = (d: MacroDefinition) => [
    d.content.macroInterface!.inputs.map(p => p.portID).sort(),
    d.content.macroInterface!.outputs.map(p => p.portID).sort(),
    fields(d.content)
      .map(f => f.id)
      .sort(),
  ];
  require(isEqual(
    signature(previous),
    signature(next)
  ), "Macro interface changed; insert the new version and reconnect explicitly");
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
  const properties: Record<string, CustomJSONSchema7> = Object.create(null);
  const resolve = (value: any, root: CustomJSONSchema7, op: OperatorPredicate, refs: string[] = []): any => {
    if (Array.isArray(value)) return value.map(v => resolve(v, root, op, refs));
    if (!value || typeof value !== "object") return value;
    if (value.$ref) {
      const { $ref, ...rest } = value;
      require($ref.startsWith("#/") && !refs.includes($ref), "Missing or recursive exposed property schema reference");
      const target = $ref
        .slice(2)
        .split("/")
        .reduce((node: any, key: string) => node?.[key.replace(/~1/g, "/").replace(/~0/g, "~")], root);
      require(target, "Missing exposed property schema reference");
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
    require(op, "Missing exposed property operator");
    const root = getSchema(op!.operatorType).jsonSchema;
    const schema = root.properties?.[f.propertyKey];
    require(schema && typeof schema !== "boolean", "Missing exposed property schema");
    properties[f.id] = {
      ...resolve(schema, root, op!),
      title: f.displayName,
      description: f.helpText,
      default: cloneDeep(
        Object.prototype.hasOwnProperty.call(op!.operatorProperties, f.propertyKey)
          ? op!.operatorProperties[f.propertyKey]
          : (schema as CustomJSONSchema7).default
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

export function macroResultAliases(content: WorkflowContent, expanded = expandMacros(content)): Record<string, string> {
  const aliases: Record<string, string> = Object.create(null);
  for (const op of content.operators.filter(isMacro)) {
    const output = op.outputPorts.flatMap(p => expanded.ports[portKey(op.operatorID, "outputs", p.portID)] ?? [])[0];
    const sink = expanded.content.operators.find(
      o => expanded.paths[o.operatorID]?.[0] === op.operatorID && o.operatorType.toLowerCase().includes("sink")
    );
    if (output || sink) aliases[op.operatorID] = output?.operatorID ?? sink!.operatorID;
  }
  return aliases;
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
