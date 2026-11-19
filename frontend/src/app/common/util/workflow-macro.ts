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

import { cloneDeep, groupBy, isEqual, memoize } from "lodash-es";
import { WorkflowContent, FormFieldBinding } from "../type/workflow";
import { LogicalPort, OperatorLink, OperatorPredicate } from "../../workspace/types/workflow-common.interface";
import { OperatorSchema } from "../../workspace/types/operator-schema.interface";
import { CustomJSONSchema7, PropertyContext } from "../../workspace/types/custom-json-schema.interface";
import { conditionalRequiredRules, customFormlyFieldType } from "../../workspace/util/custom-formly-type";
import { assert } from "./assert";

export {
  assertWorkflowIDs,
  isMacro,
  isMacroTerminal,
  readMacroInterface,
  macroType,
  portKey,
  macroInstance,
} from "@texera/workflow-macros";
export type { MacroInterface, MacroPort } from "@texera/workflow-macros";
import {
  isMacro,
  isMacroTerminal,
  readMacroInterface,
  macroType,
  portKey,
  macroInstance,
  createMacroRuntime,
} from "@texera/workflow-macros";
import { v5 as uuid } from "uuid";
import type { MacroDefinition as Definition, MacroExpansion as Expansion, MacroPort } from "@texera/workflow-macros";
const macros = createMacroRuntime(uuid, isEqual);
export const { macroRuntimeID } = macros;
export type MacroDefinition = Definition<WorkflowContent>;
export type MacroExpansion = Expansion<WorkflowContent>;
export const mergeMacroDefinitions = macros.mergeMacroDefinitions<WorkflowContent>;
const fields = (content?: WorkflowContent): FormFieldBinding[] => content?.formBinding?.fields ?? [];

/** Project a backend expansion into the breadcrumb document without expanding its definitions again. */
export function projectMacroExpansion(content: WorkflowContent, root: MacroExpansion, prefix: string[]) {
  assert(root.inspectors?.["[]"], "Missing root compiler mapping");
  // Disabled occurrences and their descendants are absent from the executable graph.
  const inspector = root.inspectors[JSON.stringify(prefix)];
  const ids = inspector?.ids ?? {};
  if (!prefix.length) return { local: root, ids, inspector };
  const boundaries = inspector ? content.operators.filter(op => isMacro(op) || isMacroTerminal(op)) : [];
  const localIDs = new Map(Object.entries(ids).map(([local, runtime]) => [runtime, local]));
  const endpoint = (port: LogicalPort) => ({ ...port, operatorID: localIDs.get(port.operatorID) ?? port.operatorID });
  const local: MacroExpansion = {
    content: {
      ...content,
      macroInterface: undefined,
      macroDependencies: undefined,
      operators: root.content.operators
        .filter(op => localIDs.has(op.operatorID))
        .map(op => ({ ...op, operatorID: localIDs.get(op.operatorID)! })),
      links: root.content.links
        .filter(link => localIDs.has(link.source.operatorID) && localIDs.has(link.target.operatorID))
        .map(link => ({ ...link, source: endpoint(link.source), target: endpoint(link.target) })),
    },
    paths: Object.fromEntries(
      Object.entries(ids).flatMap(([localID, runtime]) =>
        root.paths[runtime] ? [[localID, root.paths[runtime].slice(prefix.length)]] : []
      )
    ),
    resultTargets: Object.fromEntries(
      boundaries.flatMap(op => {
        const runtime = ids[op.operatorID];
        const target = runtime && root.resultTargets?.[runtime];
        return target
          ? [[op.operatorID, { ...target, operatorID: localIDs.get(target.operatorID) ?? target.operatorID }]]
          : [];
      })
    ),
    ports: Object.fromEntries(
      boundaries.flatMap(op =>
        (["inputs", "outputs"] as const).flatMap(direction =>
          (direction === "inputs" ? op.inputPorts : op.outputPorts).map(port => [
            portKey(op.operatorID, direction, port.portID),
            (root.ports[portKey(ids[op.operatorID], direction, port.portID)] ?? []).map(endpoint),
          ])
        )
      )
    ),
  };
  return { local, ids, inspector };
}
/** Read compiler-issued ordinals even when the source lives outside the visible canvas. */
export function compiledInputLinks(root: MacroExpansion, path: string[], op: OperatorPredicate) {
  const references = root.inspectors?.[JSON.stringify(path)]?.inputs[op.operatorID] ?? [];
  return references.map(({ link, input, output }) => {
    const edge = root.content.links[link];
    return {
      ...edge,
      source: { ...edge.source, portIndex: output },
      target: { operatorID: op.operatorID, portID: op.inputPorts[input].portID, portIndex: input },
    };
  });
}
export const macroRevisionLabel = (d: MacroDefinition): string =>
  Number.isSafeInteger(d.revision) && d.revision! > 0 ? `Revision ${d.revision}` : "Saved revision";

/** Retarget one usage; retain every captured body, including the revision it used before. */
export function replaceMacroVersion(content: WorkflowContent, id: string, next: MacroDefinition): WorkflowContent {
  return {
    ...retargetMacro(content, id, next),
    macroDependencies: mergeMacroDefinitions(content.macroDependencies ?? [], [next]),
  };
}

export function linkFitsMacroPorts(link: OperatorLink, macro: OperatorPredicate): boolean {
  const fits = (end: LogicalPort, ports: OperatorPredicate["inputPorts"]) =>
    end.operatorID !== macro.operatorID || ports.some(port => port.portID === end.portID);
  return fits(link.source, macro.outputPorts) && fits(link.target, macro.inputPorts);
}

/** Update one interface; persistence attaches the committed definition after allocating its revision. */
export function retargetMacro(content: WorkflowContent, id: string, next: MacroDefinition): WorkflowContent {
  const op = content.operators.find(o => o.operatorID === id);
  const previous = content.macroDependencies?.find(d => macroType(d) === op?.operatorType);
  assert(op && previous, "Missing pinned macro definition");
  const updated = adoptMacroVersion(op, previous, next);
  return {
    ...content,
    links: content.links.filter(link => linkFitsMacroPorts(link, updated)),
    operators: content.operators.map(o => (o === op ? updated : o)),
    ...(content.formBinding && {
      formBinding: {
        ...content.formBinding,
        fields: fields(content).filter(
          f => f.operatorID !== id || fields(next.content).some(p => p.id === f.propertyKey)
        ),
      },
    }),
  };
}

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
  const inputPorts = boundary("inputs"),
    outputPorts = boundary("outputs");
  const terminals: OperatorPredicate[] = [true, false].map(input => ({
    operatorID: macroRuntimeID([id], input ? "inputs" : "outputs"),
    operatorType: input ? "MacroInput" : "MacroOutput",
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: input
      ? []
      : outputPorts.map(({ targets, ...port }) => ({ ...port, disallowMultiInputs: true, isDynamicPort: true })),
    outputPorts: input ? inputPorts.map(({ targets, ...port }) => ({ ...port, isDynamicPort: true })) : [],
    dynamicInputPorts: !input,
    dynamicOutputPorts: input,
    showAdvanced: false,
  }));
  const points = operators.map(op => content.operatorPositions[op.operatorID]);
  const terminalLinks = [inputPorts, outputPorts].flatMap((ports, index) =>
    ports.flatMap(port =>
      port.targets.map((target, n) => {
        const end = { operatorID: terminals[index].operatorID, portID: port.portID };
        return {
          linkID: macroRuntimeID([id, port.portID], String(n)),
          source: index === 0 ? end : target,
          target: index === 0 ? target : end,
        };
      })
    )
  );
  return cloneDeep({
    ...content,
    ...(content.macroDependencies ? { macroDependencies: mergeMacroDefinitions(content.macroDependencies) } : {}),
    operators: [...operators, ...terminals],
    links: [...links, ...terminalLinks],
    operatorPositions: {
      ...Object.fromEntries(operators.map(o => [o.operatorID, content.operatorPositions[o.operatorID]])),
      [terminals[0].operatorID]: { x: Math.min(...points.map(p => p.x)) - 180, y: Math.min(...points.map(p => p.y)) },
      [terminals[1].operatorID]: { x: Math.max(...points.map(p => p.x)) + 180, y: Math.min(...points.map(p => p.y)) },
    },
    commentBoxes: [],
    macroInterface: { id },
    formBinding: { fields: fields(content).filter(f => ids.has(f.operatorID)) },
  });
}

export function adoptMacroVersion(
  op: OperatorPredicate,
  previous: MacroDefinition,
  next: MacroDefinition
): OperatorPredicate {
  assert(previous.id === next.id && op.operatorType === macroType(previous), "Different macro definition");
  const updated = macroInstance(next, op.operatorID);
  return {
    ...op,
    operatorType: updated.operatorType,
    operatorVersion: updated.operatorVersion,
    inputPorts: updated.inputPorts,
    outputPorts: updated.outputPorts,
    operatorProperties: cloneDeep(
      Object.fromEntries(
        Object.entries(op.operatorProperties).filter(([key]) => fields(next.content).some(f => f.id === key))
      )
    ),
  };
}

/** Exposed Form View fields become the macro's ordinary operator properties, including nested macros. */
export function macroSchema(
  d: MacroDefinition,
  getSchema: (type: string) => OperatorSchema,
  getOperatorSchema?: (op: OperatorPredicate) => OperatorSchema
): OperatorSchema {
  const boundary = readMacroInterface(d.content)!;
  const schemaFor = memoize(
    (op: OperatorPredicate) => (getOperatorSchema ? getOperatorSchema(op) : getSchema(op.operatorType)).jsonSchema,
    op => (getOperatorSchema ? op : op.operatorType)
  );
  const properties: Record<string, CustomJSONSchema7> = Object.create(null);
  const required: string[] = [];
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
            ? boundary.inputs.findIndex(p =>
                p.targets.some(t => t.operatorID === op.operatorID && t.portID === op.inputPorts[v as number]?.portID)
              )
            : resolve(v, root, op, refs),
        ])
    );
  };
  for (const f of fields(d.content)) {
    const op = d.content.operators.find(o => o.operatorID === f.operatorID);
    if (!op) continue;
    const root = schemaFor(op);
    const schema = root.properties?.[f.propertyKey];
    if (!schema || typeof schema === "boolean") continue;
    const aliases = Object.fromEntries(
      fields(d.content)
        .filter(p => p.operatorID === f.operatorID)
        .map(p => [p.propertyKey, p.id])
    );
    const inherited = schema.widget?.formlyConfig?.props?.["propertyContext"] as PropertyContext | undefined;
    const context: PropertyContext = inherited
      ? {
          ...inherited,
          properties: {
            ...inherited.properties,
            ...Object.fromEntries(
              Object.entries(inherited.bindings)
                .filter(([, key]) => Object.hasOwn(op.operatorProperties, key))
                .map(([key, id]) => [key, op.operatorProperties[id]])
            ),
          },
          bindings: Object.fromEntries(
            Object.entries(inherited.bindings)
              .filter(([, id]) => aliases[id])
              .map(([key, id]) => [key, aliases[id]])
          ),
        }
      : {
          operatorType: op.operatorType,
          propertyKey: f.propertyKey,
          properties: cloneDeep({
            ...Object.fromEntries(
              Object.entries(root.properties ?? {})
                .filter(([, p]) => typeof p !== "boolean" && p.default !== undefined)
                .map(([key, p]) => [key, (p as CustomJSONSchema7).default])
            ),
            ...op.operatorProperties,
          }),
          bindings: aliases,
          required: conditionalRequiredRules(root).get(f.propertyKey),
          hiddenBy: Object.entries(root.properties ?? {}).find(
            ([, p]) => typeof p !== "boolean" && p.toggleHidden?.includes(f.propertyKey)
          )?.[0],
        };
    const { widget: schemaWidget, ...valueSchema } = schema;
    const widget = schemaWidget?.formlyConfig;
    if (root.required?.includes(f.propertyKey)) required.push(f.id);
    const type = customFormlyFieldType({
      key: context.propertyKey,
      operatorType: context.operatorType,
      description: schema.description,
      currentType: schema.type,
    });
    properties[f.id] = {
      ...resolve(valueSchema, root, op),
      title: f.displayName,
      description: f.helpText,
      default: cloneDeep(
        Object.hasOwn(op.operatorProperties, f.propertyKey) ? op.operatorProperties[f.propertyKey] : schema.default
      ),
      widget: {
        formlyConfig: { ...widget, ...(type ? { type } : {}), props: { ...widget?.props, propertyContext: context } },
      },
    };
  }
  return {
    operatorType: macroType(d),
    operatorVersion: String(d.version),
    jsonSchema: { type: "object", properties, ...(required.length ? { required } : {}), additionalProperties: false },
    additionalMetadata: {
      userFriendlyName: `${d.name} (${macroRevisionLabel(d)})`,
      operatorGroupName: "Macros",
      operatorDescription: "Reusable macro. Double-click to open this revision on the canvas.",
      inputPorts: boundary.inputs.map(p => ({ displayName: p.displayName })),
      outputPorts: boundary.outputs.map(p => ({ displayName: p.displayName })),
    },
  };
}
