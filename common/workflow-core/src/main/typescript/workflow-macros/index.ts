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

/** The graph contract shared by browser and agent execution; presentation content passes through untouched. */
export interface OperatorPredicate {
  readonly operatorID: string;
  readonly operatorType: string;
  readonly operatorVersion: string;
  readonly operatorProperties: Readonly<Record<string, any>>;
  readonly inputPorts: PortDescription[];
  readonly outputPorts: PortDescription[];
  readonly dynamicInputPorts?: boolean;
  readonly dynamicOutputPorts?: boolean;
  readonly showAdvanced: boolean;
  readonly isDisabled?: boolean;
  readonly viewResult?: boolean;
  readonly markedForReuse?: boolean;
  readonly customDisplayName?: string;
}
export interface LogicalPort {
  readonly operatorID: string;
  readonly portID: string;
}
export type PartitionInfo =
  | Readonly<{ type: "hash"; hashAttributeNames: string[] }>
  | Readonly<{
      type: "range";
      rangeAttributeNames: string[];
      rangeMin: number;
      rangeMax: number;
    }>
  | Readonly<{ type: "single" }>
  | Readonly<{ type: "broadcast" }>
  | Readonly<{ type: "none" }>;
export interface PortDescription {
  readonly portID: string;
  readonly displayName?: string;
  readonly disallowMultiInputs?: boolean;
  readonly isDynamicPort?: boolean;
  readonly partitionRequirement?: PartitionInfo;
  readonly dependencies?: { id: number; internal: boolean }[];
}
export interface OperatorLink {
  readonly linkID: string;
  readonly source: LogicalPort;
  readonly target: LogicalPort;
}
export interface MacroPort extends PortDescription {
  targets: LogicalPort[];
}
export interface MacroInterface {
  id: string;
  inputs: MacroPort[];
  outputs: MacroPort[];
}
export interface MacroContent {
  operators: OperatorPredicate[];
  links: OperatorLink[];
  commentBoxes?: { commentBoxID: string }[];
  macroInterface?: { id: string };
  macroDependencies?: MacroDefinition[];
  formBinding?: unknown;
}

/** Canvas cells share an ID namespace; each pinned graph owns a separate namespace. */
export function assertWorkflowIDs(
  content: unknown,
  path = "Workflow",
  depth = 0,
): void {
  if (
    !content ||
    typeof content !== "object" ||
    Array.isArray(content) ||
    depth > 32
  )
    throw new Error(`${path}: Malformed workflow graph`);
  const graph = content as Record<string, unknown>;
  const seen = new Set<string>();
  for (const [field, key] of [
    ["operators", "operatorID"],
    ["links", "linkID"],
    ["commentBoxes", "commentBoxID"],
  ]) {
    const records = graph[field] ?? [];
    if (!Array.isArray(records))
      throw new Error(`${path}: Malformed workflow graph`);
    for (const record of records) {
      const id: unknown = record?.[key];
      if (typeof id !== "string" || !id.trim())
        throw new Error(`${path}: Invalid graph element ID`);
      if (seen.has(id))
        throw new Error(`${path}: Duplicate graph element ID '${id}'`);
      seen.add(id);
    }
  }
  const dependencies = graph.macroDependencies ?? [];
  if (!Array.isArray(dependencies))
    throw new Error(`${path}: Malformed workflow graph`);
  for (const definition of dependencies)
    assertWorkflowIDs(
      definition?.content,
      `${path} / ${definition?.name ?? "Macro"}`,
      depth + 1,
    );
}
export interface MacroDefinition<C extends MacroContent = MacroContent> {
  id: string;
  version: number;
  /** Display ordinal within this workflow's history; version remains the immutable storage key. */
  revision?: number;
  name: string;
  wid?: number;
  content: C;
}

export const isMacro = (op: OperatorPredicate): boolean =>
  op.operatorType.startsWith("WorkflowMacro:");
export const isMacroTerminal = (
  op: Pick<OperatorPredicate, "operatorType">,
): boolean =>
  op.operatorType === "MacroInput" || op.operatorType === "MacroOutput";

/** Port records own the public identity and name; ordinary links own the mapping. */
export function readMacroInterface(
  content: MacroContent,
): MacroInterface | undefined {
  if (!content.macroInterface) return undefined;
  const group = (input: boolean): MacroPort[] => {
    const nodes = content.operators.filter(
      (op) => op.operatorType === (input ? "MacroInput" : "MacroOutput"),
    );
    return nodes.flatMap((node) =>
      (input ? node.outputPorts : node.inputPorts).map((port) => ({
        ...port,
        targets: content.links
          .filter((link) => {
            const end = input ? link.source : link.target;
            return (
              end.operatorID === node.operatorID && end.portID === port.portID
            );
          })
          .map((link) => (input ? link.target : link.source)),
      })),
    );
  };
  return {
    id: content.macroInterface.id,
    inputs: group(true),
    outputs: group(false),
  };
}

export const macroType = (d: MacroDefinition): string =>
  `WorkflowMacro:${d.id}:${d.version}`;
export const portKey = (
  id: string,
  direction: "inputs" | "outputs",
  port: string,
): string => JSON.stringify([id, direction, port]);
const require = (condition: unknown, message: string): void => {
  if (!condition) throw new Error(message);
};
export function macroInstance(
  d: MacroDefinition,
  operatorID: string,
): OperatorPredicate {
  require(d.content?.macroInterface, "Missing macro interface");
  const boundary = readMacroInterface(d.content)!;
  return {
    operatorID,
    operatorType: macroType(d),
    operatorVersion: String(d.version),
    operatorProperties: {},
    inputPorts: boundary.inputs.map(({ targets, ...p }) => p),
    outputPorts: boundary.outputs.map(({ targets, ...p }) => p),
    showAdvanced: false,
    customDisplayName: d.name,
  };
}

export interface ResultTarget {
  operatorID: string;
  portIndex: number;
}

/** Index into the single expanded native graph for one breadcrumb document. */
export interface MacroInspector {
  ids: Record<string, string>;
  inputs: Record<string, { link: number; input: number; output: number }[]>;
  terminals: {
    direction: "inputs" | "outputs";
    portID: string;
    links: number[];
  }[];
}

export interface MacroExpansion<C extends MacroContent = MacroContent> {
  content: C;
  /** Runtime id -> instance chain followed by the definition's leaf id. */
  paths: Record<string, string[]>;
  resultTargets?: Record<string, ResultTarget>;
  inspectors?: Record<string, MacroInspector>;
  /** Canvas boundary -> runtime ports, for result and schema lookup. */
  ports: Record<string, LogicalPort[]>;
}

/** Stable saved port IDs are distinct from their current position in the compiler's wire graph. */
export const portOrdinal = (
  operator: OperatorPredicate,
  direction: "inputPorts" | "outputPorts",
  portID: string,
): number => operator[direction].findIndex((port) => port.portID === portID);

/** Texera's wire graph uses port ordinals; the agent retains its fallback to port zero. */
export function toLogicalGraph(
  graph: Pick<MacroContent, "operators" | "links">,
  minimumPortOrdinal = -1,
) {
  const byID = new Map(graph.operators.map((op) => [op.operatorID, op]));
  const ordinal = (end: LogicalPort, direction: "inputPorts" | "outputPorts") =>
    Math.max(
      minimumPortOrdinal,
      portOrdinal(byID.get(end.operatorID)!, direction, end.portID),
    );
  return {
    operators: graph.operators.map((op) => ({
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType,
      inputPorts: op.inputPorts,
      outputPorts: op.outputPorts,
    })),
    links: graph.links.map((link) => ({
      fromOpId: link.source.operatorID,
      fromPortId: { id: ordinal(link.source, "outputPorts"), internal: false },
      toOpId: link.target.operatorID,
      toPortId: { id: ordinal(link.target, "inputPorts"), internal: false },
    })),
  };
}

/** Hosts supply their existing UUID v5 and equality implementations; the graph core has no runtime dependencies. */
export function createMacroRuntime(
  uuid: (value: string, namespace: string) => string,
  isEqual: (a: unknown, b: unknown) => boolean,
) {
  const namespace = "33269d11-7448-5d6d-9aeb-68826068e94d";
  const macroRuntimeID = (chain: string[], id: string): string =>
    chain.length ? uuid(JSON.stringify([...chain, id]), namespace) : id;

  /** One immutable snapshot per identity/version, shared by imports, libraries and clipboard operations. */
  function mergeMacroDefinitions<C extends MacroContent>(
    ...bundles: MacroDefinition<C>[][]
  ): MacroDefinition<C>[] {
    const definitions = new Map<string, MacroDefinition<C>>();
    const seen = new WeakSet<object>();
    const collect = (d: MacroDefinition<C>): void => {
      require(d &&
        typeof d.id === "string" &&
        typeof d.name === "string" &&
        d.id.length &&
        Number.isSafeInteger(d.version) &&
        d.version > 0 &&
        d.content?.macroInterface?.id === d.id, "Malformed macro definition");
      if (seen.has(d)) return;
      seen.add(d);
      // A bundle is packaging, not part of the immutable definition body. Older saves nested it.
      const content = { ...d.content };
      delete content.macroDependencies;
      const previous = definitions.get(macroType(d));
      require(!previous ||
        isEqual(previous.content, content), "Conflicting macro version");
      definitions.set(macroType(d), { ...d, content });
      (d.content.macroDependencies ?? []).forEach((child) =>
        collect(child as MacroDefinition<C>),
      );
    };
    bundles.flat().forEach(collect);
    return [...definitions.values()];
  }

  return {
    mergeMacroDefinitions,
    macroRuntimeID,
  };
}
