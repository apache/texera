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
  | Readonly<{ type: "range"; rangeAttributeNames: string[]; rangeMin: number; rangeMax: number }>
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
interface ExposedField {
  id: string;
  operatorID: string;
  propertyKey: string;
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
  macroInterface?: MacroInterface;
  macroDependencies?: MacroDefinition[];
  formBinding?: unknown;
}
export interface MacroDefinition<C extends MacroContent = MacroContent> {
  id: string;
  version: number;
  name: string;
  wid?: number;
  content: C;
}

export const isMacro = (op: OperatorPredicate): boolean => op.operatorType.startsWith("WorkflowMacro:");
export const macroType = (d: MacroDefinition): string => `WorkflowMacro:${d.id}:${d.version}`;
export const portKey = (id: string, direction: "inputs" | "outputs", port: string): string =>
  JSON.stringify([id, direction, port]);
const require = (condition: unknown, message: string): void => {
  if (!condition) throw new Error(message);
};
const fields = (content?: MacroContent): ExposedField[] => {
  const value = (content?.formBinding as { fields?: ExposedField[] } | undefined)?.fields ?? [];
  require(Array.isArray(value), "Invalid exposed macro properties");
  return value;
};
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

export interface MacroExpansion<C extends MacroContent = MacroContent> {
  content: C;
  /** Runtime id -> instance chain followed by the definition's leaf id. */
  paths: Record<string, string[]>;
  /** Canvas boundary -> runtime ports, for result and schema lookup. */
  ports: Record<string, LogicalPort[]>;
}

/** Keep a canvas subgraph's runtime leaves and the links between them. */
export function expandMacroSubgraph(graph: Pick<MacroContent, "operators">, expanded: MacroExpansion) {
  const selected = new Set(graph.operators.map(op => op.operatorID));
  const operators = expanded.content.operators.filter(op => selected.has(expanded.paths[op.operatorID][0]));
  const ids = new Set(operators.map(op => op.operatorID));
  return {
    operators,
    links: expanded.content.links.filter(link => ids.has(link.source.operatorID) && ids.has(link.target.operatorID)),
  };
}

/** Texera's wire graph uses port ordinals; the agent retains its legacy fallback to port zero. */
export function toLogicalGraph(graph: Pick<MacroContent, "operators" | "links">, minimumPortOrdinal = -1) {
  const byID = new Map(graph.operators.map(op => [op.operatorID, op]));
  const ordinal = (end: LogicalPort, direction: "inputPorts" | "outputPorts") =>
    Math.max(
      minimumPortOrdinal,
      byID.get(end.operatorID)![direction].findIndex(port => port.portID === end.portID)
    );
  return {
    operators: graph.operators.map(op => ({
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType,
      inputPorts: op.inputPorts,
      outputPorts: op.outputPorts,
    })),
    links: graph.links.map(link => ({
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
  isEqual: (a: unknown, b: unknown) => boolean
) {
  const namespace = "33269d11-7448-5d6d-9aeb-68826068e94d";
  const macroRuntimeID = (chain: string[], id: string): string =>
    chain.length ? uuid(JSON.stringify([...chain, id]), namespace) : id;

  /** One immutable snapshot per identity/version, shared by imports, libraries and clipboard operations. */
  function mergeMacroDefinitions<C extends MacroContent>(...bundles: MacroDefinition<C>[][]): MacroDefinition<C>[] {
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
      require(!previous || isEqual(previous.content, content), "Conflicting macro version");
      definitions.set(macroType(d), { ...d, content });
      (d.content.macroDependencies ?? []).forEach(child => collect(child as MacroDefinition<C>));
    };
    bundles.flat().forEach(collect);
    return [...definitions.values()];
  }

  /** A pure, deterministic expansion used by both compilers and executions. Definitions stay immutable. */
  function expandMacros<C extends MacroContent>(content: C): MacroExpansion<C> {
    const definitions = new Map(mergeMacroDefinitions(content.macroDependencies ?? []).map(d => [macroType(d), d]));
    const paths: Record<string, string[]> = Object.create(null);
    const ports: Record<string, LogicalPort[]> = Object.create(null);
    const operators: OperatorPredicate[] = [];
    const links: MacroContent["links"] = [];

    const visit = (body: MacroContent, chain: string[], stack: string[]): Map<string, LogicalPort[]> => {
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
          operators.push({ ...structuredClone(op), operatorID: id });
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
        const inner = structuredClone(d!.content);
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
            operatorProperties: { ...target!.operatorProperties, [binding!.propertyKey]: structuredClone(value) },
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
    const children = new Map<string, string[]>();
    for (const link of links)
      children.set(link.source.operatorID, [...(children.get(link.source.operatorID) ?? []), link.target.operatorID]);
    const visiting = new Set<string>(),
      visited = new Set<string>();
    const checkCycle = (id: string): void => {
      require(!visiting.has(id), "Workflow contains a cycle");
      if (visited.has(id)) return;
      visiting.add(id);
      children.get(id)?.forEach(checkCycle);
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

  function macroResultAliases(content: MacroContent, expanded = expandMacros(content)): Record<string, string> {
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

  /** Preserve the backend response while projecting leaf schemas/errors onto the saved canvas. */
  function mapMacroCompilation<
    R extends {
      operatorOutputSchemas: Record<string, Record<string, unknown>>;
      operatorErrors: Record<string, { message: string; operatorId?: string }>;
    },
  >(response: R, content: MacroContent, expansion?: MacroExpansion): R {
    if (!content.operators.some(isMacro)) return response;
    const expanded = expansion ?? expandMacros(content);
    const schemas = { ...response.operatorOutputSchemas };
    const errors = { ...response.operatorErrors };
    for (const op of content.operators.filter(isMacro)) {
      const outputs: Record<string, unknown> = {};
      op.outputPorts.forEach((port, index) => {
        const target = expanded.ports[portKey(op.operatorID, "outputs", port.portID)]?.[0];
        const leaf = expanded.content.operators.find(o => o.operatorID === target?.operatorID);
        const ordinal = leaf?.outputPorts.findIndex(p => p.portID === target?.portID);
        const schema = target && ordinal !== undefined ? schemas[target.operatorID]?.[`${ordinal}_false`] : undefined;
        if (schema) outputs[`${index}_false`] = schema;
      });
      schemas[op.operatorID] = outputs;
    }
    for (const [id, error] of Object.entries(response.operatorErrors ?? {})) {
      const path = expanded.paths[id];
      if (path?.length > 1)
        errors[path[0]] = { ...error, operatorId: path[0], message: `${path.slice(1).join(" / ")}: ${error.message}` };
    }
    return { ...response, operatorOutputSchemas: schemas, operatorErrors: errors };
  }

  return { expandMacros, mergeMacroDefinitions, macroRuntimeID, macroResultAliases, mapMacroCompilation };
}
