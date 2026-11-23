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

import {
  readMacroInterface,
  isMacroTerminal,
} from "../../main/typescript/workflow-macros";
import type {
  MacroContent,
  MacroInterface,
} from "../../main/typescript/workflow-macros";
import graphIDs from "../resources/workflow-graph-ids.json";

const collections = Object.entries(graphIDs.collections);
export const graphIDCases: {
  name: string;
  content: unknown;
  error?: string;
}[] = [
  ...graphIDs.positive,
  ...collections.flatMap(([field, key], i) => [
    ...collections.slice(i).map(([other, otherKey]) => ({
      name: `reject ${field}/${other} collisions`,
      error: graphIDs.errors.collision,
      content:
        field === other
          ? { [field]: [{ [key]: "same" }, { [key]: "same" }] }
          : { [field]: [{ [key]: "same" }], [other]: [{ [otherKey]: "same" }] },
    })),
    ...graphIDs.invalidIDs.map(({ value, label }) => ({
      name: `reject ${field} ID ${label}`,
      content: { [field]: [{ [key]: value }] },
      error: graphIDs.errors.id,
    })),
    {
      name: `reject malformed ${field}`,
      content: { [field]: {} },
      error: graphIDs.errors.graph,
    },
  ]),
  ...graphIDs.negative,
];

/** Concise fixture builder: specify the exposed ports and generate their saved graph nodes/links. */
export function macroGraph<
  C extends Omit<MacroContent, "macroInterface"> & {
    macroInterface: MacroInterface;
    operatorPositions?: Record<string, { x: number; y: number }>;
  },
>(graph: C): Omit<C, "macroInterface"> & { macroInterface: { id: string } } {
  const boundary = graph.macroInterface;
  const oldIDs = new Set(
    graph.operators.filter(isMacroTerminal).map((op) => op.operatorID),
  );
  const operators = graph.operators.filter((op) => !isMacroTerminal(op));
  const links = graph.links.filter(
    (link) =>
      !oldIDs.has(link.source.operatorID) &&
      !oldIDs.has(link.target.operatorID),
  );
  if (!boundary.id)
    return { ...graph, operators, links, macroInterface: { id: boundary.id } };
  const terminals = [true, false].map((input) => ({
    operatorID: `${boundary.id}__${input ? "inputs" : "outputs"}`,
    operatorType: input ? "MacroInput" : "MacroOutput",
    operatorVersion: "1",
    operatorProperties: {},
    inputPorts: input
      ? []
      : boundary.outputs.map(({ targets, ...port }) => port),
    outputPorts: input
      ? boundary.inputs.map(({ targets, ...port }) => port)
      : [],
    dynamicInputPorts: !input,
    dynamicOutputPorts: input,
    showAdvanced: false,
  }));
  return {
    ...graph,
    macroInterface: { id: boundary.id },
    operators: [...operators, ...terminals],
    links: [
      ...links,
      ...[boundary.inputs, boundary.outputs].flatMap((ports, index) =>
        ports.flatMap((port) =>
          port.targets.map((target, n) => {
            const end = {
              operatorID: terminals[index].operatorID,
              portID: port.portID,
            };
            return {
              linkID: `${boundary.id}__${index}_${port.portID}_${n}`,
              source: index === 0 ? end : target,
              target: index === 0 ? target : end,
            };
          }),
        ),
      ),
    ],
    ...(graph.operatorPositions
      ? {
          operatorPositions: {
            ...graph.operatorPositions,
            [terminals[0].operatorID]: { x: -100, y: 100 },
            [terminals[1].operatorID]: { x: 600, y: 100 },
          },
        }
      : {}),
  };
}

/** Edit a fixture's public interface by rebuilding its normal graph wiring. */
export function editMacroGraph<C extends MacroContent>(
  graph: C,
  edit: (boundary: MacroInterface) => void,
): void {
  const boundary = structuredClone(readMacroInterface(graph)!);
  edit(boundary);
  Object.assign(graph, macroGraph({ ...graph, macroInterface: boundary }));
}
