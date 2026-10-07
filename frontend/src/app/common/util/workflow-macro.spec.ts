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

import { ExecutionMode, WorkflowContent } from "../type/workflow";
import { OperatorPredicate, OperatorLink } from "../../workspace/types/workflow-common.interface";
import {
  expandMacros,
  extractMacro,
  macroInstance,
  macroType,
  macroSchema,
  adoptMacroVersion,
  rollupMacroStatus,
  macroResultAliases,
} from "./workflow-macro";
import { OperatorSchema } from "../../workspace/types/operator-schema.interface";
import { OperatorState } from "../../workspace/types/execute-workflow.interface";

const op = (operatorID: string): OperatorPredicate => ({
  operatorID,
  operatorType: "Filter",
  operatorVersion: "1",
  operatorProperties: { value: "default" },
  inputPorts: [{ portID: "input-0" }],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
});
const link = (source: string, target: string): OperatorLink => ({
  linkID: `${source}-${target}`,
  source: { operatorID: source, portID: "output-0" },
  target: { operatorID: target, portID: "input-0" },
});
const graph = (operators: OperatorPredicate[], links: OperatorLink[] = []): WorkflowContent => ({
  operators,
  links,
  operatorPositions: Object.fromEntries(operators.map(o => [o.operatorID, { x: 0, y: 0 }])),
  commentBoxes: [],
  settings: { dataTransferBatchSize: 10, executionMode: ExecutionMode.PIPELINED },
});
const definition = (id = "clean", version = 5) => ({
  id,
  version,
  name: "清理",
  wid: 12,
  content: {
    ...graph([op("a"), op("b")], [link("a", "b")]),
    macroInterface: {
      id,
      inputs: [{ portID: "input-0", displayName: "Rows", targets: [{ operatorID: "a", portID: "input-0" }] }],
      outputs: [{ portID: "output-0", displayName: "Clean", targets: [{ operatorID: "b", portID: "output-0" }] }],
    },
    formBinding: { fields: [{ id: "value", operatorID: "a", propertyKey: "value", displayName: "Value" }] },
  },
});

describe("workflow macros", () => {
  it("keeps ordinary and empty workflows unchanged", () => {
    for (const content of [graph([]), graph([op("a"), op("b")], [link("a", "b")])]) {
      expect(expandMacros(content).content.operators).toEqual(content.operators);
      expect(expandMacros(content).content.links).toEqual(content.links);
    }
  });

  it("expands repeated pinned instances with distinct, deterministic short IDs and local overrides", () => {
    const d = definition();
    const first = { ...macroInstance(d, "first"), operatorProperties: { value: "override" } };
    const content = {
      ...graph(
        [op("source"), first, macroInstance(d, "second"), op("sink")],
        [link("source", "first"), link("first", "second"), link("second", "sink")]
      ),
      macroDependencies: [d],
    };
    const before = JSON.stringify(content);
    const expanded = expandMacros(content);
    expect(expanded.content.operators).toHaveLength(6);
    expect(expanded.content.links).toHaveLength(5);
    expect(new Set(expanded.content.operators.map(o => o.operatorID)).size).toBe(6);
    expect(expanded.content.operators.every(o => !o.operatorType.startsWith("WorkflowMacro:"))).toBe(true);
    const inner = expanded.content.operators.filter(o => expanded.paths[o.operatorID]?.[0] === "first");
    expect(inner.every(o => o.operatorID.length <= 40)).toBe(true);
    expect(inner.find(o => o.operatorProperties.value === "override")).toBeDefined();
    expect(expandMacros(content)).toEqual(expanded);
    expect(JSON.stringify(content)).toBe(before);
  });

  it("recurses through nested definitions and exposed properties and fans an input out", () => {
    const child = definition();
    const nested = macroInstance(child, "nested");
    const parent = {
      ...definition("outer", 8),
      content: {
        ...graph([nested]),
        macroInterface: {
          id: "outer",
          inputs: [{ portID: "input-0", targets: [{ operatorID: "nested", portID: "input-0" }] }],
          outputs: [{ portID: "output-0", targets: [{ operatorID: "nested", portID: "output-0" }] }],
        },
        formBinding: {
          fields: [{ id: "outerValue", operatorID: "nested", propertyKey: "value", displayName: "Value" }],
        },
      },
    };
    const root = { ...macroInstance(parent, "root"), operatorProperties: { outerValue: "nested override" } };
    const expanded = expandMacros({ ...graph([root]), macroDependencies: [parent, child] });
    expect(expanded.content.operators.some(o => o.operatorProperties.value === "nested override")).toBe(true);
    expect(Object.values(expanded.paths).every(path => path.join("/").startsWith("root/nested/"))).toBe(true);
    const fan = definition();
    fan.content.macroInterface.inputs[0].targets.push({ operatorID: "b", portID: "input-0" });
    const fanout = expandMacros({
      ...graph([op("s"), macroInstance(fan, "m")], [link("s", "m")]),
      macroDependencies: [fan],
    });
    expect(fanout.content.links.filter(l => l.source.operatorID === "s")).toHaveLength(2);
  });

  it("rejects missing versions, dangling ports, output fan-in, duplicate IDs, recursion, and cycles", () => {
    const d = definition();
    const root = graph([macroInstance(d, "m")]);
    expect(() => expandMacros(root)).toThrow(/missing.*version/i);
    const broken = structuredClone(d);
    broken.content.macroInterface.outputs[0].targets[0].portID = "absent";
    expect(() => expandMacros({ ...root, macroDependencies: [broken] })).toThrow(/port/i);
    const fanin = structuredClone(d);
    fanin.content.macroInterface.outputs[0].targets.push({ operatorID: "a", portID: "output-0" });
    expect(() => expandMacros({ ...root, macroDependencies: [fanin] })).toThrow(/one.*output|output.*one/i);
    expect(() => expandMacros(graph([op("a"), op("a")]))).toThrow(/duplicate/i);
    const recursive = { ...d, content: { ...d.content, operators: [macroInstance(d, "a"), op("b")] } };
    expect(() => expandMacros({ ...root, macroDependencies: [recursive] })).toThrow(/recursive/i);
    expect(() => expandMacros(graph([op("a"), op("b")], [link("a", "b"), link("b", "a")]))).toThrow(/cycle/i);
  });

  it("rejects malformed external definitions and properties rather than silently dropping them", () => {
    const d = definition();
    const root = graph([macroInstance(d, "m")]);
    for (const invalid of [
      null,
      { ...d, content: null },
      { ...d, version: 0 },
      { ...d, content: { ...d.content, macroInterface: null } },
    ]) {
      expect(() => expandMacros({ ...root, macroDependencies: [invalid] } as unknown as WorkflowContent)).toThrow();
    }
    const instance = { ...macroInstance(d, "m"), operatorProperties: { unknown: 1 } };
    expect(() => expandMacros({ ...graph([instance]), macroDependencies: [d] })).toThrow(/property/i);
  });

  it("validates a definition's boundary even before it is instantiated", () => {
    const d = definition();
    expect(() => expandMacros(d.content)).not.toThrow();
    d.content.macroInterface.inputs[0].targets[0].portID = "missing";
    expect(() => expandMacros(d.content)).toThrow(/port/i);
    const invalid = definition();
    invalid.content.macroInterface.outputs[0].targets = [];
    expect(() => expandMacros(invalid.content)).toThrow(/output/i);
  });

  it("carries referenced property schemas into exposed macro properties", () => {
    const d = definition();
    const schema = {
      jsonSchema: {
        type: "object",
        properties: { value: { $ref: "#/definitions/value" } },
        definitions: { value: { type: "string", minLength: 1 } },
      },
    } as unknown as OperatorSchema;
    expect(macroSchema(d, () => schema).jsonSchema.properties?.["value"]).toEqual(
      expect.objectContaining({ type: "string", minLength: 1 })
    );
    expect(() => macroSchema(d, () => ({ jsonSchema: { properties: {} } }) as OperatorSchema)).toThrow(/schema/i);
  });

  it("allows an exposed optional property to be filled even when its definition has no value yet", () => {
    const d = definition();
    d.content.operators[0] = { ...d.content.operators[0], operatorProperties: {} };
    const instance = { ...macroInstance(d, "m"), operatorProperties: { value: "filled" } };
    expect(
      expandMacros({ ...graph([instance]), macroDependencies: [d] }).content.operators[0].operatorProperties.value
    ).toBe("filled");
  });

  it("extracts a connected selection, preserves boundary links and Form View bindings", () => {
    const content = {
      ...graph([op("s"), op("a"), op("b"), op("t")], [link("s", "a"), link("a", "b"), link("b", "t")]),
      formBinding: { fields: [{ id: "value", operatorID: "a", propertyKey: "value", displayName: "Value" }] },
    };
    const extracted = extractMacro(content, ["a", "b"], "clean");
    expect(extracted.operators.map(o => o.operatorID)).toEqual(["a", "b"]);
    expect(extracted.links).toEqual([link("a", "b")]);
    expect(extracted.macroInterface?.inputs[0].targets).toEqual([{ operatorID: "a", portID: "input-0" }]);
    expect(extracted.macroInterface?.outputs[0].targets).toEqual([{ operatorID: "b", portID: "output-0" }]);
    expect(extracted.formBinding).toEqual(content.formBinding);
    expect(() => extractMacro(content, [], "empty")).toThrow(/select/i);
    expect(() => extractMacro(content, ["missing"], "bad")).toThrow(/missing/i);
    expect(() => extractMacro(content, ["s", "b"], "nonconvex")).toThrow(/selection/i);
  });

  it("adopts a new version explicitly, preserves values, and rejects breaking interfaces", () => {
    const old = definition();
    const instance = { ...macroInstance(old, "m"), operatorProperties: { value: "mine" } };
    const next = definition("clean", 6);
    const updated = adoptMacroVersion(instance, old, next);
    expect(updated.operatorType).toBe(macroType(next));
    expect(updated.operatorProperties).toEqual({ value: "mine" });
    expect(instance.operatorType).toBe(macroType(old));
    expect(() => adoptMacroVersion(instance, old, definition("other", 6))).toThrow(/definition/i);
    next.content.macroInterface.outputs = [];
    expect(() => adoptMacroVersion(instance, old, next)).toThrow(/interface/i);
  });

  it("omits disabled instances and their links without resolving dependencies", () => {
    const disabled = { ...macroInstance(definition(), "m"), isDisabled: true };
    const expanded = expandMacros(graph([op("s"), disabled], [link("s", "m")]));
    expect(expanded.content.operators.map(o => o.operatorID)).toEqual(["s"]);
    expect(expanded.content.links).toEqual([]);
  });

  it("reports boundary counts and aggregate state without counting internal edges twice", () => {
    const d = definition();
    const content = { ...graph([macroInstance(d, "m")]), macroDependencies: [d] };
    const expanded = expandMacros(content);
    const [a, b] = expanded.content.operators;
    const status = {
      [a.operatorID]: {
        operatorState: OperatorState.Completed,
        aggregatedInputRowCount: 100,
        aggregatedOutputRowCount: 100,
        inputPortMetrics: { "0": 100 },
        outputPortMetrics: { "0": 100 },
        numWorkers: 1,
      },
      [b.operatorID]: {
        operatorState: OperatorState.Running,
        aggregatedInputRowCount: 100,
        aggregatedOutputRowCount: 5,
        inputPortMetrics: { "0": 100 },
        outputPortMetrics: { "0": 5 },
        numWorkers: 2,
      },
    };
    const rolled = rollupMacroStatus(content, status);
    expect(rolled.m.operatorState).toBe(OperatorState.Running);
    expect(rolled.m.aggregatedInputRowCount).toBe(100);
    expect(rolled.m.aggregatedOutputRowCount).toBe(5);
    expect(rolled.m.numWorkers).toBe(3);
    expect(rolled[a.operatorID]).toEqual(status[a.operatorID]);
    expect(rollupMacroStatus(graph([]), {})).toEqual({});
    expect(rollupMacroStatus(content, {}).m).toBeUndefined();
    expect(macroResultAliases(content).m).toBe(b.operatorID);
    expect(macroResultAliases(graph([]))).toEqual({});
  });
});
