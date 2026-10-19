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

import { ExecutionMode, WorkflowContent, exportedWorkflow } from "../type/workflow";
import { DefaultView } from "../../dashboard/type/workflow-metadata.interface";
import { OperatorPredicate, OperatorLink } from "../../workspace/types/workflow-common.interface";
import {
  expandMacros,
  mergeMacroDefinitions,
  extractMacro,
  macroInstance,
  macroType,
  macroSchema,
  adoptMacroVersion,
  rollupMacroStatus,
  macroResultAliases,
} from "./workflow-macro";
import { OperatorSchema } from "../../workspace/types/operator-schema.interface";
import { OperatorRuntimeStatus, OperatorState } from "../../workspace/types/execute-workflow.interface";

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

  it("round-trips nested pinned definitions without their source workflow references", () => {
    const child = definition();
    const parent = {
      ...definition("outer", 8),
      content: {
        ...graph([macroInstance(child, "nested")]),
        macroInterface: {
          id: "outer",
          inputs: [{ portID: "input-0", targets: [{ operatorID: "nested", portID: "input-0" }] }],
          outputs: [{ portID: "output-0", targets: [{ operatorID: "nested", portID: "output-0" }] }],
        },
      },
    };
    const content = { ...graph([macroInstance(parent, "root")]), macroDependencies: [parent, child] };
    const { defaultView, ...uploaded } = JSON.parse(JSON.stringify(exportedWorkflow(content, DefaultView.FORM)));
    expect(defaultView).toBe(DefaultView.FORM);
    uploaded.macroDependencies.forEach((d: { wid?: number }) => delete d.wid);
    expect(expandMacros(uploaded)).toEqual(expandMacros(content));
    uploaded.macroDependencies.pop();
    expect(() => expandMacros(uploaded)).toThrow(/missing.*version/i);
  });

  it("deduplicates equivalent definitions regardless of JSON key order but rejects differing content", () => {
    const d = definition();
    const equivalent = { ...d, content: Object.fromEntries(Object.entries(d.content).reverse()) as WorkflowContent };
    const content = { ...graph([macroInstance(d, "m")]), macroDependencies: [d, equivalent] };
    expect(() => expandMacros(content)).not.toThrow();
    const conflicting = structuredClone(d);
    conflicting.content.operators[0] = {
      ...conflicting.content.operators[0],
      operatorProperties: { value: "different" },
    };
    expect(() => expandMacros({ ...content, macroDependencies: [d, conflicting] })).toThrow(/conflicting/i);
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
      { ...d, name: 42 },
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

  it("resolves shared property schemas once through deep nesting and rereads changed metadata", () => {
    const definitions = new Map<string, ReturnType<typeof definition>>();
    let d = definition("depth-0");
    d.content.formBinding.fields.push({ id: "other", operatorID: "b", propertyKey: "value", displayName: "Other" });
    definitions.set(macroType(d), d);
    for (let depth = 1; depth <= 10; depth++) {
      const child = d;
      d = {
        ...definition(`depth-${depth}`),
        content: {
          ...graph([macroInstance(child, "child")]),
          macroInterface: {
            id: `depth-${depth}`,
            inputs: [{ portID: "input-0", displayName: "Rows", targets: [{ operatorID: "child", portID: "input-0" }] }],
            outputs: [
              { portID: "output-0", displayName: "Clean", targets: [{ operatorID: "child", portID: "output-0" }] },
            ],
          },
          formBinding: {
            fields: ["value", "other"].map(id => ({ id, operatorID: "child", propertyKey: id, displayName: id })),
          },
        },
      };
      definitions.set(macroType(d), d);
    }
    const leaf = vi.fn(
      () =>
        ({
          jsonSchema: { properties: { value: { type: "string", default: "original" } } },
        }) as unknown as OperatorSchema
    );
    const schema = (type: string): OperatorSchema =>
      definitions.has(type) ? macroSchema(definitions.get(type)!, schema) : leaf();
    expect(schema(macroType(d)).jsonSchema.properties?.other).toEqual(
      expect.objectContaining({ type: "string", default: "default" })
    );
    expect(leaf).toHaveBeenCalledTimes(1);
    leaf.mockReturnValue({ jsonSchema: { properties: {} } } as OperatorSchema);
    expect(() => schema(macroType(d))).toThrow(/schema/i);
  });

  it("allows an exposed optional property to be filled even when its definition has no value yet", () => {
    const d = definition();
    d.content.operators[0] = { ...d.content.operators[0], operatorProperties: {} };
    const instance = { ...macroInstance(d, "m"), operatorProperties: { value: "filled" } };
    expect(
      expandMacros({ ...graph([instance]), macroDependencies: [d] }).content.operators[0].operatorProperties.value
    ).toBe("filled");
  });

  it("maps exposed attribute pickers to the macro's boundary port instead of the inner port ordinal", () => {
    const d = definition();
    d.content.operators[0].inputPorts.push({ portID: "input-1" });
    d.content.macroInterface.inputs = [
      { portID: "input-0", displayName: "Right", targets: [{ operatorID: "a", portID: "input-1" }] },
      { portID: "input-1", displayName: "Left", targets: [{ operatorID: "a", portID: "input-0" }] },
    ];
    const schema = {
      jsonSchema: { properties: { value: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 1 } } },
    } as unknown as OperatorSchema;
    expect((macroSchema(d, () => schema).jsonSchema.properties!.value as any).autofillAttributeOnPort).toBe(0);
    d.content.macroInterface.inputs = [];
    expect((macroSchema(d, () => schema).jsonSchema.properties!.value as any).autofillAttributeOnPort).toBe(-1);
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

  it("accepts property and port reordering when the exposed identities are unchanged", () => {
    const previous = definition();
    previous.content.formBinding.fields.push({
      id: "second",
      operatorID: "b",
      propertyKey: "value",
      displayName: "Second",
    });
    previous.content.macroInterface.inputs.push({
      portID: "input-1",
      displayName: "Other",
      targets: [{ operatorID: "b", portID: "input-0" }],
    });
    const next = structuredClone(previous);
    next.version++;
    next.content.formBinding.fields.reverse();
    next.content.macroInterface.inputs.reverse();
    const instance = { ...macroInstance(previous, "m"), operatorProperties: { value: "mine" } };
    expect(adoptMacroVersion(instance, previous, next).operatorProperties).toEqual({ value: "mine" });
  });

  it("rejects duplicate boundary targets even when their JSON keys are ordered differently", () => {
    const d = definition();
    d.content.macroInterface.inputs[0].targets.push({ portID: "input-0", operatorID: "a" });
    expect(() => expandMacros(d.content)).toThrow(/duplicate.*target/i);
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

  it("rolls up three nested levels independently, including timing, bytes and cached provenance", () => {
    const child = definition("child");
    const middle = {
      ...definition("middle"),
      content: {
        ...child.content,
        ...graph([macroInstance(child, "inner")]),
        macroInterface: {
          id: "middle",
          inputs: [{ portID: "input-0", targets: [{ operatorID: "inner", portID: "input-0" }] }],
          outputs: [{ portID: "output-0", targets: [{ operatorID: "inner", portID: "output-0" }] }],
        },
        formBinding: undefined,
      },
    };
    const outer = {
      ...middle,
      id: "outer",
      content: {
        ...middle.content,
        ...graph([macroInstance(middle, "inner")]),
        macroInterface: { ...middle.content.macroInterface, id: "outer" },
      },
    };
    const content = {
      ...graph([macroInstance(outer, "one"), macroInstance(outer, "two")]),
      macroDependencies: [child, middle, outer],
    };
    const expanded = expandMacros(content);
    const status: Record<string, OperatorRuntimeStatus> = Object.fromEntries(
      expanded.content.operators.map(o => {
        const first = expanded.paths[o.operatorID][0] === "one",
          input = expanded.paths[o.operatorID].at(-1) === "a";
        return [
          o.operatorID,
          {
            operatorState: OperatorState.Completed,
            aggregatedInputRowCount: first ? 100 : 20,
            aggregatedOutputRowCount: first ? (input ? 100 : 5) : 0,
            aggregatedInputSize: input ? 1000 : 800,
            aggregatedOutputSize: input ? 800 : 50,
            aggregatedDataProcessingTime: 10,
            aggregatedControlProcessingTime: 2,
            aggregatedIdleTime: 3,
            numWorkers: first ? 2 : 0,
            reusedFromCache: !first,
          },
        ];
      })
    );
    const rolled = rollupMacroStatus(content, status, expanded);
    expect(rolled.one).toMatchObject({
      operatorState: OperatorState.Completed,
      aggregatedInputRowCount: 100,
      aggregatedOutputRowCount: 5,
      aggregatedInputSize: 1000,
      aggregatedOutputSize: 50,
      aggregatedDataProcessingTime: 20,
      aggregatedControlProcessingTime: 4,
      aggregatedIdleTime: 6,
      numWorkers: 4,
      reusedFromCache: false,
    });
    expect(rolled.two).toMatchObject({
      operatorState: OperatorState.Completed,
      aggregatedInputRowCount: 20,
      aggregatedOutputRowCount: 0,
      numWorkers: 0,
      reusedFromCache: true,
    });
    const first = expanded.content.operators[0].operatorID;
    for (const state of [OperatorState.Running, OperatorState.Paused, OperatorState.Recovering]) {
      expect(
        rollupMacroStatus(content, { ...status, [first]: { ...status[first], operatorState: state } }, expanded).one
          .operatorState
      ).toBe(state);
    }
    const partial = { ...status };
    delete partial[first];
    expect(rollupMacroStatus(content, partial, expanded).one.operatorState).not.toBe(OperatorState.Completed);
    expect(rollupMacroStatus(content, partial, expanded).one.reusedFromCache).toBe(false);
    expect(rolled[first]).toEqual(status[first]);
  });

  it("distinguishes zero per-port counts from absent port maps and does not invent multiport byte totals", () => {
    const d = definition();
    const content = { ...graph([macroInstance(d, "m")]), macroDependencies: [d] };
    const expanded = expandMacros(content);
    const [a, b] = expanded.content.operators;
    const status: Record<string, OperatorRuntimeStatus> = {
      [a.operatorID]: {
        operatorState: OperatorState.Running,
        aggregatedInputRowCount: 99,
        inputPortMetrics: {},
        aggregatedOutputRowCount: 99,
      },
      [b.operatorID]: {
        operatorState: OperatorState.Completed,
        aggregatedInputRowCount: 99,
        aggregatedOutputRowCount: 99,
        outputPortMetrics: { "0": 0 },
      },
    };
    expect(rollupMacroStatus(content, status, expanded).m).toMatchObject({
      aggregatedInputRowCount: 0,
      aggregatedOutputRowCount: 0,
    });
    delete (status[a.operatorID] as any).inputPortMetrics;
    expect(rollupMacroStatus(content, status, expanded).m.aggregatedInputRowCount).toBe(99);
    status[a.operatorID] = { ...status[a.operatorID], aggregatedInputSize: 1000 };
    expect(rollupMacroStatus(content, status, expanded).m.aggregatedInputSize).toBe(1000);
    d.content.operators[0].inputPorts.push({ portID: "input-1" });
    const multi = expandMacros(content);
    expect(rollupMacroStatus(content, status, multi).m.aggregatedInputSize).toBeUndefined();
    expect(rollupMacroStatus(content, {}, multi).m).toBeUndefined();
  });

  it("counts a broadcast input once, sums independent outputs, and ignores disabled internal workers", () => {
    const d = definition();
    d.content.links = [];
    d.content.macroInterface.inputs[0].targets.push({ operatorID: "b", portID: "input-0" });
    d.content.macroInterface.outputs.push({
      portID: "output-1",
      displayName: "Other",
      targets: [{ operatorID: "a", portID: "output-0" }],
    });
    const content = { ...graph([macroInstance(d, "m")]), macroDependencies: [d] },
      expanded = expandMacros(content);
    const status: Record<string, OperatorRuntimeStatus> = Object.fromEntries(
      expanded.content.operators.map(o => [
        o.operatorID,
        {
          operatorState: OperatorState.Completed,
          aggregatedInputRowCount: 5,
          aggregatedOutputRowCount: 5,
          numWorkers: 1,
          aggregatedInputSize: 100,
          aggregatedOutputSize: 50,
        },
      ])
    );
    expect(rollupMacroStatus(content, status, expanded).m).toMatchObject({
      aggregatedInputRowCount: 5,
      aggregatedOutputRowCount: 10,
      inputPortMetrics: { "0": 5 },
      outputPortMetrics: { "0": 5, "1": 5 },
      numWorkers: 2,
      aggregatedOutputSize: 100,
    });
    expect(rollupMacroStatus(content, status, expanded).m.aggregatedInputSize).toBeUndefined();
    d.content.operators[1] = { ...d.content.operators[1], isDisabled: true };
    const disabled = rollupMacroStatus(content, status);
    expect(disabled.m).toMatchObject({
      operatorState: OperatorState.Completed,
      aggregatedInputRowCount: 5,
      aggregatedOutputRowCount: 5,
      numWorkers: 1,
    });
  });
});

describe("macro bundle normalization", () => {
  it("reads legacy nested bundles and compares immutable bodies independently of packaging", () => {
    const child = definition("child", 1);
    const parent = {
      ...definition("parent", 1),
      content: {
        ...extractMacro(
          { ...graph([macroInstance(child, "nested")]), macroDependencies: [child] },
          ["nested"],
          "parent"
        ),
      },
    };
    const legacy = { ...graph([macroInstance(parent, "root")]), macroDependencies: [parent] };
    const before = JSON.stringify(legacy);
    const flat = mergeMacroDefinitions([
      parent,
      { ...parent, content: { ...parent.content, macroDependencies: undefined } },
    ]);
    expect(flat).toHaveLength(2);
    expect(flat.every(d => !d.content.macroDependencies)).toBe(true);
    expect(expandMacros(legacy).content.operators).toHaveLength(2);
    expect(JSON.stringify(legacy)).toBe(before);
    const conflicting = structuredClone(child);
    conflicting.content.operators[0] = {
      ...conflicting.content.operators[0],
      operatorProperties: { value: "different" },
    };
    expect(() => mergeMacroDefinitions([parent, conflicting])).toThrow(/conflicting/i);
  });
  it("keeps repeated extraction linear and exports all required snapshots", () => {
    let content = graph([op("leaf")]);
    for (let depth = 1; depth <= 12; depth++) {
      const body = extractMacro(content, [content.operators[0].operatorID], `level-${depth}`);
      const d = { id: `level-${depth}`, version: 1, name: `Level ${depth}`, content: body };
      const instance = macroInstance(d, `instance-${depth}`);
      content = {
        ...graph([instance]),
        macroDependencies: mergeMacroDefinitions(content.macroDependencies ?? [], [d]),
      };
      expect(content.macroDependencies!.every(d => !d.content.macroDependencies)).toBe(true);
    }
    expect(JSON.stringify(content).length).toBeLessThan(20000);
    expect(expandMacros(JSON.parse(JSON.stringify(content))).content.operators).toHaveLength(1);
  });
});
