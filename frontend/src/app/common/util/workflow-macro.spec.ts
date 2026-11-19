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
import { testPort, testMacro, expansionFixture } from "../testing/macro-graph";
import {
  testField,
  testBinding,
  testLink as link,
  testOperator,
  testWorkflow as graph,
} from "../testing/workflow-graph";

import { editMacroGraph } from "../../../../../common/workflow-core/src/test/typescript/macro-test-graph";

import { WorkflowContent, exportedWorkflow } from "../type/workflow";
import { DefaultView } from "../../dashboard/type/workflow-metadata.interface";
import { OperatorPredicate } from "../../workspace/types/workflow-common.interface";
import {
  mergeMacroDefinitions,
  extractMacro,
  macroInstance,
  macroType,
  macroSchema,
  adoptMacroVersion,
  replaceMacroVersion,
  macroRevisionLabel,
  macroRuntimeID,
  projectMacroExpansion,
  compiledInputLinks,
  portKey,
  MacroDefinition,
  isMacroTerminal,
  readMacroInterface,
} from "./workflow-macro";

import { OperatorSchema } from "../../workspace/types/operator-schema.interface";
import { mockScanSourceSchema } from "../../workspace/service/operator-metadata/mock-operator-metadata.data";
import { setFieldBehavior, setHideExpression } from "../formly/formly-utils";
import { propertyModel } from "../../workspace/util/custom-formly-type";
import { FormlyFieldConfig } from "@ngx-formly/core";

const op = (id: string) => testOperator(id, { operatorProperties: { value: "default" } });
const nativeSchema = (jsonSchema: OperatorSchema["jsonSchema"]): OperatorSchema => ({
  ...mockScanSourceSchema,
  jsonSchema,
});
const definition = (id = "clean", version = 5) =>
  testMacro(id, [op("a"), op("b")], {
    version,
    name: "清理",
    wid: 12,
    links: [link("a", "b")],
    inputs: [testPort("input-0", "a", "input-0", "Rows")],
    outputs: [testPort("output-0", "b", "output-0", "Clean")],
    formBinding: testBinding("a", "value", "value", "Value"),
  });

const configuredInstance = (
  d: MacroDefinition,
  id: string,
  operatorProperties: OperatorPredicate["operatorProperties"]
) => ({ ...macroInstance(d, id), operatorProperties });
const macroWithFields = (id: string, operator: OperatorPredicate, ...fields: ReturnType<typeof testField>[]) =>
  testMacro(id, [operator], { formBinding: { fields } });

function conflictingBody(definition: MacroDefinition): MacroDefinition {
  const copy = structuredClone(definition);
  copy.content.operators[0] = { ...copy.content.operators[0], operatorProperties: { value: "different" } };
  return copy;
}

describe("workflow macros", () => {
  it("keeps native sibling defaults and required rules through nested exposure and Form View rekeying", () => {
    const leaf = macroWithFields(
      "leaf",
      testOperator("a", { operatorProperties: { fileName: "old", unexposed: "東京" } }),
      testField("a", "enabled", "flag", "Enabled"),
      testField("a", "fileName", "name", "File")
    );
    const native = nativeSchema({
      type: "object",
      properties: { enabled: { type: "boolean", default: true }, fileName: { type: "string" } },
      allOf: [{ if: { properties: { enabled: { const: true } } }, then: { required: ["fileName"] } }],
    });
    const inner = macroSchema(leaf, () => native);
    const innerProperty = inner.jsonSchema.properties!.name as any;
    expect(propertyModel({ key: "name", ...innerProperty.widget.formlyConfig, parent: { model: {} } })).toMatchObject({
      enabled: true,
    });
    const parent = macroWithFields(
      "parent",
      configuredInstance(leaf, "child", { flag: false, name: "child" }),
      testField("child", "name", "public-file", "File")
    );
    const pins = JSON.stringify([leaf, parent]);
    const schema = macroSchema(parent, () => inner).jsonSchema;
    const property = schema.properties!["public-file"] as any;
    const field: FormlyFieldConfig = {
      key: "public-file",
      ...property.widget.formlyConfig,
      parent: { model: { "public-file": "" } },
    };
    setFieldBehavior(field, property);
    const required = field.expressions!["props.required"] as (f: FormlyFieldConfig) => boolean;
    expect(required(field)).toBe(false);
    expect(propertyModel(field)).toEqual({ enabled: false, fileName: "", unexposed: "東京" });
    field.key = "form-view-binding";
    field.parent!.model["form-view-binding"] = null;
    expect(propertyModel(field).fileName).toBeNull();
    expect(propertyModel(field).enabled).toBe(false);
    expect(JSON.stringify([leaf, parent])).toBe(pins);
  });
  it.each([undefined, { fields: [] }])("removes unavailable forwarding only on the adopted usage: %j", formBinding => {
    const child = definition();
    const parent = testMacro(
      "parent",
      [configuredInstance(child, "child", { value: "mine" }), configuredInstance(child, "sibling", { value: "東京" })],
      {
        macroDependencies: [child],
        formBinding: {
          fields: [testField("child", "value", "forward"), testField("sibling", "value", "kept")],
          shownResultIds: ["child"],
        },
      }
    ).content;
    const before = JSON.stringify(parent);
    const removed = { ...child, version: 6, content: { ...child.content, formBinding } };
    const saved = replaceMacroVersion(parent, "child", removed);
    expect(saved.formBinding).toEqual({ fields: [parent.formBinding.fields[1]], shownResultIds: ["child"] });
    expect(saved.operators[0].operatorProperties).toEqual({});
    expect(saved.operators[1]).toEqual(parent.operators[1]);
    expect(saved.macroDependencies!.find(d => macroType(d) === macroType(child))).toEqual(child);
    expect(JSON.stringify(parent)).toBe(before);
  });

  it("keeps native widgets and sibling visibility through public field IDs", () => {
    const scan = testOperator("a", {
      operatorType: "FileScanOp",
      operatorProperties: { progressive: false, fileName: "a.csv", min: 1 },
    });
    const d = macroWithFields(
      "scan",
      scan,
      testField("a", "fileName", "field-file", "File"),
      testField("a", "progressive", "field-progressive", "Progressive"),
      testField("a", "min", "field-min", "Minimum")
    );
    const native = nativeSchema({
      properties: {
        fileName: { type: "string" },
        progressive: { type: "boolean", toggleHidden: ["min"] },
        min: { type: "number" },
      },
    });
    const schema = macroSchema(d, () => native).jsonSchema;
    const properties = schema.properties! as Record<string, any>;
    expect(properties["field-file"].widget?.formlyConfig?.type).toBe("inputautocomplete");
    const model = { "field-progressive": false, "field-min": 2 };
    const fields = Object.entries(properties).map(
      ([key, value]) =>
        ({ key, props: value.widget?.formlyConfig?.props ?? {}, parent: { model } }) as FormlyFieldConfig
    );
    setHideExpression(["min"], fields, "field-progressive");
    const hide = fields.find(f => f.key === "field-min")!.expressions!.hide as (f: any) => boolean;
    expect(hide(fields[2])).toBe(true);
    model["field-progressive"] = true;
    expect(hide(fields[2])).toBe(false);
  });

  it.each([DefaultView.CANVAS, DefaultView.FORM])(
    "exports selected and nested usages with their captured revisions in %s",
    view => {
      const previous = definition(),
        next = definition("clean", 64);
      next.content.operators[0] = { ...next.content.operators[0], operatorProperties: { value: "new default" } };
      const parent = testMacro("outer", [macroInstance(previous, "nested")], {
        version: 8,
        wid: 12,
        inputs: [testPort("input-0", "nested")],
        outputs: [testPort("output-0", "nested")],
      });
      const pins = [previous, parent, definition("unused")];
      const content = graph(
        [macroInstance(previous, "chosen"), macroInstance(previous, "sibling"), macroInstance(parent, "outer")],
        [],
        { macroDependencies: pins }
      );
      const before = structuredClone(content);
      const updated = replaceMacroVersion(content, "chosen", next);
      expect(updated.operators.map(o => o.operatorType)).toEqual([
        macroType(next),
        macroType(previous),
        macroType(parent),
      ]);
      expect(updated.operatorPositions).toEqual(content.operatorPositions);
      expect(() => replaceMacroVersion(content, "missing", next)).toThrow(/missing/i);
      const { defaultView, ...uploaded } = JSON.parse(JSON.stringify(exportedWorkflow(updated, view)));
      expect(defaultView).toBe(view);
      expect(uploaded).toEqual(updated);
      expect(uploaded.macroDependencies).toEqual(expect.arrayContaining([...pins, next]));
      for (const d of uploaded.macroDependencies) delete d.wid;
      expect(content).toEqual(before);
    }
  );

  it("removes only obsolete connections and forwards surviving boundaries when an interface shrinks", () => {
    const previous = definition(),
      next = definition("clean", 6);
    editMacroGraph(next.content, boundary => {
      boundary.inputs = [];
      boundary.outputs = [];
    });
    next.content.formBinding.fields = [];
    const chosen = configuredInstance(previous, "chosen", { value: "mine" });
    const sibling = macroInstance(previous, "sibling");
    const shared = {
      portID: "shared",
      targets: [chosen, sibling].map(({ operatorID }) => ({ operatorID, portID: "input-0" })),
    };
    const content = testMacro("outer", [op("source"), chosen, sibling, op("sink")], {
      links: [link("source", "chosen"), link("chosen", "sink"), link("source", "sibling")],
      macroDependencies: [previous],
      inputs: [shared],
      outputs: [testPort("removed", "chosen", "output-0")],
      formBinding: {
        fields: [testField("chosen", "value", "removed", "Removed"), testField("sibling", "value", "kept", "Kept")],
      },
    }).content;
    const snapshot = JSON.stringify(content);
    const updated = replaceMacroVersion(content, "chosen", next);
    expect(updated.links.filter(link => ["source", "chosen", "sibling"].includes(link.source.operatorID))).toEqual([
      link("source", "sibling"),
    ]);
    expect(readMacroInterface(updated)).toEqual({
      id: "outer",
      inputs: [testPort("shared", "sibling", "input-0")],
      outputs: [{ portID: "removed", targets: [] }],
    });
    expect(updated.formBinding).toEqual({ fields: [content.formBinding!.fields[1]] });
    expect(updated.operators[1].operatorProperties).toEqual({});
    expect(updated.operators[2]).toEqual(sibling);
    expect(JSON.stringify(content)).toBe(snapshot);
    const { defaultView } = JSON.parse(JSON.stringify(exportedWorkflow(updated, DefaultView.FORM)));
    expect(defaultView).toBe(DefaultView.FORM);

    const root = { ...content, macroInterface: undefined };
    expect(replaceMacroVersion(root, "chosen", next).formBinding).toEqual(updated.formBinding);
  });

  it("labels per-workflow revisions without exposing a global history ID as a version number", () => {
    expect(macroRevisionLabel({ ...definition("clean", 64), revision: 3 })).toBe("Revision 3");
    expect(macroRevisionLabel(definition("clean", 64))).toBe("Saved revision");
  });

  it("deduplicates equivalent definitions regardless of JSON key order but rejects differing content", () => {
    const d = definition();
    const equivalent = { ...d, content: Object.fromEntries(Object.entries(d.content).reverse()) as WorkflowContent };
    expect(mergeMacroDefinitions([d, equivalent])).toEqual([d]);
    expect(() => mergeMacroDefinitions([d, conflictingBody(d)])).toThrow(/conflicting/i);
  });

  it("carries referenced property schemas into exposed macro properties", () => {
    const d = definition();
    const schema = nativeSchema({
      properties: { value: { $ref: "#/definitions/value" } },
      definitions: { value: { type: "string", minLength: 1 } },
    });
    expect(macroSchema(d, () => schema).jsonSchema.properties?.value).toMatchObject({ type: "string", minLength: 1 });
    expect(macroSchema(d, () => nativeSchema({ properties: {} })).jsonSchema.properties).toEqual({});
  });

  it("keeps compiled column choices specific to each exposed operator and reuses its schema across fields", () => {
    const d = definition();
    d.content.formBinding.fields.push(
      testField("a", "value", "same", "Again"),
      testField("b", "value", "other", "Other")
    );
    const before = JSON.stringify(d);
    const compiled = vi.fn((op: OperatorPredicate) =>
      nativeSchema({ properties: { value: { type: "string", enum: [op.operatorID] } } })
    );
    const result = macroSchema(d, () => nativeSchema({}), compiled).jsonSchema.properties;
    expect(result).toMatchObject({ value: { enum: ["a"] }, same: { enum: ["a"] }, other: { enum: ["b"] } });
    expect(compiled).toHaveBeenCalledTimes(2);
    expect(JSON.stringify(d)).toBe(before);
  });

  it("resolves shared property schemas once through deep nesting and rereads changed metadata", () => {
    const definitions = new Map<string, ReturnType<typeof definition>>();
    let d = definition("depth-0");
    d.content.formBinding.fields.push(testField("b", "value", "other", "Other"));
    definitions.set(macroType(d), d);
    for (let depth = 1; depth <= 10; depth++) {
      const child = d;
      d = testMacro(`depth-${depth}`, [macroInstance(child, "child")], {
        inputs: [testPort("input-0", "child", "input-0", "Rows")],
        outputs: [testPort("output-0", "child", "output-0", "Clean")],
        formBinding: { fields: ["value", "other"].map(id => testField("child", id)) },
      });
      definitions.set(macroType(d), d);
    }
    const leaf = vi.fn(() => nativeSchema({ properties: { value: { type: "string", default: "original" } } }));
    const schema = (type: string): OperatorSchema =>
      definitions.has(type) ? macroSchema(definitions.get(type)!, schema) : leaf();
    expect(schema(macroType(d)).jsonSchema.properties?.other).toMatchObject({ type: "string", default: "default" });
    expect(leaf).toHaveBeenCalledTimes(1);
    leaf.mockReturnValue(nativeSchema({ properties: {} }));
    expect(schema(macroType(d)).jsonSchema.properties).toEqual({});
  });

  it("maps exposed attribute pickers to the macro's boundary port instead of the inner port ordinal", () => {
    const d = definition();
    d.content.operators[0].inputPorts.push({ portID: "input-1" });
    editMacroGraph(d.content, boundary => {
      boundary.inputs = [testPort("input-0", "a", "input-1", "Right"), testPort("input-1", "a", "input-0", "Left")];
    });
    const schema = nativeSchema({
      properties: { value: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 1 } },
    });
    expect((macroSchema(d, () => schema).jsonSchema.properties!.value as any).autofillAttributeOnPort).toBe(0);
    editMacroGraph(d.content, boundary => {
      boundary.inputs = [];
    });
    expect((macroSchema(d, () => schema).jsonSchema.properties!.value as any).autofillAttributeOnPort).toBe(-1);
  });

  it("extracts a connected selection with a portable terminal graph and its Form View bindings", () => {
    const a = { ...op("a"), inputPorts: [{ portID: "in", displayName: "Rows 東京" }] };
    const b = { ...op("b"), outputPorts: [{ portID: "out" }] };
    const content = graph(
      [op("s"), a, b, op("t")],
      [link("s", "a", "output-0", "in"), link("a", "b"), link("b", "t", "out")],
      {
        formBinding: testBinding("a", "value", "value", "Value"),
      }
    );
    const extracted = extractMacro(content, ["a", "b"], "clean");
    expect(extracted.operators.filter(op => !isMacroTerminal(op)).map(o => o.operatorID)).toEqual(["a", "b"]);
    expect(extracted.links.filter(link => link.source.operatorID === "a")).toEqual([link("a", "b")]);
    expect(extracted.operators).toHaveLength(4);
    expect(extracted.links).toHaveLength(3);
    expect(extracted.macroInterface).toEqual({ id: "clean" });
    const before = JSON.stringify(extracted);
    expect(readMacroInterface(extracted)?.inputs[0].targets).toEqual([{ operatorID: "a", portID: "in" }]);
    expect(readMacroInterface(extracted)?.outputs[0].targets).toEqual([{ operatorID: "b", portID: "out" }]);
    const copy = { id: "clean", version: 2, name: "M", content: JSON.parse(before) };
    expect(macroInstance(copy, "usage").inputPorts[0]).toMatchObject({ portID: "input-0", displayName: "Rows 東京" });
    expect(JSON.stringify(extracted)).toBe(before);
    expect(extracted.formBinding).toEqual(content.formBinding);
    expect(() => extractMacro(content, [], "empty")).toThrow(/select/i);
    expect(() => extractMacro(content, ["missing"], "bad")).toThrow(/missing/i);
    expect(() => extractMacro(content, ["s", "b"], "nonconvex")).toThrow(/selection/i);
  });

  it("keeps port identity through rename and disconnection, including duplicate or missing terminal drafts", () => {
    const body = definition().content;
    const input = body.operators.find(op => op.operatorType === "MacroInput")!;
    input.outputPorts[0] = { ...input.outputPorts[0], displayName: "New" };
    const renamed = { ...body, links: [] };
    expect(readMacroInterface(renamed)?.inputs[0]).toMatchObject({
      portID: "input-0",
      displayName: "New",
      targets: [],
    });
    expect(
      readMacroInterface({ ...body, operators: [...body.operators, { ...input, operatorID: "other" }] })?.inputs.length
    ).toBe(2);
    expect(readMacroInterface({ ...body, operators: body.operators.filter(op => op !== input) })?.inputs).toEqual([]);
    expect(readMacroInterface({ ...body, macroInterface: undefined })).toBeUndefined();
  });

  it("adopts interface changes explicitly while preserving values and checking macro identity", () => {
    const old = definition();
    const instance = configuredInstance(old, "m", { value: "mine" });
    const next = definition("clean", 6);
    const updated = adoptMacroVersion(instance, old, next);
    expect(updated.operatorType).toBe(macroType(next));
    expect(updated.operatorProperties).toEqual({ value: "mine" });
    expect(instance.operatorType).toBe(macroType(old));
    expect(() => adoptMacroVersion(instance, old, definition("other", 6))).toThrow(/definition/i);
    editMacroGraph(next.content, boundary => {
      boundary.outputs = [];
    });
    expect(adoptMacroVersion(instance, old, next).outputPorts).toEqual([]);
  });

  it("adopts additive interface edits while keeping existing ports, values and snapshots", () => {
    const previous = definition(),
      before = JSON.stringify(previous);
    const next = structuredClone(previous);
    next.version++;
    editMacroGraph(next.content, boundary => {
      boundary.inputs.push(testPort("additional", "b", "input-0", "Additional"));
    });
    next.content.formBinding.fields.push(testField("b", "value", "extra", "Extra"));
    const instance = { ...macroInstance(previous, "m"), operatorProperties: { value: "mine" }, viewResult: true };
    const updated = adoptMacroVersion(instance, previous, next);
    expect(updated.inputPorts.map(p => p.portID)).toEqual(["input-0", "additional"]);
    expect(updated.outputPorts).toEqual(instance.outputPorts);
    expect(updated.operatorProperties).toEqual({ value: "mine" });
    expect(updated.viewResult).toBe(true);
    expect(JSON.stringify(previous)).toBe(before);
    expect(instance.operatorType).toBe(macroType(previous));
    const reordered = structuredClone(next);
    reordered.version++;
    reordered.content.formBinding.fields.reverse();
    editMacroGraph(reordered.content, boundary => boundary.inputs.reverse());
    const reorderedUsage = adoptMacroVersion(updated, next, reordered);
    expect(reorderedUsage.operatorProperties).toEqual({ value: "mine" });
    expect(reorderedUsage.inputPorts.map(p => p.portID)).toEqual(["additional", "input-0"]);
    next.content.formBinding.fields = [];
    expect(adoptMacroVersion(instance, previous, next).operatorProperties).toEqual({});
  });

  it("rejects a compiler response missing its root index even when a nested scope is requested", () => {
    const content = graph();
    const incomplete = { content, paths: {}, ports: {}, inspectors: {} };
    for (const path of [[], ["missing"]])
      expect(() => projectMacroExpansion(content, incomplete, path)).toThrow(/missing.*root/i);
  });

  it("keeps the compiler's native result port when projecting a nested usage", () => {
    const d = definition();
    const graphWithUsage = graph([macroInstance(d, "inner")], [], { macroDependencies: [d] });
    const compiled = expansionFixture(graphWithUsage, [[["outer", "inner", "b"], op("b")]]);
    const localID = macroRuntimeID(["inner"], "b");
    const runtimeMacro = macroRuntimeID(["outer"], "inner");
    const runtimeLeaf = compiled.content.operators[0].operatorID;
    compiled.resultTargets = { [runtimeMacro]: { operatorID: runtimeLeaf, portIndex: 1 } };
    compiled.ports[portKey(runtimeMacro, "outputs", "output-0")] = [{ operatorID: runtimeLeaf, portID: "out" }];
    const view = projectMacroExpansion(graphWithUsage, compiled, ["outer"]).local;
    expect(view.resultTargets).toEqual({ inner: { operatorID: localID, portIndex: 1 } });
    expect(view.ports[portKey("inner", "outputs", "output-0")]).toEqual([{ operatorID: localID, portID: "out" }]);
  });
  it("reads the compiler's distinct sources for two exposed inputs that converge on one native port", () => {
    const ports = ["left", "right"];
    const usage = testOperator("usage", { inputPorts: ports.map(portID => ({ portID })) });
    const compiled = expansionFixture(
      graph([usage]),
      [],
      ports.map(id => link(id, "same-native-target"))
    );
    compiled.inspectors["[]"].inputs.usage = ports.map((_, i) => ({ link: i, input: i, output: i }));
    expect(compiledInputLinks(compiled, [], usage)).toMatchObject(
      ports.map((id, i) => ({
        source: { operatorID: id, portIndex: i },
        target: { operatorID: "usage", portID: id, portIndex: i },
      }))
    );
  });
});

describe("macro bundle normalization", () => {
  it("reads legacy nested bundles and compares immutable bodies independently of packaging", () => {
    const child = definition("child", 1);
    const parent = testMacro("parent", [macroInstance(child, "nested")], { macroDependencies: [child] });
    const before = structuredClone(parent);
    const flat = mergeMacroDefinitions([
      parent,
      { ...parent, content: { ...parent.content, macroDependencies: undefined } },
    ]);
    expect(flat).toHaveLength(2);
    expect(flat.every(d => !d.content.macroDependencies)).toBe(true);

    expect(parent).toEqual(before);
    expect(() => mergeMacroDefinitions([parent, conflictingBody(child)])).toThrow(/conflicting/i);
  });
  it("keeps repeated extraction linear with a flat snapshot bundle", () => {
    let content = graph([op("leaf")]);
    for (let depth = 1; depth <= 12; depth++) {
      const id = `level-${depth}`;
      const body = extractMacro(content, [content.operators[0].operatorID], id);
      const d = { id, version: 1, name: `Level ${depth}`, content: body };
      content = graph([macroInstance(d, `instance-${depth}`)], [], {
        macroDependencies: mergeMacroDefinitions(content.macroDependencies ?? [], [d]),
      });
      expect(content.macroDependencies!.every(d => !d.content.macroDependencies)).toBe(true);
    }
    expect(JSON.stringify(content).length).toBeLessThan(25000);
  });
});
