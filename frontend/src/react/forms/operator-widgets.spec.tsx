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

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SchemaForm } from "./schema-form";
import { resolveOperatorWidget } from "./operator-widgets";
import { delimiterError } from "./delimiter";
import { coerceOrReference, isReference } from "./loop-variables";
import { validateSchemaValue, type OperatorPropertySchema } from "./schema";

afterEach(cleanup);

describe("operator schema widgets", () => {
  it("keeps descriptor-only dummy fields locked and shows extracted output names only when applicable", () => {
    const dummySchema: OperatorPropertySchema = {
      type: "object",
      properties: {
        dummyOperator: { type: "string", title: "Dummy operator" },
        dummyPropertyList: { type: "array", title: "Dummy properties", items: { type: "string" } },
      },
    };
    const { rerender } = render(
      <SchemaForm
        schema={dummySchema}
        value={{ dummyOperator: "internal", dummyPropertyList: [] }}
        context={{ operatorType: "Other" }}
        onChange={vi.fn()}
      />
    );
    expect((screen.getByLabelText("Dummy operator") as HTMLInputElement).disabled).toBe(true);
    expect(screen.queryByRole("button", { name: "Add Dummy properties item" })).toBeNull();
    const scanSchema: OperatorPropertySchema = {
      type: "object",
      properties: { outputFileName: { type: "string", title: "Output name" } },
    };
    rerender(
      <SchemaForm
        schema={scanSchema}
        value={{ extract: false, attributeType: "string" }}
        context={{ operatorType: "FileScanOp" }}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryByLabelText("Output name")).toBeNull();
    rerender(
      <SchemaForm
        schema={scanSchema}
        value={{ extract: true, attributeType: "string" }}
        context={{ operatorType: "FileScanOp" }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByLabelText("Output name")).toBeTruthy();
  });

  it("searches enum choices while retaining the saved selection and its stored type", () => {
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={{ type: "object", properties: { choice: { title: "Choice", enum: [10, 20, 30] } } }}
        value={{ choice: 10 }}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Search Choice options"), { target: { value: "20" } });
    expect(screen.queryByRole("option", { name: "30" })).toBeNull();
    expect((screen.getByLabelText("Choice") as HTMLSelectElement).value).toBe("0");
    fireEvent.change(screen.getByLabelText("Choice"), { target: { value: "1" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ choice: 20 });
  });

  it("resolves the existing backend widget and operator/property metadata", () => {
    expect(
      resolveOperatorWidget({ type: "string", widget: { formlyConfig: { type: "delimiter" } } }, "delimiter", "CSVScan")
    ).toBe("delimiter");
    expect(resolveOperatorWidget({ type: "string" }, "fileName", "CSVScan")).toBe("inputautocomplete");
    expect(resolveOperatorWidget({ type: "string" }, "datasetVersionPath", "Scan")).toBe("datasetversionselector");
    expect(resolveOperatorWidget({ type: "string" }, "modelId", "HuggingFace")).toBe("huggingface");
    expect(resolveOperatorWidget({ type: "string" }, "modelId", "Other")).toBeUndefined();
    expect(resolveOperatorWidget({ type: "string", description: "Input your code here" }, "code", "PythonUDF")).toBe(
      "codearea"
    );
    expect(resolveOperatorWidget({ type: "array" }, "attributes", "Projection")).toBe("repeat-section-dnd");
  });

  it("reads the real delimiter schema, writes literal tabs, and does not snap custom typing to presets", () => {
    const onChange = vi.fn();
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        delimiter: {
          type: "string",
          title: "Delimiter",
          maxLength: 1,
          widget: { formlyConfig: { type: "delimiter", props: { delimiterMode: "char" } } },
        },
      },
    };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ delimiter: "," }}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Delimiter"), { target: { value: "__custom__" } });
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        onChange={onChange}
      />
    );
    const custom = screen.getByLabelText("Custom Delimiter");
    expect(custom.getAttribute("maxlength")).toBeNull();
    fireEvent.change(custom, { target: { value: ";" } });
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        onChange={onChange}
      />
    );
    expect(screen.getByLabelText("Custom Delimiter")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Custom Delimiter"), { target: { value: "\\t" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ delimiter: "\t" });
  });

  it("validates regex delimiters immediately and defers Java-only patterns to the backend", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        delimiter: {
          type: "string",
          title: "Delimiter",
          widget: { formlyConfig: { type: "delimiter", props: { delimiterMode: "regex" } } },
        },
      },
    };
    render(
      <SchemaForm
        schema={schema}
        value={{ delimiter: "|" }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole("alert").textContent).toContain("empty string");
    expect(delimiterError("\\|", "regex")).toBeUndefined();
    expect(delimiterError("(?i)(", "regex")).toBeUndefined();
    expect(delimiterError("(", "regex")).toContain("Invalid regular expression");
  });

  it("renders backend textarea and password metadata without x-widget annotations", () => {
    render(
      <SchemaForm
        schema={{
          type: "object",
          properties: {
            memo: { type: "string", title: "Memo", widget: { formlyConfig: { type: "textarea" } } },
            password: {
              type: "string",
              title: "Password",
              widget: { formlyConfig: { templateOptions: { type: "password" } } },
            },
          },
        }}
        value={{ memo: "line1\nline2", password: "secret" }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByLabelText("Memo").tagName).toBe("TEXTAREA");
    expect(screen.getByLabelText("Password").getAttribute("type")).toBe("password");
  });

  it("keeps constrained numeric values as strings and re-evaluates the sibling's enum", () => {
    const onChange = vi.fn();
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        mode: { type: "string" },
        value: {
          type: "string",
          title: "Value",
          valueRules: {
            allOf: [
              { if: { mode: { valEnum: ["count"] } }, then: { type: "integer", minimum: 1 } },
              { if: { mode: { valEnum: ["choice"] } }, then: { enum: ["auto", "exact"] } },
            ],
          },
        },
      },
    };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ mode: "count", value: "2" }}
        onChange={onChange}
      />
    );
    const field = screen.getByLabelText("Value");
    expect(field.getAttribute("type")).toBe("number");
    fireEvent.change(field, { target: { value: "3" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ value: "3" });
    rerender(
      <SchemaForm
        schema={schema}
        value={{ mode: "choice", value: "auto" }}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Value"), { target: { value: "1" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ value: "exact" });
  });
});

describe("loop-variable fields", () => {
  it("preserves complete primitives and incomplete typed numerals, and recognizes only whole references", () => {
    expect(coerceOrReference("1.0", "number")).toBe(1);
    expect(coerceOrReference("1.", "number")).toBe("1.");
    expect(coerceOrReference("TRUE", "boolean")).toBe(true);
    expect(coerceOrReference("", "integer")).toBeUndefined();
    expect(coerceOrReference("  $K ", "integer")).toBe("$K");
    expect(isReference("prefix $K")).toBe(false);
    expect(isReference("$名前")).toBe(false);
  });

  it("offers references for plain primitives, preserves lexical edits and warns without rejecting unknown variables", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: { count: { type: "number", title: "Count" } },
    };
    const onChange = vi.fn();
    const context = { loopVariableNames: ["K", "index"] };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ count: 0 }}
        context={context}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Count"), { target: { value: "1.0" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ count: 1 });
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        context={context}
        onChange={onChange}
      />
    );
    expect((screen.getByLabelText("Count") as HTMLInputElement).value).toBe("1.0");
    fireEvent.change(screen.getByLabelText("Count"), { target: { value: "$missing" } });
    expect(onChange.mock.lastCall?.[1]).toEqual([]);
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        context={context}
        onChange={onChange}
      />
    );
    expect(screen.getByRole("alert").textContent).toContain("not a variable");
    expect(screen.queryAllByRole("option").map(option => option.getAttribute("value"))).toEqual([]);
  });

  it("does not permit references below noLoopVariable, or replace enum and specialized controls", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        locked: { type: "object", noLoopVariable: true, properties: { count: { type: "integer", title: "Count" } } },
        choice: { type: "string", title: "Choice", enum: ["a", "b"] },
      },
    };
    render(
      <SchemaForm
        schema={schema}
        value={{ locked: { count: 2 }, choice: "a" }}
        context={{ loopVariableNames: ["K"] }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByLabelText("Count").getAttribute("type")).toBe("number");
    expect(screen.getByLabelText("Choice").tagName).toBe("SELECT");
    expect(validateSchemaValue(schema, { locked: { count: "$K" }, choice: "a" }, { loopVariableNames: ["K"] })).toEqual(
      expect.arrayContaining([expect.objectContaining({ path: ["locked", "count"], keyword: "type" })])
    );
  });
});
