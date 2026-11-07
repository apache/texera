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
import type { OperatorPropertySchema } from "./schema";

afterEach(cleanup);

describe("SchemaForm", () => {
  it("edits text, false, zero and enum values without losing their types", () => {
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={{
          type: "object",
          properties: {
            label: { type: "string", title: "Label" },
            enabled: { type: "boolean", title: "Enabled" },
            count: { type: "integer", title: "Count" },
            mode: { enum: [0, 1], title: "Mode" },
          },
        }}
        value={{ label: "old", enabled: true, count: 5, mode: 1 }}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByLabelText("Label"), { target: { value: "東京 🐧" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ label: "東京 🐧" });
    fireEvent.click(screen.getByLabelText("Enabled"));
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ enabled: false });
    fireEvent.change(screen.getByLabelText("Count"), { target: { value: "0" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ count: 0 });
    fireEvent.change(screen.getByLabelText("Mode"), { target: { value: "0" } });
    expect(onChange.mock.lastCall?.[0]).toMatchObject({ mode: 0 });
  });

  it("renders nested array entries with add/remove/reorder operations and schema bounds", () => {
    const onChange = vi.fn();
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        rows: {
          type: "array",
          title: "Rows",
          minItems: 1,
          maxItems: 2,
          items: { type: "object", properties: { name: { type: "string", title: "Name", default: "new" } } },
        },
      },
    };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ rows: [{ name: "first" }] }}
        onChange={onChange}
      />
    );
    expect((screen.getByRole("button", { name: "Remove Rows item 1" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Add Rows item" }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ rows: [{ name: "first" }, { name: "new" }] });
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        onChange={onChange}
      />
    );
    expect((screen.getByRole("button", { name: "Add Rows item" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: "Move Rows item 2 up" }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ rows: [{ name: "new" }, { name: "first" }] });
    fireEvent.click(screen.getByRole("button", { name: "Remove Rows item 2" }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ rows: [{ name: "first" }] });
  });

  it("renders required and backend-loaded invalid values accessibly and honors disabled state", () => {
    const onChange = vi.fn();
    render(
      <SchemaForm
        disabled
        schema={{
          type: "object",
          required: ["name"],
          properties: {
            name: { type: "string", title: "Name", description: "An operator name" },
          },
        }}
        value={{ name: "" }}
        onChange={onChange}
      />
    );
    const name = screen.getByLabelText("Name *") as HTMLInputElement;
    expect(name.disabled).toBe(true);
    expect(name.getAttribute("aria-invalid")).toBe("true");
    expect(screen.getByRole("alert").textContent).toContain("required");
    expect(screen.getByText("An operator name")).toBeTruthy();
  });

  it("reacts to sibling hiding and keeps hidden values in the submitted model", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        show: { type: "boolean", title: "Show", toggleHidden: ["detail"] },
        detail: { type: "string", title: "Detail" },
      },
    };
    const onChange = vi.fn();
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ show: false, detail: "saved" }}
        onChange={onChange}
      />
    );
    expect(screen.queryByLabelText("Detail")).toBeNull();
    fireEvent.click(screen.getByLabelText("Show"));
    expect(onChange.mock.lastCall?.[0]).toEqual({ show: true, detail: "saved" });
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        onChange={onChange}
      />
    );
    expect((screen.getByLabelText("Detail") as HTMLInputElement).value).toBe("saved");
  });

  it("uses registered special widgets instead of silently treating them as ordinary text", () => {
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={{
          type: "object",
          properties: {
            code: { type: "string", title: "Code", "x-widget": "codearea" },
          },
        }}
        value={{ code: "print(1)" }}
        onChange={onChange}
        widgets={{
          codearea: props => <button onClick={() => props.onChange("print(2)")}>{String(props.value)}</button>,
        }}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "print(1)" }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ code: "print(2)" });
  });

  it("reports an unsupported requested widget instead of hiding a migration gap", () => {
    render(
      <SchemaForm
        schema={{
          type: "object",
          properties: {
            file: { type: "string", title: "File", "x-widget": "dataset" },
          },
        }}
        value={{}}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole("alert").textContent).toContain('Unsupported field widget "dataset"');
  });

  it("lets nullable fields switch between null and their typed value", () => {
    const onChange = vi.fn();
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        limit: { type: ["integer", "null"], title: "Limit", default: 10 },
      },
    };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ limit: 0 }}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getByRole("combobox", { name: "Limit value type" }), { target: { value: "null" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ limit: null });
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        onChange={onChange}
      />
    );
    expect(screen.queryByRole("spinbutton")).toBeNull();
    fireEvent.change(screen.getByRole("combobox", { name: "Limit value type" }), { target: { value: "value" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ limit: 10 });
  });

  it("renders single-null unions without offering an editable string value", () => {
    render(
      <SchemaForm
        schema={{ type: "object", properties: { value: { type: ["null"], title: "Value" } } }}
        value={{ value: null }}
        onChange={vi.fn()}
      />
    );
    expect(screen.queryByRole("textbox")).toBeNull();
    expect(screen.getByText("null").tagName).toBe("OUTPUT");
  });
});
