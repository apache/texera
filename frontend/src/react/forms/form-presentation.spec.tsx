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

import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { SchemaForm } from "./schema-form";
import { validateSchemaValue, type OperatorPropertySchema } from "./schema";

afterEach(cleanup);

describe("operator field presentation", () => {
  it("lets form authors rename a binding title while reserving whole-input removal for its card", () => {
    render(
      <SchemaForm
        schema={{ type: "object", properties: { name: { type: "string", title: "Name" } } }}
        value={{ name: "value" }}
        context={{ fieldPresentation: { authoring: true, canHide: path => path !== "name" } }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByLabelText("Rename Name")).toBeTruthy();
    expect(screen.queryByRole("button", { name: "Hide Name" })).toBeNull();
  });

  it("renames and hides fields without clearing saved values and applies one override to every array row", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        locked: { type: "string", title: "Locked" },
        rows: { type: "array", items: { type: "object", properties: { alias: { type: "string", title: "Alias" } } } },
      },
    };
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={schema}
        value={{ locked: "keep", rows: [{ alias: "a" }, { alias: "b" }] }}
        onChange={onChange}
        context={{
          fieldPresentation: { overrides: { locked: { hidden: true }, "rows.alias": { displayName: "Result name" } } },
        }}
      />
    );
    expect(screen.queryByLabelText("Locked")).toBeNull();
    const aliases = screen.getAllByLabelText("Result name");
    fireEvent.change(aliases[1], { target: { value: "new" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ locked: "keep", rows: [{ alias: "a" }, { alias: "new" }] });
  });

  it("author mode keeps hidden fields editable and exposes naming/visibility callbacks", () => {
    const rename = vi.fn();
    const hide = vi.fn();
    const expose = vi.fn();
    render(
      <SchemaForm
        schema={{ type: "object", properties: { name: { type: "string", title: "Name" } } }}
        value={{ name: "stored" }}
        onChange={vi.fn()}
        context={{
          fieldPresentation: {
            authoring: true,
            showExposure: true,
            overrides: { name: { hidden: true, displayName: "A name" } },
            exposedPaths: ["name"],
            onRename: rename,
            onHiddenChange: hide,
            onExposedChange: expose,
          },
        }}
      />
    );
    expect(screen.getByLabelText("A name")).toBeTruthy();
    fireEvent.change(screen.getByLabelText("Rename Name"), { target: { value: "New caption" } });
    expect(rename).toHaveBeenCalledWith("name", "New caption");
    fireEvent.click(screen.getByRole("button", { name: "Show Name" }));
    expect(hide).toHaveBeenCalledWith("name", false);
    fireEvent.click(screen.getByLabelText("Expose Name on form"));
    expect(expose).toHaveBeenCalledWith("name", false);
  });

  it("drag-reorders Projection rows and preserves edits after the move", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        attributes: {
          type: "array",
          title: "Columns",
          items: { type: "object", properties: { alias: { type: "string", title: "Alias" } } },
        },
      },
    };
    const onChange = vi.fn();
    const reorder = vi.fn();
    const context = { operatorType: "Projection", onArrayReorder: reorder };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ attributes: [{ alias: "a" }, { alias: "b" }] }}
        context={context}
        onChange={onChange}
      />
    );
    const dataTransfer = { setData: vi.fn(), getData: vi.fn(() => JSON.stringify({ path: ["attributes"], from: 0 })) };
    fireEvent.dragStart(screen.getByLabelText("Drag Columns item 1"), { dataTransfer });
    fireEvent.drop(screen.getByLabelText("Drop before Columns item 2"), { dataTransfer });
    expect(onChange.mock.lastCall?.[0]).toEqual({ attributes: [{ alias: "b" }, { alias: "a" }] });
    expect(reorder).toHaveBeenCalledWith(["attributes"], 0, 1);
    rerender(
      <SchemaForm
        schema={schema}
        value={onChange.mock.lastCall?.[0]}
        context={context}
        onChange={onChange}
      />
    );
    fireEvent.change(screen.getAllByLabelText("Alias")[1], { target: { value: "moved" } });
    expect(onChange.mock.lastCall?.[0]).toEqual({ attributes: [{ alias: "b" }, { alias: "moved" }] });
  });

  it("offers presets, saves only enabled sibling fields and applies all selected settings together", async () => {
    const presetRepository = {
      list: vi.fn(async () => [{ host: "saved", username: "reader" }]),
      create: vi.fn().mockResolvedValue(undefined),
      remove: vi.fn().mockResolvedValue(undefined),
    };
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        host: { type: "string", title: "Host", "enable-presets": true },
        username: { type: "string", title: "Username", "enable-presets": true },
        password: { type: "string", title: "Password" },
      },
    };
    const onChange = vi.fn();
    render(
      <SchemaForm
        schema={schema}
        value={{ host: "current", username: "user", password: "private" }}
        context={{ operatorType: "MySQLSource", userPresetEnabled: true, presetRepository }}
        onChange={onChange}
      />
    );
    fireEvent.click(screen.getByRole("button", { name: "Presets for host" }));
    fireEvent.click(await screen.findByRole("button", { name: "saved", exact: true }));
    expect(onChange.mock.lastCall?.[0]).toEqual({ host: "saved", username: "reader", password: "private" });
    fireEvent.click(screen.getByRole("button", { name: "Presets for host" }));
    fireEvent.click(screen.getByRole("button", { name: "Save preset" }));
    await waitFor(() =>
      expect(presetRepository.create).toHaveBeenCalledWith("MySQLSource", { host: "current", username: "user" })
    );
  });
});

describe("input attribute metadata", () => {
  const inputAttributes = {
    0: [
      { attributeName: "left", attributeType: "integer" },
      { attributeName: "time", attributeType: "timestamp" },
    ],
    1: [{ attributeName: "right", attributeType: "string" }],
  };

  it("fills only the annotated port's choices, keeps optional defaults and changes dependent descriptions", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        attribute: {
          type: "string",
          title: "Attribute",
          autofill: "attributeName",
          autofillAttributeOnPort: 0,
          additionalEnumValue: "*",
        },
        amount: { type: "string", title: "Amount", dependOn: "attribute" },
      },
    };
    const { rerender } = render(
      <SchemaForm
        schema={schema}
        value={{ attribute: "time" }}
        context={{ inputAttributes }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByRole("option", { name: "left" })).toBeTruthy();
    expect(screen.queryByRole("option", { name: "right" })).toBeNull();
    expect(screen.getByRole("option", { name: "*", exact: true })).toBeTruthy();
    expect(screen.getByText("Input a datetime string")).toBeTruthy();
    rerender(
      <SchemaForm
        schema={schema}
        value={{ attribute: "left" }}
        context={{ inputAttributes }}
        onChange={vi.fn()}
      />
    );
    expect(screen.getByText("Input a positive number")).toBeTruthy();
  });

  it("validates enum, same-type and conditional attribute rules for scalar and multi-column values", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        first: { type: "string", autofillAttributeOnPort: 0 },
        second: { type: "string", autofillAttributeOnPort: 1 },
        columns: { type: "array", items: { type: "string" }, autofillAttributeOnPort: 0 },
        mode: { type: "string" },
      },
      attributeTypeRules: {
        first: { enum: ["integer"] },
        second: { const: { $data: "first" } },
        columns: { allOf: [{ if: { mode: { valEnum: ["numeric"] } }, then: { enum: ["integer", "double"] } }] },
      },
    };
    const issues = validateSchemaValue(
      schema,
      { first: "left", second: "right", columns: ["left", "time"], mode: "numeric" },
      { inputAttributes }
    );
    expect(issues).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          keyword: "attributeTypeRules",
          message: expect.stringContaining("same type as 'left'"),
        }),
        expect.objectContaining({
          keyword: "attributeTypeRules",
          message: expect.stringContaining("'time' is timestamp"),
        }),
      ])
    );
    expect(
      validateSchemaValue(schema, { first: "left", columns: ["left"], mode: "numeric" }, { inputAttributes })
    ).toEqual([]);
  });
});
