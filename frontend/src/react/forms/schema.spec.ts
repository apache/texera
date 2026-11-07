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

import { describe, expect, it } from "vitest";
import {
  applySchemaDefaults,
  fieldIsHidden,
  getAtPath,
  setAtPath,
  sortedProperties,
  validateSchemaValue,
  type OperatorPropertySchema,
} from "./schema";

describe("operator property schema", () => {
  it("applies falsy and nested defaults without changing schema or saved properties", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        count: { type: "integer", default: 0 },
        enabled: { type: "boolean", default: false },
        settings: { type: "object", default: {}, properties: { delimiter: { type: "string", default: "\t" } } },
        optional: { type: "object", properties: { name: { type: "string", default: "untouched" } } },
      },
    };
    const input = { label: "東京 🐧" };
    expect(applySchemaDefaults(schema, input)).toEqual({
      label: "東京 🐧", count: 0, enabled: false, settings: { delimiter: "\t" },
    });
    expect(input).toEqual({ label: "東京 🐧" });
    expect(schema.properties?.settings).toMatchObject({ default: {} });
  });

  it("retains null, false and zero instead of replacing them with defaults", () => {
    expect(applySchemaDefaults({ default: 10 }, 0)).toBe(0);
    expect(applySchemaDefaults({ default: true }, false)).toBe(false);
    expect(applySchemaDefaults({ type: ["string", "null"], default: "default" }, null)).toBeNull();
  });

  it("validates required text, numeric bounds, enums, unique arrays and nested paths", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      required: ["name"],
      properties: {
        name: { type: "string", minLength: 1 },
        amount: { type: "integer", minimum: 0, maximum: 3 },
        rows: {
          type: "array", uniqueItems: true,
          items: { type: "object", properties: { mode: { enum: ["a", "b"] } } },
        },
      },
    };
    expect(validateSchemaValue(schema, { name: "有效", amount: 0, rows: [{ mode: "a" }] })).toEqual([]);
    const issues = validateSchemaValue(schema, { name: "", amount: 4, rows: [{ mode: "wrong" }, { mode: "wrong" }] });
    expect(issues).toEqual(expect.arrayContaining([
      expect.objectContaining({ keyword: "required", path: ["name"] }),
      expect.objectContaining({ keyword: "maximum", path: ["amount"] }),
      expect.objectContaining({ keyword: "enum", path: ["rows", 0, "mode"] }),
      expect.objectContaining({ keyword: "uniqueItems", path: ["rows"] }),
    ]));
  });

  it("re-evaluates conditional required and custom sibling value rules", () => {
    const schema: OperatorPropertySchema = {
      type: "object",
      properties: {
        kind: { enum: ["count", "ratio"] },
        value: {
          type: "string",
          valueRules: { allOf: [
            { if: { kind: { valEnum: ["count"] } }, then: { type: "integer", minimum: 1 } },
            { if: { kind: { valEnum: ["ratio"] } }, then: { type: "number", exclusiveMinimum: 0 } },
          ] },
        },
      },
      allOf: [{ if: { properties: { kind: { const: "count" } }, required: ["kind"] }, then: { required: ["value"] } }],
    };
    expect(validateSchemaValue(schema, { kind: "count", value: "2" })).toEqual([]);
    expect(validateSchemaValue(schema, { kind: "count", value: "2.5" })).toEqual(expect.arrayContaining([
      expect.objectContaining({ keyword: "valueRules", path: ["value"] }),
    ]));
    expect(validateSchemaValue(schema, { kind: "ratio", value: "2.5" })).toEqual([]);
    expect(validateSchemaValue(schema, { kind: "count" })).toEqual(expect.arrayContaining([
      expect.objectContaining({ keyword: "required", path: ["value"] }),
    ]));
  });

  it("detects unique-among-rows without rejecting the first duplicate pair only", () => {
    const schema: OperatorPropertySchema = {
      type: "array", items: {
        type: "object", properties: { name: { type: "string", uniqueAmongRows: true } },
      },
    };
    expect(validateSchemaValue(schema, [{ name: "a" }, { name: "b" }])).toEqual([]);
    const issues = validateSchemaValue(schema, [{ name: "a" }, { name: "b" }, { name: "a" }]);
    expect(issues).toEqual(expect.arrayContaining([
      expect.objectContaining({ keyword: "uniqueAmongRows", path: [0, "name"] }),
      expect.objectContaining({ keyword: "uniqueAmongRows", path: [2, "name"] }),
    ]));
  });

  it("orders schema properties stably and evaluates toggle and declarative hiding", () => {
    const schema: OperatorPropertySchema = {
      type: "object", properties: {
        advanced: { type: "boolean", propertyOrder: 0, toggleHidden: ["secret"] },
        secret: { type: "string", propertyOrder: 2 },
        plain: { type: "string", propertyOrder: 1 },
        detail: { type: "string", hideTarget: "kind", hideType: "regex", hideExpectedValue: "a|b", hideOnNull: true },
      },
    };
    expect(sortedProperties(schema).map(([name]) => name)).toEqual(["advanced", "plain", "secret", "detail"]);
    expect(fieldIsHidden("secret", schema, { advanced: false })).toBe(true);
    expect(fieldIsHidden("secret", schema, { advanced: true })).toBe(false);
    expect(fieldIsHidden("detail", schema, {})).toBe(true);
    expect(fieldIsHidden("detail", schema, { kind: "a" })).toBe(true);
    expect(fieldIsHidden("detail", schema, { kind: "aa" })).toBe(false);
  });

  it("updates nested arrays immutably and rejects paths that could change prototypes", () => {
    const input = { rows: [{ label: "old", count: 0 }] };
    const next = setAtPath(input, ["rows", 0, "label"], "新");
    expect(getAtPath(next, ["rows", 0, "label"])).toBe("新");
    expect(input.rows[0].label).toBe("old");
    expect(() => setAtPath({}, ["__proto__", "polluted"], true)).toThrow("Unsafe property path");
    expect(() => setAtPath({}, ["constructor", "prototype", "polluted"], true)).toThrow("Unsafe property path");
  });

  it("returns an explicit schema issue for malformed schemas instead of accepting values", () => {
    expect(validateSchemaValue({ type: "invalid" } as unknown as OperatorPropertySchema, {})).toEqual([
      expect.objectContaining({ keyword: "schema", path: [] }),
    ]);
  });
});
