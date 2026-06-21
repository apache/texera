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

import { describe, expect, test } from "bun:test";
import {
  WorkflowSystemMetadata,
  formatValidationErrors,
  formatCompactSchemaForError,
} from "./workflow-system-metadata";
import type { OperatorMetadata, OperatorSchema } from "../../types/metadata";

function operator(overrides: Partial<OperatorSchema> & Pick<OperatorSchema, "operatorType">): OperatorSchema {
  return {
    operatorVersion: "1",
    jsonSchema: {},
    additionalMetadata: {
      userFriendlyName: overrides.operatorType,
      operatorGroupName: "Test",
      inputPorts: [],
      outputPorts: [],
    },
    ...overrides,
  };
}

function metadataWith(...operators: OperatorSchema[]): OperatorMetadata {
  return { operators, groups: [] };
}

// Filter exercises ref-inlining + key filtering; Limit is a clean Ajv-validatable schema.
const FILTER = operator({
  operatorType: "Filter",
  additionalMetadata: {
    userFriendlyName: "Filter",
    operatorGroupName: "Filter",
    inputPorts: [],
    outputPorts: [],
    operatorDescription: "Filters rows",
  },
  jsonSchema: {
    properties: {
      attribute: { $ref: "#/definitions/AttributeName" },
      limit: { type: "integer", propertyOrder: 5 },
      dummyPropertyList: { type: "array" },
    },
    definitions: {
      AttributeName: { type: "string", title: "Attribute" },
      PortDescription: { type: "object" },
    },
    required: ["attribute"],
  },
});

const LIMIT = operator({
  operatorType: "Limit",
  jsonSchema: { type: "object", properties: { limit: { type: "integer" } }, required: ["limit"] },
});

function loaded(...operators: OperatorSchema[]): WorkflowSystemMetadata {
  const meta = new WorkflowSystemMetadata();
  meta.loadFromMetadata(metadataWith(...operators));
  return meta;
}

describe("WorkflowSystemMetadata.loadFromMetadata", () => {
  test("indexes operators by type", () => {
    const meta = loaded(FILTER, LIMIT);
    expect(meta.getOperatorCount()).toBe(2);
    expect(meta.operatorTypeExists("Filter")).toBe(true);
    expect(meta.operatorTypeExists("Nope")).toBe(false);
    expect(meta.getSchema("Filter")).toEqual(FILTER.jsonSchema);
    expect(meta.getAllOperatorTypes()).toEqual({ Filter: "Filters rows", Limit: "Limit" });
  });

  test("getDescription falls back to userFriendlyName when no description", () => {
    const meta = loaded(FILTER, LIMIT);
    expect(meta.getDescription("Filter")).toBe("Filters rows");
    expect(meta.getDescription("Limit")).toBe("Limit");
    expect(meta.getDescription("Unknown")).toBe("");
  });
});

describe("WorkflowSystemMetadata.getCompactSchema", () => {
  test("returns null for an unknown operator type", () => {
    expect(loaded(FILTER).getCompactSchema("Nope")).toBeNull();
  });

  test("inlines $refs, strips noise keys, and drops filtered properties", () => {
    const compact = loaded(FILTER).getCompactSchema("Filter");
    expect(compact).not.toBeNull();
    // $ref resolved to the AttributeName definition.
    expect(compact!.properties.attribute).toEqual({ type: "string", title: "Attribute" });
    // propertyOrder is in COMPACT_SCHEMA_EXCLUDED_KEYS and is stripped.
    expect(compact!.properties.limit).toEqual({ type: "integer" });
    // dummyPropertyList is in FILTERED_PROPERTY_KEYS and is removed.
    expect(compact!.properties).not.toHaveProperty("dummyPropertyList");
    expect(compact!.required).toEqual(["attribute"]);
  });
});

describe("WorkflowSystemMetadata.getAllSchemasAsJson", () => {
  test("emits filtered properties and definitions as JSON", () => {
    const parsed = JSON.parse(loaded(FILTER).getAllSchemasAsJson());
    expect(Object.keys(parsed.Filter.properties)).toEqual(["attribute", "limit"]); // dummyPropertyList filtered
    expect(parsed.Filter.definitions).toHaveProperty("AttributeName");
    expect(parsed.Filter.definitions).not.toHaveProperty("PortDescription"); // filtered definition
    expect(parsed.Filter.required).toEqual(["attribute"]);
  });
});

describe("WorkflowSystemMetadata.validateOperatorProperties", () => {
  test("accepts properties that satisfy the schema", () => {
    expect(loaded(LIMIT).validateOperatorProperties("Limit", { limit: 5 })).toEqual({ isValid: true });
  });

  test("reports the missing required property", () => {
    const result = loaded(LIMIT).validateOperatorProperties("Limit", {});
    expect(result.isValid).toBe(false);
    expect(result.isValid ? {} : result.messages).toHaveProperty("limit");
  });

  test("rejects an unknown operator type", () => {
    const result = loaded(LIMIT).validateOperatorProperties("Nope", {});
    expect(result.isValid).toBe(false);
    expect(result.isValid ? "" : result.messages.error).toContain("Unknown operator type");
  });
});

describe("formatValidationErrors", () => {
  test("returns empty string when valid", () => {
    expect(formatValidationErrors({ isValid: true })).toBe("");
  });

  test("joins messages as 'key: msg'", () => {
    expect(formatValidationErrors({ isValid: false, messages: { limit: "is required", attribute: "bad" } })).toBe(
      "limit: is required; attribute: bad"
    );
  });
});

describe("formatCompactSchemaForError", () => {
  test("renders only the required properties", () => {
    const formatted = formatCompactSchemaForError({
      properties: { a: { type: "string" }, b: { type: "integer" } },
      required: ["a"],
    });
    expect(formatted).toBe('required: [a], properties: {"a":{"type":"string"}}');
  });
});
