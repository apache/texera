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

/* eslint-disable @typescript-eslint/no-explicit-any -- Preserve existing legacy characterization fixtures and assertions. */
import type { JSONSchema7Definition } from "json-schema";
import { describe, expect, it } from "vitest";
import type { OperatorSchema } from "../../core/workspace/types/operator-schema.interface";
import { serializePortIdentity } from "../../core/common/util/port-identity-serde";
import * as WorkflowCompilingService from "./schema-propagation";

// The existing pure-transform fixtures and user-facing assertions are retained unchanged.
describe("WorkflowCompilingService.dropInvalidAttributeValues", () => {
  // A schema shaped like the Aggregate operator after schema propagation has filled in the
  // valid input attribute names ("col_y" is the only attribute available on the new input).
  const aggregateSchema = (): JSONSchema7Definition =>
    ({
      type: "object",
      properties: {
        groupByKeys: {
          type: "array",
          autofill: "attributeNameList",
          items: { type: "string", enum: ["col_y", ""] },
        },
        aggregations: {
          type: "array",
          items: {
            type: "object",
            properties: {
              attribute: { type: "string", autofill: "attributeName", enum: ["col_y"] },
              aggFunction: { type: "string" },
              resultAttribute: { type: "string" },
            },
          },
        },
      },
    }) as unknown as JSONSchema7Definition;

  it("drops list entries and resets single attributes that are no longer valid", () => {
    const properties = {
      groupByKeys: ["col_x", "col_y"],
      aggregations: [{ attribute: "col_x", aggFunction: "sum", resultAttribute: "r" }],
    };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), properties);

    expect(changed).toBe(true);
    expect(value.groupByKeys).toEqual(["col_y"]);
    expect(value.aggregations[0].attribute).toBe("");
    // non-attribute fields are preserved
    expect(value.aggregations[0].aggFunction).toBe("sum");
    expect(value.aggregations[0].resultAttribute).toBe("r");
    // the input object is never mutated
    expect(properties.groupByKeys).toEqual(["col_x", "col_y"]);
    expect(properties.aggregations[0].attribute).toBe("col_x");
  });

  it("reports no change when all attribute references are valid", () => {
    const properties = {
      groupByKeys: ["col_y"],
      aggregations: [{ attribute: "col_y", aggFunction: "sum", resultAttribute: "r" }],
    };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), properties);

    expect(changed).toBe(false);
    expect(value).toBe(properties);
  });

  it("makes no change when the input schema (enum) is unknown", () => {
    const schemaWithoutEnum: JSONSchema7Definition = {
      type: "object",
      properties: {
        groupByKeys: {
          type: "array",
          autofill: "attributeNameList",
          items: { type: "string" },
        },
        aggregations: {
          type: "array",
          items: {
            type: "object",
            properties: {
              attribute: { type: "string", autofill: "attributeName" },
            },
          },
        },
      },
    } as unknown as JSONSchema7Definition;

    const properties = {
      groupByKeys: ["col_x"],
      aggregations: [{ attribute: "col_x" }],
    };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(schemaWithoutEnum, properties);

    expect(changed).toBe(false);
    expect(value).toBe(properties);
  });

  it("returns the value unchanged for non-object schemas or nullish values", () => {
    // boolean schema (e.g. `additionalProperties: true`)
    expect(WorkflowCompilingService.dropInvalidAttributeValues(true, { a: 1 })).toEqual({
      value: { a: 1 },
      changed: false,
    });
    // null / undefined values are not walked
    expect(WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), null)).toEqual({
      value: null,
      changed: false,
    });
    expect(WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), undefined)).toEqual({
      value: undefined,
      changed: false,
    });
  });

  it("skips schema properties that are absent from the value object", () => {
    // the value is missing both `groupByKeys` and `aggregations` defined in the schema
    const properties = { unrelated: "keep-me" };

    const { value, changed } = WorkflowCompilingService.dropInvalidAttributeValues(aggregateSchema(), properties);

    expect(changed).toBe(false);
    expect(value).toBe(properties);
  });
});

describe("WorkflowCompilingService.setOperatorInputAttrs / restoreOperatorInputAttrs", () => {
  const port0 = serializePortIdentity({ id: 0, internal: false });

  // Builds an OperatorSchema wrapping the given json schema. Only jsonSchema matters for these pure transforms;
  // additionalMetadata is included only to satisfy the OperatorSchema type.
  const makeOperatorSchema = (jsonSchema: any): OperatorSchema =>
    ({
      operatorType: "TestOp",
      operatorVersion: "1",
      jsonSchema,
      additionalMetadata: {
        userFriendlyName: "Test Op",
        operatorGroupName: "Test",
        inputPorts: [{ displayName: "input" }],
        outputPorts: [{ displayName: "output" }],
      },
    }) as unknown as OperatorSchema;

  // input port 0 exposes two attributes.
  const inputPortSchemaMap = {
    [port0]: [
      { attributeName: "col_a", attributeType: "string" },
      { attributeName: "col_b", attributeType: "integer" },
    ],
  } as any;

  describe("setOperatorInputAttrs", () => {
    it("returns the original operator schema unchanged when the input schema map is undefined", () => {
      const schema = makeOperatorSchema({ type: "object", properties: {} });
      expect(WorkflowCompilingService.setOperatorInputAttrs(schema, undefined)).toBe(schema);
    });

    it("returns the original operator schema unchanged when the input schema map is empty", () => {
      const schema = makeOperatorSchema({ type: "object", properties: {} });
      expect(WorkflowCompilingService.setOperatorInputAttrs(schema, {})).toBe(schema);
    });

    it("injects the input attribute names as an enum on an optional attributeName property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      });

      const result = WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap);
      const prop = (result.jsonSchema.properties as any).attribute;
      // optional (not in `required`) properties append "" so the empty selection passes validation.
      expect(prop.enum).toEqual(["col_a", "col_b", ""]);
      expect(prop.type).toBe("string");
      expect(prop.uniqueItems).toBe(true);
    });

    it('appends the property\'s string default instead of "" for optional properties', () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0, default: "col_a" },
        },
      });

      const result = WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap);
      expect((result.jsonSchema.properties as any).attribute.enum).toEqual(["col_a", "col_b", "col_a"]);
    });

    it("throws when the property's default value is not a string", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0, default: 42 },
        },
      });

      expect(() => WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap)).toThrow(
        "default value must be a string"
      );
    });

    it("does not append an empty option for required properties", () => {
      const schema = makeOperatorSchema({
        type: "object",
        required: ["attribute"],
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toEqual(["col_a", "col_b"]);
    });

    it("includes additionalEnumValue before the optional empty option", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: {
            type: "string",
            autofill: "attributeName",
            autofillAttributeOnPort: 0,
            additionalEnumValue: "*",
          },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toEqual(["col_a", "col_b", "*", ""]);
    });

    it("yields an undefined enum when autofillAttributeOnPort is missing", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName" },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toBeUndefined();
    });

    it("yields an undefined enum when the referenced input port has no schema", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          // port 5 is not present in inputPortSchemaMap.
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 5 },
        },
      });

      expect(
        (WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any)
          .attribute.enum
      ).toBeUndefined();
    });

    it("injects the enum into the items of an attributeNameList property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attributes: {
            type: "array",
            autofill: "attributeNameList",
            autofillAttributeOnPort: 0,
            items: { type: "string" },
          },
        },
      });

      const prop = (
        WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap).jsonSchema.properties as any
      ).attributes;
      expect(prop.type).toBe("array");
      expect(prop.uniqueItems).toBe(true);
      expect(prop.items.type).toBe("string");
      expect(prop.items.enum).toEqual(["col_a", "col_b", ""]);
    });
  });

  describe("restoreOperatorInputAttrs", () => {
    it("clears the injected enum/uniqueItems from an attributeName property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: {
            type: "string",
            autofill: "attributeName",
            autofillAttributeOnPort: 0,
            enum: ["col_a", "col_b", ""],
            uniqueItems: true,
          },
        },
      });

      const prop = (WorkflowCompilingService.restoreOperatorInputAttrs(schema).jsonSchema.properties as any).attribute;
      expect(prop.enum).toBeUndefined();
      expect(prop.uniqueItems).toBeUndefined();
      expect(prop.type).toBe("string");
    });

    it("clears the injected enum from the items of an attributeNameList property", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attributes: {
            type: "array",
            autofill: "attributeNameList",
            autofillAttributeOnPort: 0,
            uniqueItems: true,
            items: { type: "string", enum: ["col_a", "col_b", ""] },
          },
        },
      });

      const prop = (WorkflowCompilingService.restoreOperatorInputAttrs(schema).jsonSchema.properties as any).attributes;
      expect(prop.type).toBe("array");
      expect(prop.uniqueItems).toBeUndefined();
      expect(prop.items.enum).toBeUndefined();
      expect(prop.items.type).toBe("string");
    });

    it("round-trips: restore reverses the enums that setOperatorInputAttrs injected", () => {
      const schema = makeOperatorSchema({
        type: "object",
        properties: {
          attribute: { type: "string", autofill: "attributeName", autofillAttributeOnPort: 0 },
        },
      });

      const propagated = WorkflowCompilingService.setOperatorInputAttrs(schema, inputPortSchemaMap);
      expect((propagated.jsonSchema.properties as any).attribute.enum).toEqual(["col_a", "col_b", ""]);

      const restored = WorkflowCompilingService.restoreOperatorInputAttrs(propagated);
      expect((restored.jsonSchema.properties as any).attribute.enum).toBeUndefined();
    });
  });
});
