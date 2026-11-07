/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, expect, it } from "vitest";
import { inputSchemasForOperator } from "./schema-propagation";
import type { OperatorPortSchemaMap } from "../../core/workspace/types/workflow-compiling.interface";
import { applyInputAttributeSchema } from "./input-attributes";

const schemaA = [{ attributeName: "col_a", attributeType: "string" }] as const;
const schemaB = [{ attributeName: "列 β", attributeType: "integer" }] as const;
const link = (source = "source", sourcePort = "output-0", targetPort = "input-0") => ({
  source: { operatorID: source, portID: sourcePort },
  target: { operatorID: "target", portID: targetPort },
});
function resolve(
  links = [link()],
  outputSchemas: Readonly<Record<string, OperatorPortSchemaMap>> = { source: { "0_false": schemaA } },
  ports = 1
) {
  return inputSchemasForOperator({
    operatorId: "target",
    graph: { getInputLinksByOperatorId: () => links },
    dynamicSchema: { additionalMetadata: { inputPorts: Array.from({ length: ports }, () => ({})) } },
    outputSchemas,
  });
}

describe("operator input port schema resolution", () => {
  it("keeps the legacy autofill validation metadata and rejects malformed defaults", () => {
    const schema = {
      type: "object" as const,
      properties: {
        attribute: { type: "string" as const, autofill: "attributeName" as const, autofillAttributeOnPort: 0 },
      },
    };
    expect(applyInputAttributeSchema(schema, { "0_false": schemaA }).properties?.attribute).toMatchObject({
      enum: ["col_a", ""],
      uniqueItems: true,
    });
    expect(() =>
      applyInputAttributeSchema(
        {
          ...schema,
          properties: {
            attribute: { ...schema.properties.attribute, default: 42 },
          },
        },
        { "0_false": schemaA }
      )
    ).toThrow("default value must be a string");
  });
  it("resolves each declared port from its upstream output port without changing the response", () => {
    const outputSchemas = { source: { "2_false": schemaB } };
    const result = resolve([link("source", "output-2", "input-1")], outputSchemas, 3);
    expect(result.inputSchemas).toEqual({ "0_false": undefined, "1_false": schemaB, "2_false": undefined });
    expect(result.inputSchemas?.["1_false"]).toBe(schemaB);
    expect(result.errors).toEqual([]);
    expect(outputSchemas).toEqual({ source: { "2_false": schemaB } });
  });

  it("returns no map for no links, no descriptor, and source operators with no input ports", () => {
    expect(resolve([]).inputSchemas).toBeUndefined();
    expect(resolve([link()], undefined, 0).inputSchemas).toBeUndefined();
    expect(
      inputSchemasForOperator({
        operatorId: "target",
        graph: { getInputLinksByOperatorId: () => [link()] },
        dynamicSchema: undefined,
        outputSchemas: {},
      })
    ).toEqual({ inputSchemas: undefined, errors: [] });
  });

  it("retains unknown ports when upstream schemas or link port IDs cannot be resolved", () => {
    for (const links of [
      [link("missing")],
      [link("source", "the-output")],
      [link("source", "output-0", "the-input")],
    ]) {
      expect(resolve(links)).toEqual({ inputSchemas: { "0_false": undefined }, errors: [] });
    }
  });

  it("accepts equal multi-link schemas including all unknown or all known empty schemas", () => {
    for (const schemas of [
      [schemaA, [...schemaA]],
      [undefined, undefined],
      [[], []],
    ] as const) {
      const result = resolve([link(), link("second")], {
        source: { "0_false": schemas[0] },
        second: { "0_false": schemas[1] },
      });
      expect(result.inputSchemas?.["0_false"]).toEqual(schemas[0]);
      expect(result.errors).toEqual([]);
    }
  });

  it("reports conflicting schemas and preserves independently resolved ports", () => {
    for (const other of [schemaB, undefined, []]) {
      const result = resolve(
        [link(), link("second"), link("source", "output-0", "input-1")],
        {
          source: { "0_false": schemaA },
          second: { "0_false": other },
        },
        2
      );
      expect(result.inputSchemas).toEqual({ "0_false": undefined, "1_false": schemaA });
      expect(result.errors).toEqual([
        {
          operatorId: "target",
          portIndex: 0,
          message: "Multiple links with different schemas connected to the same input port 0",
          details: "Port 0 received 2 different schemas (some may be undefined)",
        },
      ]);
    }
  });

  it("distinguishes an empty known schema from an unknown one", () => {
    const result = resolve([link(), link("second")], { source: { "0_false": [] }, second: {} });
    expect(result.inputSchemas?.["0_false"]).toBeUndefined();
    expect(result.errors).toHaveLength(1);
  });
});
