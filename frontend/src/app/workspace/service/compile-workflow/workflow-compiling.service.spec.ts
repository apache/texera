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

import { JSONSchema7Definition } from "json-schema";
import { WorkflowCompilingService } from "./workflow-compiling.service";

describe("WorkflowCompilingService.dropInvalidAttributeValues", () => {
  // A schema shaped like the Aggregate operator after schema propagation has filled in the
  // valid input attribute names ("col_y" is the only attribute available on the new input).
  const aggregateSchema = (): JSONSchema7Definition => ({
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
  } as unknown as JSONSchema7Definition);

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
});
