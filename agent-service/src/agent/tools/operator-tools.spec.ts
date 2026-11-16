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
import type { OperatorSchema } from "../../api/backend-api";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import { createGetOperatorSchemaTool, createListOperatorTypesTool } from "./operator-tools";

function schema(operatorType: string, friendly: string, group: string, description: string): OperatorSchema {
  return {
    operatorType,
    operatorVersion: "1.0",
    jsonSchema: {
      properties: {
        groupByKeys: { type: "array", items: { type: "string" } },
        dummyPropertyList: { type: "array" },
      },
      required: ["groupByKeys"],
    },
    additionalMetadata: {
      userFriendlyName: friendly,
      operatorGroupName: group,
      operatorDescription: description,
      inputPorts: [],
      outputPorts: [],
    },
  };
}

function metadata(): WorkflowSystemMetadata {
  const store = new WorkflowSystemMetadata();
  store.loadFromMetadata({
    operators: [
      schema("Sort", "Sort", "Data Cleaning", "Sort rows by columns"),
      schema("Aggregate", "Aggregate", "Data Cleaning", "Calculate aggregates\n  by group"),
      schema("HashJoin", "Hash Join", "Join", "Join two inputs on a key"),
    ],
    groups: [],
  });
  return store;
}

const run = async (agentTool: any, args: Record<string, unknown>) =>
  agentTool.execute(args, { toolCallId: "test-call", messages: [] });

describe("list_operator_types", () => {
  test("names every type and what it does, one line each, ordered by name", async () => {
    expect(await run(createListOperatorTypesTool(metadata()), {})).toBe(
      [
        "Aggregate: Calculate aggregates by group",
        "HashJoin: Join two inputs on a key",
        "Sort: Sort rows by columns",
      ].join("\n")
    );
  });

  test("fails when no operator type is loaded", async () => {
    await expect(run(createListOperatorTypesTool(new WorkflowSystemMetadata()), {})).rejects.toThrow(
      "No operator types are loaded."
    );
  });
});

describe("get_operator_schema", () => {
  const get = createGetOperatorSchemaTool(metadata());
  const groupBy = { groupByKeys: { type: "array", items: { type: "string" } } };

  test("gives each type's description and compact schema, without placeholder properties", async () => {
    expect(JSON.parse(await run(get, { operatorTypes: ["HashJoin", "Sort"] }))).toEqual({
      HashJoin: { description: "Join two inputs on a key", properties: groupBy, required: ["groupByKeys"] },
      Sort: { description: "Sort rows by columns", properties: groupBy, required: ["groupByKeys"] },
    });
  });

  test.each([
    ["a type that does not exist", ["Sort", "Pivot"], "Pivot"],
    ["a type in another case", ["aggregate"], "aggregate"],
  ])("refuses %s, naming it and the tool that lists the types", async (_, operatorTypes, refused) => {
    await expect(run(get, { operatorTypes })).rejects.toThrow(
      `No such operator types: ${refused}. list_operator_types names the types that exist.`
    );
  });

  test("takes at least one type", async () => {
    expect(get.inputSchema).toBeDefined();
    expect((get.inputSchema as any).safeParse({ operatorTypes: [] }).success).toBe(false);
  });
});
