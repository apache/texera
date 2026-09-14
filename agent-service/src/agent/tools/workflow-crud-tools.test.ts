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
  createAddOperatorTool,
  createModifyOperatorTool,
  createDeleteOperatorTool,
  type ToolContext,
} from "./workflow-crud-tools";
import { WorkflowState } from "../workflow-state";
import { makeMetadataFixture, FIXTURE_METADATA } from "../util/metadata-fixture";
import type { WorkflowSystemMetadata } from "../util/workflow-system-metadata";

// The AI SDK tool().execute takes (args, options); the CRUD tools ignore the
// options argument, so a minimal stub is sufficient.
const EXEC_OPTS = { toolCallId: "test-call", messages: [] } as any;

function buildOperatorSchemas(store: WorkflowSystemMetadata): Map<string, any> {
  const map = new Map<string, any>();
  for (const op of FIXTURE_METADATA.operators) {
    map.set(op.operatorType, { jsonSchema: store.getSchema(op.operatorType) });
  }
  return map;
}

function setup() {
  const state = new WorkflowState();
  const store = makeMetadataFixture();
  const context: ToolContext = { metadataStore: store };
  const addTool = createAddOperatorTool(state, buildOperatorSchemas(store), context);
  const modifyTool = createModifyOperatorTool(state, context);
  const deleteTool = createDeleteOperatorTool(state, context);
  return { state, addTool, modifyTool, deleteTool };
}

async function run(tool: any, args: unknown): Promise<string> {
  return (await tool.execute(args, EXEC_OPTS)) as string;
}

describe("addOperator tool", () => {
  test("adds a valid source operator", async () => {
    const { state, addTool } = setup();
    const out = await run(addTool, {
      operatorId: "op1",
      operatorType: "CSVFileScan",
      properties: { fileName: "data.csv" },
      summary: "Load CSV",
    });
    expect(out).toContain("Added operator op1");
    expect(state.getOperator("op1")).toBeDefined();
    expect(state.getOperator("op1")?.operatorProperties.fileName).toBe("data.csv");
  });

  test("connects input links via inputOperatorIds", async () => {
    const { state, addTool } = setup();
    await run(addTool, {
      operatorId: "op1",
      operatorType: "CSVFileScan",
      properties: { fileName: "d.csv" },
      summary: "src",
    });
    const out = await run(addTool, {
      operatorId: "op2",
      operatorType: "Filter",
      properties: {},
      inputOperatorIds: { "0": ["op1"] },
      summary: "filter",
    });
    expect(out).toContain("created links");
    expect(state.getAllLinks()).toHaveLength(1);
    expect(state.getAllLinks()[0].source.operatorID).toBe("op1");
    expect(state.getAllLinks()[0].target.operatorID).toBe("op2");
  });

  test("rejects an unknown operator type", async () => {
    const { addTool } = setup();
    const out = await run(addTool, { operatorId: "op1", operatorType: "Nope", properties: {}, summary: "s" });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("Unknown operator type");
  });

  test("rejects properties that violate the operator schema", async () => {
    const { addTool } = setup();
    // CSVFileScan requires fileName.
    const out = await run(addTool, { operatorId: "op1", operatorType: "CSVFileScan", properties: {}, summary: "s" });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("Invalid properties");
  });

  test("rejects an operatorId that is not in op<N> form", async () => {
    const { addTool } = setup();
    const out = await run(addTool, {
      operatorId: "weird-id",
      operatorType: "CSVFileScan",
      properties: { fileName: "d.csv" },
      summary: "s",
    });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("Invalid operatorId");
  });

  test("rejects a duplicate operatorId", async () => {
    const { addTool } = setup();
    const args = { operatorId: "op1", operatorType: "CSVFileScan", properties: { fileName: "d.csv" }, summary: "s" };
    await run(addTool, args);
    const out = await run(addTool, args);
    expect(out).toContain("already exists");
  });

  test("rejects a link to a non-existent source operator", async () => {
    const { addTool } = setup();
    const out = await run(addTool, {
      operatorId: "op1",
      operatorType: "Filter",
      properties: {},
      inputOperatorIds: { "0": ["ghost"] },
      summary: "s",
    });
    expect(out).toContain("[ERROR]");
    expect(out).toContain('Source operator "ghost" not found');
  });

  test("rejects an out-of-range input port index", async () => {
    const { state, addTool } = setup();
    await run(addTool, {
      operatorId: "op1",
      operatorType: "CSVFileScan",
      properties: { fileName: "d.csv" },
      summary: "s",
    });
    const out = await run(addTool, {
      operatorId: "op2",
      operatorType: "Filter",
      properties: {},
      inputOperatorIds: { "5": ["op1"] },
      summary: "s",
    });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("out of range");
    expect(state.getOperator("op2")).toBeDefined(); // operator was added before link validation failed
  });
});

describe("modifyOperator tool", () => {
  test("merges new properties into an existing operator", async () => {
    const { state, addTool, modifyTool } = setup();
    await run(addTool, {
      operatorId: "op1",
      operatorType: "CSVFileScan",
      properties: { fileName: "a.csv" },
      summary: "s",
    });
    const out = await run(modifyTool, { operatorId: "op1", properties: { fileName: "b.csv" }, summary: "rename" });
    expect(out).toContain("Operator op1 modified");
    expect(state.getOperator("op1")?.operatorProperties.fileName).toBe("b.csv");
  });

  test("returns an error for a missing operator", async () => {
    const { modifyTool } = setup();
    const out = await run(modifyTool, { operatorId: "ghost", properties: {}, summary: "s" });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("not found");
  });

  test("replaces incoming links when inputOperatorIds is provided", async () => {
    const { state, addTool, modifyTool } = setup();
    await run(addTool, {
      operatorId: "op1",
      operatorType: "CSVFileScan",
      properties: { fileName: "a.csv" },
      summary: "s",
    });
    await run(addTool, {
      operatorId: "op2",
      operatorType: "CSVFileScan",
      properties: { fileName: "b.csv" },
      summary: "s",
    });
    await run(addTool, {
      operatorId: "op3",
      operatorType: "Filter",
      properties: {},
      inputOperatorIds: { "0": ["op1"] },
      summary: "s",
    });
    expect(state.getAllLinks()).toHaveLength(1);

    const out = await run(modifyTool, { operatorId: "op3", inputOperatorIds: { "0": ["op2"] }, summary: "relink" });
    expect(out).toContain("deleted links");
    expect(out).toContain("created links");
    expect(state.getAllLinks()).toHaveLength(1);
    expect(state.getAllLinks()[0].source.operatorID).toBe("op2");
  });
});

describe("deleteOperator tool", () => {
  test("deletes an existing operator and its links", async () => {
    const { state, addTool, deleteTool } = setup();
    await run(addTool, {
      operatorId: "op1",
      operatorType: "CSVFileScan",
      properties: { fileName: "a.csv" },
      summary: "s",
    });
    await run(addTool, {
      operatorId: "op2",
      operatorType: "Filter",
      properties: {},
      inputOperatorIds: { "0": ["op1"] },
      summary: "s",
    });
    const out = await run(deleteTool, { operatorId: "op1" });
    expect(out).toContain("Deleted operator: op1");
    expect(state.getOperator("op1")).toBeUndefined();
    expect(state.getAllLinks()).toHaveLength(0); // connected link removed
  });

  test("returns an error when deleting a missing operator", async () => {
    const { deleteTool } = setup();
    const out = await run(deleteTool, { operatorId: "ghost" });
    expect(out).toContain("[ERROR]");
    expect(out).toContain("not found");
  });
});
