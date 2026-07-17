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
import { createAddOperatorTool, createModifyOperatorTool, createDeleteOperatorTool } from "./workflow-crud-tools";
import { WorkflowState } from "../workflow-state";
import { WorkflowSystemMetadata } from "../util/workflow-system-metadata";
import type { OperatorMetadata } from "../../api/backend-api";
import type { OperatorPredicate } from "../../types/workflow";

// A single synthetic operator type with one input and one output port, backed
// by a real (in-memory, no backend fetch) WorkflowSystemMetadata so the tools
// exercise the genuine schema-validation and predicate-construction paths.
const TEST_OP_SCHEMA = {
  type: "object",
  properties: {
    title: { type: "string" },
    count: { type: "number" },
  },
  required: [],
  additionalProperties: true,
};

const OPERATOR_METADATA: OperatorMetadata = {
  operators: [
    {
      operatorType: "TestOp",
      jsonSchema: TEST_OP_SCHEMA,
      operatorVersion: "1.0",
      additionalMetadata: {
        userFriendlyName: "Test Operator",
        operatorGroupName: "Test",
        operatorDescription: "A test operator",
        inputPorts: [{ displayName: "in-0" }],
        outputPorts: [{ displayName: "out-0" }],
        dynamicInputPorts: false,
        dynamicOutputPorts: false,
      },
    },
  ],
  groups: [],
};

function buildMetadataStore(): WorkflowSystemMetadata {
  const store = new WorkflowSystemMetadata();
  store.loadFromMetadata(OPERATOR_METADATA);
  return store;
}

const metadataStore = buildMetadataStore();
const context = { metadataStore };
const operatorSchemas = new Map<string, any>([["TestOp", TEST_OP_SCHEMA]]);

// Invoke a tool's execute() with the minimal ToolExecutionOptions the SDK requires.
async function runTool(agentTool: any, args: Record<string, any>): Promise<string> {
  return (await agentTool.execute(args, { toolCallId: "test-call", messages: [] })) as string;
}

function makeOperator(id: string, numInputPorts = 1, numOutputPorts = 1): OperatorPredicate {
  return {
    operatorID: id,
    operatorType: "TestOp",
    operatorVersion: "1.0",
    operatorProperties: {},
    inputPorts: Array.from({ length: numInputPorts }, (_, i) => ({ portID: `input-${i}` })),
    outputPorts: Array.from({ length: numOutputPorts }, (_, i) => ({ portID: `output-${i}` })),
    showAdvanced: false,
  };
}

describe("createDeleteOperatorTool", () => {
  test("deletes an existing operator and its connected links, leaving others intact", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1", 0, 1));
    state.addOperator(makeOperator("op2", 1, 1));
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op1", portID: "output-0" },
      target: { operatorID: "op2", portID: "input-0" },
    });

    const result = await runTool(createDeleteOperatorTool(state), { operatorId: "op1" });

    expect(result).toBe("Deleted operator: op1");
    expect(state.getOperator("op1")).toBeUndefined();
    expect(state.getAllLinks()).toHaveLength(0); // the connected link is removed too
    expect(state.getOperator("op2")).toBeDefined(); // the unrelated operator survives
  });

  test("returns an error and leaves state untouched when the operator does not exist", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    const result = await runTool(createDeleteOperatorTool(state), { operatorId: "ghost" });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Operator ghost not found");
    expect(state.getAllOperators()).toHaveLength(1);
  });
});

describe("createModifyOperatorTool", () => {
  test("returns an error when the operator does not exist", async () => {
    const state = new WorkflowState();

    const result = await runTool(createModifyOperatorTool(state), { operatorId: "ghost", summary: "x" });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Operator ghost not found");
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("updates properties and display name on an existing operator", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      properties: { title: "hello" },
      summary: "renamed",
    });

    expect(result).toBe("Operator op1 modified");
    const op = state.getOperator("op1")!;
    expect(op.operatorProperties.title).toBe("hello");
    expect(op.customDisplayName).toBe("renamed");
  });

  test("returns an error and leaves the operator unchanged when merged properties fail validation", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    const result = await runTool(createModifyOperatorTool(state, context), {
      operatorId: "op1",
      properties: { count: "not-a-number" },
      summary: "bad",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Invalid properties");
    // The tool returns before mutating: properties and display name stay as they were.
    const op = state.getOperator("op1")!;
    expect(op.operatorProperties.count).toBeUndefined();
    expect(op.customDisplayName).toBeUndefined();
  });

  test("replaces incoming links when inputOperatorIds is provided", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1", 0, 1));
    state.addOperator(makeOperator("op2", 1, 1));
    // Seed an existing link op0 --> op2 that the modify should replace.
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op0", portID: "output-0" },
      target: { operatorID: "op2", portID: "input-0" },
    });

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op2",
      inputOperatorIds: { "0": ["op1"] },
      summary: "re-link",
    });

    expect(result).toContain("Operator op2 modified");
    expect(result).toContain("deleted links: [op0 --> op2]");
    expect(result).toContain("created links: [op1 --> op2]");
    const links = state.getAllLinks();
    expect(links).toHaveLength(1);
    expect(links[0].source.operatorID).toBe("op1");
    expect(links[0].target.operatorID).toBe("op2");
  });
});

describe("createAddOperatorTool", () => {
  test("returns an error for an unknown operator type", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "NoSuchOp",
      properties: {},
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Unknown operator type");
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("returns an error when no metadata store is available", async () => {
    const state = new WorkflowState();

    // No context => workflowUtil is null even though the operator type is known.
    const result = await runTool(createAddOperatorTool(state, operatorSchemas), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Metadata store not available");
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("returns an error for an operatorId that does not match op<number>", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "not-valid",
      operatorType: "TestOp",
      properties: {},
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Invalid operatorId");
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("returns an error when the operator id already exists", async () => {
    const state = new WorkflowState();
    const addTool = createAddOperatorTool(state, operatorSchemas, context);
    await runTool(addTool, { operatorId: "op1", operatorType: "TestOp", properties: {}, summary: "first" });

    const result = await runTool(addTool, {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      summary: "again",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("already exists");
    expect(state.getAllOperators()).toHaveLength(1); // the duplicate is not added
  });

  test("returns an error when properties fail validation", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: { count: "not-a-number" },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Invalid properties");
    expect(state.getAllOperators()).toHaveLength(0);
  });

  test("returns an error when an input port index is out of range", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "5": ["whatever"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("out of range");
  });

  test("returns an error when an input port index is negative", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "-1": ["whatever"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("non-negative integer");
  });

  test("returns an error when an input port index is not a number", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { foo: ["whatever"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("non-negative integer");
  });

  test("returns an error when a referenced source operator does not exist", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "0": ["ghost"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Source operator");
    expect(result).toContain("not found");
  });

  test("adds a source operator with no input links", async () => {
    const state = new WorkflowState();

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: { title: "load" },
      summary: "source",
    });

    expect(result).toBe("Added operator op1, input ports: 1, output ports: 1");
    expect(state.getOperator("op1")).toBeDefined();
    expect(state.getAllLinks()).toHaveLength(0);
  });

  test("adds an operator and links it to an existing source", async () => {
    const state = new WorkflowState();
    const addTool = createAddOperatorTool(state, operatorSchemas, context);
    await runTool(addTool, { operatorId: "op1", operatorType: "TestOp", properties: {}, summary: "source" });

    const result = await runTool(addTool, {
      operatorId: "op2",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "0": ["op1"] },
      summary: "sink",
    });

    expect(result).toContain("Added operator op2");
    expect(result).toContain("created links: [op1 --> op2]");
    const links = state.getAllLinks();
    expect(links).toHaveLength(1);
    expect(links[0].source.operatorID).toBe("op1");
    expect(links[0].target.operatorID).toBe("op2");
  });
});
