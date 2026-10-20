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
    {
      operatorType: "TestJoinOp",
      jsonSchema: TEST_OP_SCHEMA,
      operatorVersion: "1.0",
      additionalMetadata: {
        userFriendlyName: "Test Join Operator",
        operatorGroupName: "Test",
        operatorDescription: "A test operator with two input ports",
        inputPorts: [{ displayName: "in-0" }, { displayName: "in-1" }],
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
const operatorSchemas = new Map<string, any>([
  ["TestOp", TEST_OP_SCHEMA],
  ["TestJoinOp", TEST_OP_SCHEMA],
]);

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

  test("applies properties that pass metadata validation", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    // Same validating context as the failure case above, but with values the
    // schema accepts, so the tool falls through the validation guard.
    const result = await runTool(createModifyOperatorTool(state, context), {
      operatorId: "op1",
      properties: { title: "kept", count: 7 },
      summary: "valid",
    });

    expect(result).toBe("Operator op1 modified");
    const op = state.getOperator("op1")!;
    expect(op.operatorProperties.title).toBe("kept");
    expect(op.operatorProperties.count).toBe(7);
    expect(op.customDisplayName).toBe("valid");
  });

  test("returns an error when an input port index is not a number", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1"));
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op0", portID: "output-0" },
      target: { operatorID: "op1", portID: "input-0" },
    });

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      inputOperatorIds: { foo: ["op0"] },
      summary: "bad port",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("non-negative integer");
    // The port indices are validated before anything changes, so the failed
    // modify keeps the existing link and doesn't rename the operator.
    expect(state.getAllLinks().map(link => link.linkID)).toEqual(["l0"]);
    expect(state.getOperator("op1")!.customDisplayName).toBeUndefined();
  });

  test("returns an error when an input port index is negative", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      inputOperatorIds: { "-1": ["op1"] },
      summary: "bad port",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("non-negative integer");
    expect(state.getAllLinks()).toHaveLength(0);
  });

  test("returns an error when an input port index is out of range", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1", 1, 1));

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      inputOperatorIds: { "3": ["op0"] },
      summary: "bad port",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Input port index 3 out of range");
    expect(result).toContain("has 1 input port(s)");
    expect(state.getAllLinks()).toHaveLength(0);
  });

  test.each(["1abc", "1.5"])("returns an error when an input port index is not all digits: %p", async portKey => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1", 2, 1));

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      inputOperatorIds: { [portKey]: ["op0"] },
      summary: "bad port",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("non-negative integer");
    // parseInt alone would read the key as port 1, which exists here.
    expect(state.getAllLinks()).toHaveLength(0);
  });

  test("returns an error when a referenced source operator does not exist", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      inputOperatorIds: { "0": ["ghost"] },
      summary: "bad source",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain('Source operator "ghost" not found');
    expect(state.getAllLinks()).toHaveLength(0);
  });

  test("returns an error and keeps the links when the operator names itself as a source", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1"));
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op0", portID: "output-0" },
      target: { operatorID: "op1", portID: "input-0" },
    });
    const before = structuredClone(state.getWorkflowContent());

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      inputOperatorIds: { "0": ["op1"] },
      summary: "self",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain('Operator "op1" cannot be its own input source');
    expect(state.getWorkflowContent()).toEqual(before);
  });

  test("leaves properties, display name and links unchanged when a new source does not exist", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator({
      ...makeOperator("op1"),
      operatorProperties: { title: "original" },
      customDisplayName: "original",
    });
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op0", portID: "output-0" },
      target: { operatorID: "op1", portID: "input-0" },
    });
    const before = structuredClone(state.getWorkflowContent());

    const result = await runTool(createModifyOperatorTool(state, context), {
      operatorId: "op1",
      properties: { title: "changed" },
      inputOperatorIds: { "0": ["ghost"] },
      summary: "changed",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain('Source operator "ghost" not found');
    expect(state.getWorkflowContent()).toEqual(before);
  });

  test("leaves the existing links unchanged when a later port names a missing source", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1", 0, 1));
    state.addOperator(makeOperator("op2", 2, 1));
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op0", portID: "output-0" },
      target: { operatorID: "op2", portID: "input-0" },
    });
    state.addLink({
      linkID: "l1",
      source: { operatorID: "op1", portID: "output-0" },
      target: { operatorID: "op2", portID: "input-1" },
    });
    const before = structuredClone(state.getWorkflowContent());

    // Port 0 is valid and would be relinked first; port 1 names a source that does not exist.
    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op2",
      inputOperatorIds: { "0": ["op1"], "1": ["ghost"] },
      summary: "re-link",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain('Source operator "ghost" not found');
    expect(state.getWorkflowContent()).toEqual(before);
  });

  test("emits no change events when the call fails", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1"));
    state.addLink({
      linkID: "l0",
      source: { operatorID: "op0", portID: "output-0" },
      target: { operatorID: "op1", portID: "input-0" },
    });
    const events: unknown[] = [];
    const subscription = state.getWorkflowChangedStream().subscribe(event => events.push(event));

    const result = await runTool(createModifyOperatorTool(state, context), {
      operatorId: "op1",
      properties: { title: "changed" },
      inputOperatorIds: { "0": ["ghost"] },
      summary: "changed",
    });
    subscription.unsubscribe();

    expect(result).toContain("[ERROR]");
    // Subscribers such as auto-persist see nothing from a failed call.
    expect(events).toHaveLength(0);
  });

  test("wraps an unexpected state failure with the operator id", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    // Force the state layer to blow up so the tool's catch block is exercised.
    state.updateOperatorProperties = () => {
      throw new Error("state exploded");
    };

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      properties: { title: "x" },
      summary: "boom",
    });

    expect(result).toBe("[ERROR] Error on operator op1: state exploded");
  });

  test("falls back to the stringified value when a thrown error has no message", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op1"));
    state.updateOperatorProperties = () => {
      throw "plain string failure";
    };

    const result = await runTool(createModifyOperatorTool(state), {
      operatorId: "op1",
      properties: { title: "x" },
      summary: "boom",
    });

    expect(result).toBe("[ERROR] Error on operator op1: plain string failure");
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

  test.each(["1abc", "1.5"])("returns an error when an input port index is not all digits: %p", async portKey => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    const before = structuredClone(state.getWorkflowContent());

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestJoinOp",
      properties: {},
      inputOperatorIds: { [portKey]: ["op0"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("non-negative integer");
    expect(state.getWorkflowContent()).toEqual(before);
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

  test("returns an error when the operator names itself as a source", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    const before = structuredClone(state.getWorkflowContent());

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "0": ["op1"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain('Operator "op1" cannot be its own input source');
    expect(state.getWorkflowContent()).toEqual(before);
  });

  test("leaves the workflow unchanged when an input port index is out of range", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    const before = structuredClone(state.getWorkflowContent());

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "5": ["op0"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain("Input port index 5 out of range");
    expect(state.getWorkflowContent()).toEqual(before);
  });

  test("leaves the workflow unchanged when a later port names a missing source", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    const before = structuredClone(state.getWorkflowContent());

    // Port 0 is valid and would be linked first; port 1 names a source that does not exist.
    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestJoinOp",
      properties: {},
      inputOperatorIds: { "0": ["op0"], "1": ["ghost"] },
      summary: "x",
    });

    expect(result).toContain("[ERROR]");
    expect(result).toContain('Source operator "ghost" not found');
    expect(state.getWorkflowContent()).toEqual(before);
  });

  test("emits no change events when the call fails", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    const events: unknown[] = [];
    const subscription = state.getWorkflowChangedStream().subscribe(event => events.push(event));

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestJoinOp",
      properties: {},
      inputOperatorIds: { "0": ["op0"], "1": ["ghost"] },
      summary: "x",
    });
    subscription.unsubscribe();

    expect(result).toContain("[ERROR]");
    expect(events).toHaveLength(0);
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

  test("links each input port of a multi-port operator", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    state.addOperator(makeOperator("op1", 0, 1));

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op2",
      operatorType: "TestJoinOp",
      properties: {},
      inputOperatorIds: { "0": ["op0"], "1": ["op1"] },
      summary: "join",
    });

    expect(result).toBe(
      "Added operator op2, input ports: 2, output ports: 1, created links: [op0 --> op2, op1 --> op2]"
    );
    expect(state.getAllLinks().map(link => [link.source.operatorID, link.target.portID])).toEqual([
      ["op0", "input-0"],
      ["op1", "input-1"],
    ]);
  });

  test("accepts the same operator id again after a failed add", async () => {
    const state = new WorkflowState();
    state.addOperator(makeOperator("op0", 0, 1));
    const addTool = createAddOperatorTool(state, operatorSchemas, context);
    const failed = await runTool(addTool, {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "0": ["ghost"] },
      summary: "x",
    });
    expect(failed).toContain("[ERROR]");

    const result = await runTool(addTool, {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      inputOperatorIds: { "0": ["op0"] },
      summary: "x",
    });

    expect(result).toContain("Added operator op1");
    expect(result).toContain("created links: [op0 --> op1]");
  });

  test("reports an unexpected state failure without adding the operator", async () => {
    const state = new WorkflowState();
    // Force the state layer to blow up so the tool's catch block is exercised.
    state.addOperator = () => {
      throw new Error("insert failed");
    };

    const result = await runTool(createAddOperatorTool(state, operatorSchemas, context), {
      operatorId: "op1",
      operatorType: "TestOp",
      properties: {},
      summary: "boom",
    });

    // Unlike modifyOperator, addOperator reports the raw message with no id prefix.
    expect(result).toBe("[ERROR] insert failed");
    expect(state.getAllOperators()).toHaveLength(0);
  });
});
