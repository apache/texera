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
import { TexeraAgent } from "./texera-agent";
import type { LanguageModel } from "ai";
import { INITIAL_STEP_ID, OperatorResultSerializationMode } from "../types/agent";
import type { AgentSnapshot } from "../types/agent";
import type { WorkflowContent } from "../types/workflow";

function newAgent(createdAt?: Date): TexeraAgent {
  return new TexeraAgent({
    model: {} as LanguageModel,
    modelType: "test-model",
    agentId: "agent-x",
    agentName: "Bob",
    createdAt,
  });
}

const sampleWorkflow: WorkflowContent = {
  operators: [
    {
      operatorID: "op1",
      operatorType: "Filter",
      operatorVersion: "1.0",
      operatorProperties: { predicate: "x > 1" },
      inputPorts: [{ portID: "input-0", displayName: "in" }],
      outputPorts: [{ portID: "output-0", displayName: "out" }],
      showAdvanced: false,
    },
  ],
  operatorPositions: { op1: { x: 10, y: 20 } },
  links: [],
  commentBoxes: [],
  settings: { dataTransferBatchSize: 400 },
};

function sampleSnapshot(): AgentSnapshot {
  return {
    version: 1,
    agentId: "agent-x",
    agentName: "Bob",
    modelType: "test-model",
    createdAt: "2024-01-01T00:00:00.000Z",
    head: "s1",
    stepCounter: 3,
    messageCounter: 2,
    settings: {
      disabledTools: ["executeOperator"],
      maxOperatorResultCharLimit: 1500,
      maxOperatorResultCellCharLimit: 800,
      operatorResultSerializationMode: OperatorResultSerializationMode.TSV,
      toolTimeoutMs: 120000,
      executionTimeoutMs: 180000,
      maxSteps: 42,
      allowedOperatorTypes: ["Filter", "CSVFileScan"],
    },
    delegate: {
      userInfo: { uid: 1, name: "user", email: "u@example.com", role: "REGULAR" },
      workflowId: 7,
      workflowName: "wf",
      computingUnitId: 3,
    },
    steps: [
      {
        id: INITIAL_STEP_ID,
        messageId: "initial",
        stepId: -1,
        timestamp: 0,
        role: "user",
        content: "",
        isBegin: true,
        isEnd: true,
      },
      {
        id: "s1",
        parentId: INITIAL_STEP_ID,
        messageId: "m1",
        stepId: 0,
        timestamp: 123,
        role: "user",
        content: "build a filter",
        isBegin: true,
        isEnd: true,
      },
    ],
    messageGroups: { m1: ["s1"] },
    workflowContent: sampleWorkflow,
  };
}

describe("TexeraAgent.toSnapshot", () => {
  test("captures a fresh agent: version, empty history, initial head", () => {
    const snap = newAgent().toSnapshot();
    expect(snap.version).toBe(1);
    expect(snap.agentId).toBe("agent-x");
    expect(snap.head).toBe(INITIAL_STEP_ID);
    // Only the sentinel initial step exists.
    expect(snap.steps).toHaveLength(1);
    expect(snap.steps[0].id).toBe(INITIAL_STEP_ID);
    expect(snap.messageGroups).toEqual({});
    expect(snap.delegate).toBeUndefined();
  });

  test("preserves createdAt as an ISO string", () => {
    const snap = newAgent(new Date("2024-01-01T00:00:00.000Z")).toSnapshot();
    expect(snap.createdAt).toBe("2024-01-01T00:00:00.000Z");
  });
});

describe("TexeraAgent.restoreFromSnapshot", () => {
  test("restores conversation, settings, workflow, and delegate", () => {
    const snap = sampleSnapshot();
    const agent = newAgent(new Date(snap.createdAt));
    agent.restoreFromSnapshot(snap);

    expect(agent.getHead()).toBe("s1");
    expect(agent.getReActSteps().map(s => s.id)).toEqual(["s1"]);
    expect(agent.getAllSteps().map(s => s.id)).toEqual(["s1"]); // excludes the initial sentinel

    const settings = agent.getSettings();
    expect(settings.maxSteps).toBe(42);
    expect(settings.maxOperatorResultCharLimit).toBe(1500);
    expect(Array.from(settings.disabledTools)).toEqual(["executeOperator"]);

    expect(
      agent
        .getWorkflowState()
        .getWorkflowContent()
        .operators.map(o => o.operatorID)
    ).toEqual(["op1"]);

    // Delegate metadata is restored, but the user token is not persisted.
    expect(agent.getDelegateConfig()?.workflowId).toBe(7);
    expect(agent.getDelegateConfig()?.userToken).toBe("");
  });

  test("round-trips: toSnapshot(restore(s)) deep-equals s", () => {
    const snap = sampleSnapshot();
    const agent = newAgent(new Date(snap.createdAt));
    agent.restoreFromSnapshot(snap);

    // Survives a JSON round-trip as it would on disk.
    const roundTripped = JSON.parse(JSON.stringify(agent.toSnapshot())) as AgentSnapshot;
    expect(roundTripped).toEqual(snap);
  });

  test("rejects an unsupported snapshot version", () => {
    const snap = { ...sampleSnapshot(), version: 2 as unknown as 1 };
    expect(() => newAgent().restoreFromSnapshot(snap)).toThrow(/Unsupported agent snapshot version/);
  });
});
