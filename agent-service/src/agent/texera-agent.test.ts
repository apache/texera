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

import { describe, expect, test, afterEach, beforeAll } from "bun:test";
import { TexeraAgent } from "./texera-agent";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";
import { AgentState, INITIAL_STEP_ID } from "../types/agent";
import { ExecutionMode, type OperatorPredicate, type WorkflowContent } from "../types/workflow";

beforeAll(() => {
  // Seed the metadata singleton so the addOperator tool can build CSVFileScan.
  WorkflowSystemMetadata.getInstance().loadFromMetadata({
    operators: [
      {
        operatorType: "CSVFileScan",
        jsonSchema: { type: "object", properties: { fileName: { type: "string" } }, required: [] },
        additionalMetadata: { userFriendlyName: "CSV", operatorGroupName: "source", inputPorts: [], outputPorts: [{}] },
        operatorVersion: "1",
      },
    ],
    groups: [],
  });
});

// A LanguageModel is only used inside sendMessage(); the lifecycle/accessor
// methods exercised here never touch it, so a stub is sufficient.
function newAgent(agentName?: string) {
  return new TexeraAgent({ model: {} as any, modelType: "test-model", agentId: "a1", agentName });
}

describe("TexeraAgent lifecycle", () => {
  test("starts AVAILABLE with the initial step as HEAD and no history", () => {
    const agent = newAgent("Tester");
    expect(agent.agentId).toBe("a1");
    expect(agent.agentName).toBe("Tester");
    expect(agent.modelType).toBe("test-model");
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
    expect(agent.getHead()).toBe(INITIAL_STEP_ID);
    expect(agent.getReActSteps()).toEqual([]);
    expect(agent.getAllSteps()).toEqual([]);
  });

  test("derives a default name from the id when none is given", () => {
    expect(new TexeraAgent({ model: {} as any, modelType: "m", agentId: "x9" }).agentName).toBe("Agent-x9");
  });
});

describe("TexeraAgent delegation", () => {
  test("stores and returns its delegation", () => {
    const agent = newAgent();
    expect(agent.getDelegation()).toBeUndefined();
    agent.setDelegation({ userToken: "tok", workflowId: 12, computingUnitId: 3 });
    expect(agent.getDelegation()).toEqual({ userToken: "tok", workflowId: 12, computingUnitId: 3 });
  });

  test("exposes the executeOperator tool only after a delegation is set", () => {
    const agent = newAgent();
    expect(agent.getSystemInfo().tools.map(t => t.name)).not.toContain("executeOperator");
    agent.setDelegation({ userToken: "tok", workflowId: 1 });
    expect(agent.getSystemInfo().tools.map(t => t.name)).toContain("executeOperator");
  });
});

describe("TexeraAgent settings & history", () => {
  test("updates settings, including allowed operator types", () => {
    const agent = newAgent();
    agent.updateSettings({ maxSteps: 7, allowedOperatorTypes: ["Filter"] });
    const settings = agent.getSettings();
    expect(settings.maxSteps).toBe(7);
    expect(settings.allowedOperatorTypes).toEqual(["Filter"]);
  });

  test("getReActStepsByOperatorIds returns all steps when no ids are given", () => {
    const agent = newAgent();
    expect(agent.getReActStepsByOperatorIds([])).toEqual([]);
    expect(agent.getReActStepsByOperatorIds(["op1"])).toEqual([]);
  });

  test("clearHistory resets HEAD to the initial step", () => {
    const agent = newAgent();
    agent.clearHistory();
    expect(agent.getHead()).toBe(INITIAL_STEP_ID);
    expect(agent.getAllSteps()).toEqual([]);
  });

  test("checkout accepts the initial step and rejects an unknown one", () => {
    const agent = newAgent();
    expect(agent.checkout(INITIAL_STEP_ID)).toBe(true);
    expect(agent.checkout("nonexistent")).toBe(false);
  });

  test("destroy leaves the agent with no history", () => {
    const agent = newAgent();
    agent.destroy();
    expect(agent.getReActSteps()).toEqual([]);
  });
});

// --- Integration: drive a generation turn with a stub language model ---

const realFetch = globalThis.fetch;

const EMPTY_CONTENT: WorkflowContent = {
  operators: [],
  operatorPositions: {},
  links: [],
  commentBoxes: [],
  settings: { dataTransferBatchSize: 400, executionMode: ExecutionMode.PIPELINED },
};

// A LanguageModelV2 stub that returns a single text response and then stops.
function textModel(text: string = "done") {
  return {
    specificationVersion: "v2",
    provider: "mock",
    modelId: "mock",
    supportedUrls: {},
    async doGenerate() {
      return {
        content: [{ type: "text", text }],
        finishReason: "stop",
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        warnings: [],
      };
    },
    async doStream() {
      throw new Error("stream unused");
    },
  } as any;
}

function routeFetch(handler: (url: string, init: any) => Response): void {
  globalThis.fetch = (async (input: any, init: any) => handler(String(input), init)) as unknown as typeof fetch;
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { status: 200 });
}

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("TexeraAgent.sendMessage", () => {
  test("refreshes the delegated workflow and records a completed turn", async () => {
    routeFetch(url => {
      if (url.includes("/api/workflow/")) {
        return jsonResponse({ wid: 9, name: "WF", content: JSON.stringify(EMPTY_CONTENT) });
      }
      return jsonResponse({});
    });

    const agent = new TexeraAgent({ model: textModel("all set"), modelType: "m", agentId: "gen1" });
    agent.setDelegation({ userToken: "tok", workflowId: 9 });

    const result = await agent.sendMessage("hello");

    expect(result.stopped).toBe(false);
    expect(result.response).toBe("all set");
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
    const steps = agent.getReActSteps();
    expect(steps.some(s => s.role === "user" && s.content === "hello")).toBe(true);
    expect(steps.some(s => s.role === "agent" && s.isEnd)).toBe(true);
  });
});

function toolThenText(toolInput: Record<string, unknown>) {
  let calls = 0;
  return {
    specificationVersion: "v2",
    provider: "mock",
    modelId: "mock",
    supportedUrls: {},
    async doGenerate() {
      calls += 1;
      if (calls === 1) {
        return {
          content: [{ type: "tool-call", toolCallId: "c1", toolName: "addOperator", input: JSON.stringify(toolInput) }],
          finishReason: "tool-calls",
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
          warnings: [],
        };
      }
      return {
        content: [{ type: "text", text: "done" }],
        finishReason: "stop",
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        warnings: [],
      };
    },
    async doStream() {
      throw new Error("stream unused");
    },
  } as any;
}

describe("TexeraAgent.sendMessage with tool execution", () => {
  test("applies an addOperator tool call and runs post-step execution", async () => {
    routeFetch(url => {
      if (url.includes("/api/workflow/persist"))
        return jsonResponse({ wid: 9, name: "WF", content: JSON.stringify(EMPTY_CONTENT) });
      if (url.includes("/api/compile")) return jsonResponse({ operatorOutputSchemas: {}, operatorErrors: {} });
      if (url.includes("/api/execution/")) {
        return jsonResponse({
          success: true,
          state: "Completed",
          operators: {
            op1: {
              state: "Completed",
              inputTuples: 0,
              outputTuples: 1,
              resultMode: "table",
              result: [{ a: 1 }],
              totalRowCount: 1,
            },
          },
        });
      }
      if (url.includes("/api/workflow/"))
        return jsonResponse({ wid: 9, name: "WF", content: JSON.stringify(EMPTY_CONTENT) });
      return jsonResponse({});
    });

    const model = toolThenText({ operatorId: "op1", operatorType: "CSVFileScan", properties: {}, summary: "load csv" });
    const agent = new TexeraAgent({ model, modelType: "m", agentId: "tool1" });
    agent.setDelegation({ userToken: "tok", workflowId: 9 });

    const result = await agent.sendMessage("add a csv source");

    expect(result.response).toBe("done");
    expect(agent.getWorkflowState().getOperator("op1")).toBeDefined();
    // The post-step auto-execution stored a result snapshot for op1.
    expect(agent.getWorkflowResultState().getAllVisible().has("op1")).toBe(true);
    agent.destroy();
  });
});

describe("TexeraAgent auto-persist", () => {
  test("persists the workflow to the backend after a debounced edit", async () => {
    let persisted = false;
    routeFetch(url => {
      if (url.includes("/api/workflow/persist")) {
        persisted = true;
        return jsonResponse({ wid: 9, name: "WF", content: JSON.stringify(EMPTY_CONTENT) });
      }
      return jsonResponse({ wid: 9, name: "WF", content: JSON.stringify(EMPTY_CONTENT) });
    });

    const agent = new TexeraAgent({ model: textModel(), modelType: "m", agentId: "persist1" });
    agent.setDelegation({ userToken: "tok", workflowId: 9 });

    const operator: OperatorPredicate = {
      operatorID: "op1",
      operatorType: "CSVFileScan",
      operatorVersion: "1",
      operatorProperties: {},
      inputPorts: [],
      outputPorts: [{ portID: "output-0", displayName: "", disallowMultiInputs: false, isDynamicPort: false }],
      showAdvanced: false,
    } as OperatorPredicate;
    agent.getWorkflowState().addOperator(operator);

    await new Promise(resolve => setTimeout(resolve, 750));

    expect(persisted).toBe(true);
    agent.destroy();
  });
});
