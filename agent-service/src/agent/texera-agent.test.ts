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
import { AgentState, INITIAL_STEP_ID } from "../types/agent";

// These tests exercise the agent's in-memory tree/settings/tool surface, which
// do not invoke the LLM, so a stub model is sufficient. The ReAct generation
// loop (sendMessage) talks to a real provider and is covered separately.
function newAgent(overrides: { agentName?: string } = {}): TexeraAgent {
  return new TexeraAgent({
    model: {} as LanguageModel,
    modelType: "test-model",
    agentId: "a1",
    agentName: overrides.agentName ?? "Bob",
  });
}

describe("TexeraAgent - construction", () => {
  test("starts AVAILABLE with the initial step as head and no visible steps", () => {
    const agent = newAgent();
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
    expect(agent.getHead()).toBe(INITIAL_STEP_ID);
    expect(agent.getAllSteps()).toEqual([]);
    expect(agent.getReActSteps()).toEqual([]);
    expect(agent.getAncestorPath()).toEqual([INITIAL_STEP_ID]);
  });

  test("exposes identity fields from the config", () => {
    const agent = newAgent({ agentName: "Ada" });
    expect(agent.agentId).toBe("a1");
    expect(agent.agentName).toBe("Ada");
    expect(agent.modelType).toBe("test-model");
    expect(agent.createdAt).toBeInstanceOf(Date);
  });
});

describe("TexeraAgent - settings", () => {
  test("returns the documented defaults", () => {
    const settings = newAgent().getSettings();
    expect(settings.maxSteps).toBe(100);
    expect(settings.maxOperatorResultCharLimit).toBe(2000);
  });

  test("updateSettings applies numeric overrides", () => {
    const agent = newAgent();
    agent.updateSettings({ maxSteps: 5, maxOperatorResultCharLimit: 123, toolTimeoutMs: 1000 });
    const settings = agent.getSettings();
    expect(settings.maxSteps).toBe(5);
    expect(settings.maxOperatorResultCharLimit).toBe(123);
    expect(settings.toolTimeoutMs).toBe(1000);
  });

  test("changing allowedOperatorTypes rebuilds the system prompt", () => {
    const agent = newAgent();
    agent.updateSettings({ allowedOperatorTypes: ["Filter"] });
    expect(agent.getSettings().allowedOperatorTypes).toEqual(["Filter"]);
    // The rebuilt prompt always contains the base template header.
    expect(agent.getSystemInfo().systemPrompt).toContain("You are a data science Copilot");
  });
});

describe("TexeraAgent - tool surface", () => {
  test("without a delegate, exposes only the CRUD tools", () => {
    const names = newAgent()
      .getSystemInfo()
      .tools.map(t => t.name)
      .sort();
    expect(names).toEqual(["addOperator", "deleteOperator", "modifyOperator"]);
  });

  test("setting a delegate config adds the execute-operator tool", () => {
    const agent = newAgent();
    expect(agent.getDelegateConfig()).toBeUndefined();

    agent.setDelegateConfig({ userToken: "tok", workflowId: 1, workflowName: "wf" });
    expect(agent.getDelegateConfig()?.workflowId).toBe(1);

    const names = agent
      .getSystemInfo()
      .tools.map(t => t.name)
      .sort();
    expect(names).toContain("executeOperator");
    expect(names).toHaveLength(4);

    agent.destroy(); // tears down the persistence subscription
  });
});

describe("TexeraAgent - history and checkout", () => {
  test("checkout rejects an unknown step but accepts the initial step", () => {
    const agent = newAgent();
    expect(agent.checkout("does-not-exist")).toBe(false);
    expect(agent.checkout(INITIAL_STEP_ID)).toBe(true);
    expect(agent.getHead()).toBe(INITIAL_STEP_ID);
  });

  test("getReActStepsByOperatorIds returns empty when there is no history", () => {
    const agent = newAgent();
    expect(agent.getReActStepsByOperatorIds([])).toEqual([]);
    expect(agent.getReActStepsByOperatorIds(["op1"])).toEqual([]);
  });

  test("clearHistory resets head to the initial step", () => {
    const agent = newAgent();
    agent.clearHistory();
    expect(agent.getHead()).toBe(INITIAL_STEP_ID);
    expect(agent.getReActSteps()).toEqual([]);
  });
});

describe("TexeraAgent - lifecycle", () => {
  test("stop transitions the state to STOPPING", () => {
    const agent = newAgent();
    agent.stop();
    expect(agent.getState()).toBe(AgentState.STOPPING);
  });

  test("destroy clears history and is safe to call", () => {
    const agent = newAgent();
    expect(() => agent.destroy()).not.toThrow();
    expect(agent.getReActSteps()).toEqual([]);
  });

  test("exposes its workflow and result state objects", () => {
    const agent = newAgent();
    expect(agent.getWorkflowState()).toBeDefined();
    expect(agent.getWorkflowResultState()).toBeDefined();
    expect(agent.getMetadataStore()).toBeDefined();
  });
});
