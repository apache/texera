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
import { AgentState, INITIAL_STEP_ID } from "../types/agent";

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
