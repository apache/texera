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

import { beforeEach, describe, expect, test } from "bun:test";
import { TexeraAgent } from "./texera-agent";
import { AgentState, INITIAL_STEP_ID, type ReActStep } from "../types/agent";

/**
 * These tests cover the agent's bookkeeping — the ReAct step tree, settings, and client set — which
 * `sendMessage` maintains but which is entirely separate from talking to a model. The constructor
 * only stores the model reference, so a placeholder is enough to build a real agent.
 */
function makeAgent(): TexeraAgent {
  return new TexeraAgent({
    model: {} as any,
    modelType: "test-model",
    agentId: "agent-1",
  });
}

function makeStep(id: string, parentId: string | undefined, extra: Partial<ReActStep> = {}): ReActStep {
  return {
    id,
    parentId,
    messageId: "msg-1",
    stepId: 0,
    timestamp: 0,
    role: "agent",
    content: "",
    isBegin: false,
    isEnd: false,
    ...extra,
  };
}

/** `addStep` and `head` are the agent's own internals; sendMessage is the only public writer. */
function addStep(agent: TexeraAgent, step: ReActStep): void {
  (agent as any).addStep(step);
}

function setHead(agent: TexeraAgent, id: string): void {
  (agent as any).head = id;
}

/** A step whose tool call reported a result, as the LLM loop would record it. */
function toolStep(id: string, toolName: string, output: unknown, isError = false): ReActStep {
  return makeStep(id, INITIAL_STEP_ID, {
    toolCalls: [{ toolName, toolCallId: "call-1", input: {} }],
    toolResults: [{ toolCallId: "call-1", output, isError }],
  });
}

describe("TexeraAgent", () => {
  let agent: TexeraAgent;

  beforeEach(() => {
    agent = makeAgent();
  });

  describe("construction", () => {
    test("starts available, at the initial step, with no steps of its own", () => {
      expect(agent.getState()).toBe(AgentState.AVAILABLE);
      expect(agent.getHead()).toBe(INITIAL_STEP_ID);
      expect(agent.getAllSteps()).toEqual([]);
      expect(agent.getVisibleReActSteps()).toEqual([]);
    });

    test("falls back to a name derived from the id", () => {
      expect(makeAgent().agentName).toBe("Agent-agent-1");
      expect(new TexeraAgent({ model: {} as any, modelType: "m", agentId: "a", agentName: "Named" }).agentName).toBe(
        "Named"
      );
    });
  });

  describe("getAncestorPath", () => {
    test("walks from the root down to the requested step", () => {
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      addStep(agent, makeStep("b", "a"));

      expect(agent.getAncestorPath("b")).toEqual([INITIAL_STEP_ID, "a", "b"]);
    });

    test("defaults to the current head", () => {
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      setHead(agent, "a");

      expect(agent.getAncestorPath()).toEqual([INITIAL_STEP_ID, "a"]);
    });

    test("returns an unknown id on its own rather than an empty path", () => {
      expect(agent.getAncestorPath("ghost")).toEqual(["ghost"]);
    });
  });

  describe("getVisibleReActSteps", () => {
    test("shows only the branch the head sits on", () => {
      // Two branches off the same parent: regenerating an answer leaves the abandoned branch in
      // stepsById, and it must not reappear in the conversation the user sees.
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      addStep(agent, makeStep("b", "a"));
      addStep(agent, makeStep("c", "a"));

      setHead(agent, "b");
      expect(agent.getVisibleReActSteps().map(s => s.id)).toEqual(["a", "b"]);

      setHead(agent, "c");
      expect(agent.getVisibleReActSteps().map(s => s.id)).toEqual(["a", "c"]);
    });

    test("hides the synthetic initial step", () => {
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      setHead(agent, "a");

      expect(agent.getVisibleReActSteps().map(s => s.id)).not.toContain(INITIAL_STEP_ID);
    });

    test("getAllSteps keeps the abandoned branch that getVisibleReActSteps drops", () => {
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      addStep(agent, makeStep("b", "a"));
      addStep(agent, makeStep("c", "a"));
      setHead(agent, "b");

      expect(
        agent
          .getAllSteps()
          .map(s => s.id)
          .sort()
      ).toEqual(["a", "b", "c"]);
    });
  });

  describe("getReActStepsByOperatorIds", () => {
    test("returns every step when no operator is named", () => {
      // An empty filter means "no filter", not "match nothing" — the opposite reading would make
      // the UI show an empty history whenever no operator is selected.
      addStep(agent, toolStep("s1", "addOperator", "Added operator op-7"));

      expect(agent.getReActStepsByOperatorIds([]).map(s => s.id)).toEqual(["s1"]);
    });

    test("matches an operator added through the tool's text output", () => {
      addStep(agent, toolStep("s1", "addOperator", "Added operator op-7"));
      addStep(agent, toolStep("s2", "addOperator", "Added operator op-8"));

      expect(agent.getReActStepsByOperatorIds(["op-7"]).map(s => s.id)).toEqual(["s1"]);
    });

    test("matches an operator modified through the tool's text output", () => {
      addStep(agent, toolStep("s1", "modifyOperator", "Operator op-8 modified"));

      expect(agent.getReActStepsByOperatorIds(["op-8"]).map(s => s.id)).toEqual(["s1"]);
    });

    test("matches an operator reported as structured JSON", () => {
      addStep(agent, toolStep("s1", "addCodeOperator", JSON.stringify({ operatorId: "op-9" })));

      expect(agent.getReActStepsByOperatorIds(["op-9"]).map(s => s.id)).toEqual(["s1"]);
    });

    test("ignores a tool result that failed", () => {
      addStep(agent, toolStep("s1", "addOperator", "Added operator op-7", true));

      expect(agent.getReActStepsByOperatorIds(["op-7"])).toEqual([]);
    });

    test("ignores an add message produced by a tool that does not add", () => {
      // The operator id is only trusted when the tool name agrees with the message, so a delete
      // tool echoing "Added operator" in its output cannot claim that operator.
      addStep(agent, toolStep("s1", "deleteOperator", "Added operator op-7"));

      expect(agent.getReActStepsByOperatorIds(["op-7"])).toEqual([]);
    });

    test("ignores a modify message produced by a tool that does not modify", () => {
      addStep(agent, toolStep("s1", "deleteOperator", "Operator op-8 modified"));

      expect(agent.getReActStepsByOperatorIds(["op-8"])).toEqual([]);
    });

    test("accepts JSON only from the exactly-named add and modify tools", () => {
      // The text path accepts any tool whose name contains "add", but the JSON path requires an
      // exact name. Pinning the asymmetry so that narrowing or widening one path is a visible change.
      addStep(agent, toolStep("s1", "addSomethingElse", JSON.stringify({ operatorId: "op-9" })));

      expect(agent.getReActStepsByOperatorIds(["op-9"])).toEqual([]);
    });

    test("skips a step with no tool results at all", () => {
      addStep(agent, makeStep("s1", INITIAL_STEP_ID, { content: "just talking" }));

      expect(agent.getReActStepsByOperatorIds(["op-7"])).toEqual([]);
    });
  });

  describe("settings", () => {
    test("applies only the fields given and leaves the rest alone", () => {
      const before = agent.getSettings();

      agent.updateSettings({ maxSteps: 99 });

      const after = agent.getSettings();
      expect(after.maxSteps).toBe(99);
      expect(after.toolTimeoutMs).toBe(before.toolTimeoutMs);
      expect(after.maxOperatorResultCharLimit).toBe(before.maxOperatorResultCharLimit);
    });

    test("returns a new settings object (mutating primitive fields does not stick)", () => {
      agent.getSettings().maxSteps = 12345;

      expect(agent.getSettings().maxSteps).not.toBe(12345);
    });

    test("a disabled tool is reported as disabled in the system info", () => {
      const toolName = agent.getSystemInfo().tools[0].name;

      agent.updateSettings({ disabledTools: new Set([toolName]) });

      const info = agent.getSystemInfo().tools.find(t => t.name === toolName);
      expect(info?.enabled).toBe(false);
    });

    test("restricting the allowed operator types rewrites the system prompt", () => {
      const before = agent.getSystemInfo().systemPrompt;

      agent.updateSettings({ allowedOperatorTypes: ["CSVFileScan"] });

      expect(agent.getSystemInfo().systemPrompt).not.toBe(before);
      expect(agent.getSettings().allowedOperatorTypes).toEqual(["CSVFileScan"]);
    });
  });

  describe("clients", () => {
    test("tracks and drops subscribers", () => {
      const ws = { id: 1 };

      agent.addClient(ws);
      expect(agent.getClients().has(ws)).toBe(true);

      agent.removeClient(ws);
      expect(agent.getClients().has(ws)).toBe(false);
    });
  });

  describe("lifecycle", () => {
    test("a new step is announced to the registered callback", () => {
      const seen: string[] = [];
      agent.setStepCallback(step => seen.push(step.id));

      addStep(agent, makeStep("a", INITIAL_STEP_ID));

      expect(seen).toEqual(["a"]);
    });

    test("clearHistory rewinds to the initial step but keeps the agent usable", () => {
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      setHead(agent, "a");

      agent.clearHistory();

      expect(agent.getHead()).toBe(INITIAL_STEP_ID);
      expect(agent.getAllSteps()).toEqual([]);
      expect(agent.getAncestorPath()).toEqual([INITIAL_STEP_ID]);
    });

    test("stop moves the agent into the stopping state", () => {
      agent.stop();

      expect(agent.getState()).toBe(AgentState.STOPPING);
    });

    test("destroy drops the steps and the subscribers", () => {
      addStep(agent, makeStep("a", INITIAL_STEP_ID));
      agent.addClient({ id: 1 });

      agent.destroy();

      expect(agent.getAllSteps()).toEqual([]);
      expect(agent.getReActSteps()).toEqual([]);
      expect(agent.getClients().size).toBe(0);
    });
  });
});
