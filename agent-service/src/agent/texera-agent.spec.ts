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

import { afterEach, beforeEach, describe, expect, spyOn, test } from "bun:test";
import { TexeraAgent } from "./texera-agent";
import { AgentState, INITIAL_STEP_ID, type ReActStep } from "../types/agent";
import { MockLanguageModelV4 } from "ai/test";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";

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

/**
 * LanguageModelV4FinishReason is an object, not a string: a bare `finishReason: "stop"` runs fine
 * under `bun test` but fails `tsc --noEmit`, so the mocks below build it through here.
 */
const finish = (unified: "stop" | "tool-calls") => ({ unified, raw: undefined });

/**
 * LanguageModelV4Usage is nested. A flat `{ inputTokens: 11 }` is silently discarded, and every
 * usage assertion then passes against a gutted mapping — so usage is always built through here.
 * `totalTokens` is deliberately absent: the SDK derives it.
 */
const usage = (i: number, o: number) => ({
  inputTokens: { total: i, noCache: i, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: o, text: o, reasoning: 0 },
});

const textModel = (text: string, i = 11, o = 7) =>
  new MockLanguageModelV4({
    doGenerate: async () => ({
      content: [{ type: "text" as const, text }],
      // `as const` on both: without it these widen to `string` and the mock no longer satisfies
      // LanguageModelV4GenerateResult, which `bun test` accepts but `tsc --noEmit` rejects.
      finishReason: finish("stop"),
      usage: usage(i, o),
      warnings: [],
    }),
  });

/** Sibling of makeAgent() that takes a stand-in model, so sendMessage runs with no network.
 *  Every agent built here is tracked and destroyed from `afterEach` rather than per test: a
 *  failing assertion would skip an in-test `destroy()`, and a pending auto-persist debounce
 *  could then fire after the fetch spy is restored and issue a real request. */
const liveAgents: TexeraAgent[] = [];
function makeAgentWith(model: any): TexeraAgent {
  const agent = new TexeraAgent({ model, modelType: "test-model", agentId: "agent-1", systemPrompt: "SYS-XYZ" });
  liveAgents.push(agent);
  return agent;
}

/** A source operator. `inputPorts: []` matters — a non-empty one fails validateOperatorConnection
 *  and masks the delegate-mode assertions behind a validation error. */
const srcOp = (id: string, props: Record<string, any> = {}) => ({
  operatorID: id,
  operatorType: "CSVFileScan",
  operatorVersion: "1.0",
  operatorProperties: props,
  inputPorts: [],
  outputPorts: [{ portID: "output-0" }],
  showAdvanced: false,
});

/**
 * A tripwire rather than a stub: with an empty workflow and no delegate config, sendMessage makes
 * no network calls at all, and the tests below assert that. Tests that do need I/O install their
 * own implementation over it.
 */
let fetchSpy: any;
let urls: string[];

beforeEach(() => {
  urls = [];
  fetchSpy = spyOn(globalThis, "fetch").mockImplementation((async (u: any) => {
    urls.push(String(u));
    throw new Error("unexpected fetch");
  }) as any);
});
afterEach(() => {
  // Destroy before restoring the spy — destruction is what cancels a pending auto-persist.
  for (const agent of liveAgents.splice(0)) agent.destroy();
  fetchSpy.mockRestore();
});

/**
 * sendMessage was the largest untested region in the file. Everything it needs is in-process:
 * `ai/test` supplies a model that never reaches the network, so the ReAct loop, its usage
 * accounting, its branch bookkeeping and its failure paths can all be driven directly.
 *
 * One shape to know when adding tests here: a tool call's `input` must be a JSON *string*, e.g.
 * `{ type: "tool-call", toolCallId: "c1", toolName: "modifyOperator", input: JSON.stringify(...) }`.
 */
describe("sendMessage", () => {
  test("records the turn as a linear two-step branch", async () => {
    const model = textModel("hello there");
    const agent = makeAgentWith(model);
    const res = await agent.sendMessage("12345678", "feedback");
    const steps = agent.getAllSteps();
    expect(res.response).toBe("hello there");
    expect(res.stopped).toBe(false);
    expect(res.error).toBeUndefined();
    expect(res.messages).toEqual([{ role: "assistant", content: [{ type: "text", text: "hello there" }] }]);
    expect(res.usage).toEqual({ inputTokens: 11, outputTokens: 7, totalTokens: 18 });
    expect(steps.map(s => [s.role, s.stepId, s.content, s.isBegin, s.isEnd])).toEqual([
      ["user", 0, "12345678", true, true],
      ["agent", 1, "hello there", true, true],
    ]);
    expect(steps[0].usage).toEqual({ inputTokens: 2, outputTokens: 0, totalTokens: 2 });
    expect(steps[0].messageSource).toBe("feedback");
    expect(steps[0].parentId).toBe(INITIAL_STEP_ID);
    expect(steps[1].parentId).toBe(steps[0].id);
    expect(agent.getHead()).toBe(steps[1].id);
    expect(agent.getAncestorPath()).toEqual([INITIAL_STEP_ID, steps[0].id, steps[1].id]);
    expect(steps[0].id).toMatch(/^step-agent-1-1-\d+$/);
    expect(steps[1].id).toMatch(/^step-agent-1-2-\d+$/);
    expect(steps[0].messageId).toMatch(/^msg-agent-1-1-\d+$/);
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
    expect((agent as any).abortController).toBeNull();
    expect(urls).toEqual([]);
  });

  test("pinned call options + assembled context replaces the raw message", async () => {
    const model = textModel("ok");
    const agent = makeAgentWith(model);
    await agent.sendMessage("raw-user-text");
    const call = (model as any).doGenerateCalls[0];
    expect(call.temperature).toBe(0.2);
    expect(call.providerOptions).toEqual({
      openai: { parallelToolCalls: false },
      anthropic: { disableParallelToolUse: true },
      mistral: { parallelToolCalls: false },
    });
    expect(call.abortSignal).toBeInstanceOf(AbortSignal);
    expect(call.abortSignal.aborted).toBe(false);
    expect(call.tools.map((t: any) => t.name)).toEqual(["deleteOperator", "addOperator", "modifyOperator"]);
    expect(call.prompt[0]).toEqual({ role: "system", content: "SYS-XYZ" });
    const txt = call.prompt[1].content[0].text;
    expect(txt).toContain("# Ongoing Task");
    expect(txt).toContain("raw-user-text");
    expect(agent.getAllSteps()[1].inputMessages).toEqual([{ role: "user", content: txt }]);
  });

  test("two-step tool run: per-step + summed usage, tool projection, rolling snapshots", async () => {
    let n = 0;
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        n++;
        if (n === 1)
          return {
            content: [
              {
                type: "tool-call",
                toolCallId: "c1",
                toolName: "deleteOperator",
                input: JSON.stringify({ operatorId: "ghost" }),
              },
            ],
            finishReason: finish("tool-calls"),
            usage: usage(100, 10),
            warnings: [],
          } as any;
        return {
          content: [{ type: "text", text: "done" }],
          finishReason: finish("stop"),
          usage: usage(200, 20),
          warnings: [],
        } as any;
      },
    });
    const agent = makeAgentWith(model);
    const res = await agent.sendMessage("delete it");
    const steps = agent.getAllSteps();
    expect(res.usage).toEqual({ inputTokens: 300, outputTokens: 30, totalTokens: 330 });
    expect(res.response).toBe("done");
    expect(steps[1].usage).toEqual({ inputTokens: 100, outputTokens: 10, totalTokens: 110 });
    expect(steps[2].usage).toEqual({ inputTokens: 200, outputTokens: 20, totalTokens: 220 });
    expect(steps[1].toolCalls).toEqual([
      { toolName: "deleteOperator", toolCallId: "c1", input: { operatorId: "ghost" } },
    ]);
    expect(steps[1].toolResults).toEqual([
      { toolCallId: "c1", output: "[ERROR] Operator ghost not found", isError: false },
    ]);
    expect(steps[1].content).toBe("");
    expect(steps.map(s => [s.stepId, s.isBegin, s.isEnd])).toEqual([
      [0, true, true],
      [1, true, false],
      [2, false, true],
    ]);
    expect(steps[2].parentId).toBe(steps[1].id);
    expect(urls).toEqual([]);
  });

  test("rolling before/after snapshots across a mutating step", async () => {
    let n = 0;
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        n++;
        if (n === 1)
          return {
            content: [
              {
                type: "tool-call",
                toolCallId: "c1",
                toolName: "deleteOperator",
                input: JSON.stringify({ operatorId: "op1" }),
              },
            ],
            finishReason: finish("tool-calls"),
            usage: usage(1, 1),
            warnings: [],
          } as any;
        return {
          content: [{ type: "text", text: "x" }],
          finishReason: finish("stop"),
          usage: usage(1, 1),
          warnings: [],
        } as any;
      },
    });
    const agent = makeAgentWith(model);
    agent.getWorkflowState().addOperator(srcOp("op1") as any);
    fetchSpy.mockImplementation((async (u: any) => {
      urls.push(String(u));
      return { ok: true, json: async () => ({ operatorOutputSchemas: {}, operatorErrors: {} }) } as any;
    }) as any);
    await agent.sendMessage("del");
    const counts = agent
      .getAllSteps()
      .map(s => [s.role, s.beforeWorkflowContent?.operators.length, s.afterWorkflowContent?.operators.length]);
    expect(counts).toEqual([
      ["user", 1, 1],
      ["agent", 1, 0],
      ["agent", 0, 0],
    ]);
  });

  test("caps the loop at maxSteps", async () => {
    // The cap is the only thing standing between a looping model and an unbounded run.
    let n = 0;
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        n++;
        if (n > 20) throw new Error("runaway");
        return {
          content: [
            {
              type: "tool-call",
              toolCallId: "c" + n,
              toolName: "deleteOperator",
              input: JSON.stringify({ operatorId: "ghost" }),
            },
          ],
          finishReason: finish("tool-calls"),
          usage: usage(1, 1),
          warnings: [],
        } as any;
      },
    });
    const agent = makeAgentWith(model);
    agent.updateSettings({ maxSteps: 2 });
    await agent.sendMessage("go");
    expect(n).toBe(2);
    expect(agent.getAllSteps().map(s => s.stepId)).toEqual([0, 1, 2]);
  });

  test("second turn chains on and reads turn 1 as completed", async () => {
    const model = textModel("a", 1, 1);
    const agent = makeAgentWith(model);
    await agent.sendMessage("one");
    const headAfter1 = agent.getHead();
    await agent.sendMessage("two");
    const steps = agent.getAllSteps();
    expect(steps.map(s => s.stepId)).toEqual([0, 1, 0, 1]);
    expect(steps[2].parentId).toBe(headAfter1);
    expect(steps[0].messageId).not.toBe(steps[2].messageId);
    expect(steps[2].messageId).toMatch(/^msg-agent-1-2-\d+$/);
    expect(steps[3].id).toMatch(/^step-agent-1-4-\d+$/);
    const txt = (model as any).doGenerateCalls[1].prompt[1].content[0].text;
    expect(txt).toContain("# Completed Tasks");
    expect(txt).toContain("## Task (completed)");
    expect(txt).toContain("# Ongoing Task");
  });

  test("abandoned branch is invisible to the model", async () => {
    // Branches are how a retried turn discards its predecessor; if the abandoned one still reached
    // the prompt the model would answer against history the user already rejected.
    const model = textModel("a", 1, 1);
    const agent = makeAgentWith(model);
    await agent.sendMessage("first");
    (agent as any).head = agent.getAllSteps()[0].id;
    await agent.sendMessage("second");
    const txt = (model as any).doGenerateCalls[1].prompt[1].content[0].text;
    expect(txt).not.toContain("### Turn 1");
    expect(agent.getAllSteps().length).toBe(4);
    expect(agent.getVisibleReActSteps().length).toBe(3);
  });

  test("compiles the DAG and feeds schemas + cached results into the prompt", async () => {
    const model = textModel("ok", 1, 1);
    const agent = makeAgentWith(model);
    agent.getWorkflowState().addOperator(srcOp("op-1", { fileName: "f.csv" }) as any);
    agent.getWorkflowResultState().set("op-1", INITIAL_STEP_ID, {
      state: "COMPLETED",
      inputTuples: 0,
      outputTuples: 2,
      resultMode: "SET_SNAPSHOT",
      result: [{ a: 1 }, { a: 2 }],
    } as any);
    fetchSpy.mockImplementation((async (u: any) => {
      urls.push(String(u));
      return {
        ok: true,
        json: async () => ({
          operatorOutputSchemas: { "op-1": { "0_0": [{ attributeName: "a", attributeType: "integer" }] } },
          operatorErrors: {},
        }),
      } as any;
    }) as any);
    const res = await agent.sendMessage("look");
    const txt = (model as any).doGenerateCalls[0].prompt[1].content[0].text;
    expect(res.error).toBeUndefined();
    expect(urls).toEqual(["http://localhost:9090/api/compile"]);
    expect(txt).toContain("Output Schema: [a: integer]");
    expect(txt).toContain("(CSVFileScan, executed)");
    expect(txt).toContain("Result:\n  Executed operator op-1\n  Output table shape: (2, 1)");
    expect(txt).toContain("Properties:\n  fileName: f.csv");
  });

  test("swallows a plan-build failure and still answers", async () => {
    const model = textModel("still-here", 1, 1);
    const agent = makeAgentWith(model);
    const ws = agent.getWorkflowState();
    ws.addOperator({ ...srcOp("src"), outputPorts: undefined } as any);
    ws.addOperator({ ...srcOp("dst"), inputPorts: [{ portID: "input-0" }] } as any);
    ws.addLink({
      linkID: "l1",
      source: { operatorID: "src", portID: "output-0" },
      target: { operatorID: "dst", portID: "input-0" },
    } as any);
    const res = await agent.sendMessage("hi");
    expect(res.response).toBe("still-here");
    expect(res.error).toBeUndefined();
  });

  test("reports a model failure as an error step", async () => {
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw new Error("model exploded");
      },
    });
    const agent = makeAgentWith(model);
    const res = await agent.sendMessage("hi");
    const steps = agent.getAllSteps();
    expect(res).toEqual({
      response: "",
      messages: [],
      usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
      stopped: false,
      error: "model exploded",
    });
    expect(steps.map(s => [s.role, s.stepId, s.content, s.isBegin, s.isEnd])).toEqual([
      ["user", 0, "hi", true, true],
      ["agent", 1, "Error: model exploded", false, true],
    ]);
    expect(agent.getHead()).toBe(steps[1].id);
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
    expect((agent as any).abortController).toBeNull();
    expect((agent as any).currentMessageId).toBeUndefined();
  });

  test("a non-Error throw is stringified", async () => {
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw "just-a-string";
      },
    });
    const agent = makeAgentWith(model);
    const res = await agent.sendMessage("hi");
    expect(res.error).toBe("just-a-string");
    expect(agent.getAllSteps()[1].content).toBe("Error: just-a-string");
  });

  test.each([null, undefined, false, 0, ""])("a falsy throw resolves as an error step: %p", async thrown => {
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw thrown;
      },
    });
    const agent = makeAgentWith(model);
    const res = await agent.sendMessage("hi");
    const expected = String(thrown);
    expect(res).toEqual({
      response: "",
      messages: [],
      usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
      stopped: false,
      error: expected,
    });
    expect(agent.getAllSteps()[1].content).toBe(`Error: ${expected}`);
  });

  test("a failed turn stays on the branch", async () => {
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw new Error("nope");
      },
    });
    const agent = makeAgentWith(model);
    await agent.sendMessage("one");
    const headAfter1 = agent.getHead();
    await agent.sendMessage("two");
    expect(agent.getAllSteps()[2].parentId).toBe(headAfter1);
    expect(agent.getVisibleReActSteps().length).toBe(4);
  });

  test("reports a cancelled run as stopped and swallows the real error", async () => {
    let agent!: TexeraAgent;
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        agent.stop();
        throw new Error("real-provider-failure");
      },
    });
    agent = makeAgentWith(model);
    const res = await agent.sendMessage("hi");
    const steps = agent.getAllSteps();
    expect(res).toEqual({
      response: "",
      messages: [],
      usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
      stopped: true,
    });
    expect(res.error).toBeUndefined();
    expect(steps.map(s => [s.role, s.stepId, s.content, s.isBegin, s.isEnd])).toEqual([
      ["user", 0, "hi", true, true],
      ["agent", 1, "Generation stopped by user.", false, true],
    ]);
    expect(JSON.stringify(steps)).not.toContain("real-provider-failure");
    expect(agent.getHead()).toBe(steps[1].id);
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
  });

  test("an AbortError-named provider error reads as a user stop", async () => {
    // Providers signal cancellation by name rather than by type, so the name is what has to be read.
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        throw Object.assign(new Error("boom"), { name: "AbortError" });
      },
    });
    const agent = makeAgentWith(model);
    const res = await agent.sendMessage("hi");
    expect(res.stopped).toBe(true);
    expect(res.error).toBeUndefined();
    expect(agent.getAllSteps()[1].content).toBe("Generation stopped by user.");
  });

  test("stop() mid-run prevents the next model call and keeps the partial step", async () => {
    // Stopping has to be observable at the next step boundary, and the work already done has to
    // survive - otherwise pressing stop silently discards the turn.
    let agent!: TexeraAgent;
    let n = 0;
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        n++;
        agent.stop();
        return {
          content: [
            {
              type: "tool-call",
              toolCallId: "c" + n,
              toolName: "deleteOperator",
              input: JSON.stringify({ operatorId: "ghost" }),
            },
          ],
          finishReason: finish("tool-calls"),
          usage: usage(5, 5),
          warnings: [],
        } as any;
      },
    });
    agent = makeAgentWith(model);
    const res = await agent.sendMessage("go");
    expect(n).toBe(1);
    expect(res.stopped).toBe(true);
    expect(res.usage.totalTokens).toBe(0);
    expect(res.messages).toEqual([]);
    expect(agent.getAllSteps().map(s => [s.role, s.stepId, s.isEnd])).toEqual([
      ["user", 0, true],
      ["agent", 1, false],
      ["agent", 2, true],
    ]);
    expect(agent.getHead()).toBe(agent.getAllSteps()[2].id);
  });

  test("mid-run state is GENERATING", async () => {
    let release!: () => void;
    const gate = new Promise<void>(r => (release = r));
    const observed: string[] = [];
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        await gate;
        return {
          content: [{ type: "text", text: "x" }],
          finishReason: finish("stop"),
          usage: usage(1, 1),
          warnings: [],
        } as any;
      },
    });
    const agent = makeAgentWith(model);
    const p = agent.sendMessage("hi");
    await new Promise(r => setTimeout(r, 5));
    observed.push(agent.getState());
    release();
    await p;
    expect(observed).toEqual([AgentState.GENERATING]);
    expect(agent.getState()).toBe(AgentState.AVAILABLE);
  });
});

/**
 * With a delegate config the agent talks to the backend, so these drive it through `fetch`.
 *
 * One trap. Setting a delegate config makes the first turn refresh from the backend, and that
 * refresh replaces the whole workflow — so operators have to arrive through `dispatch`'s stub
 * rather than being seeded on the agent. Destruction, which is what cancels the auto-persist
 * debounce, is centralized in the root `afterEach` so it runs even when a test fails mid-way.
 */
describe("delegate mode", () => {
  const wfBody = (ops: any[]) => ({
    wid: 7,
    name: "w",
    content: {
      operators: ops,
      links: [],
      operatorPositions: {},
      commentBoxes: [],
      settings: { dataTransferBatchSize: 400 },
    },
  });

  /** Routes each backend call the agent makes to a canned body, and records the URL. */
  function dispatch(execBody: any, seed: any = srcOp("op-1")) {
    fetchSpy.mockImplementation((async (u: any) => {
      const url = String(u);
      urls.push(url);
      if (url.includes("/api/compile"))
        return { ok: true, json: async () => ({ operatorOutputSchemas: {}, operatorErrors: {} }) } as any;
      if (url.includes("/api/workflow/persist")) return { ok: true, json: async () => wfBody([]) } as any;
      if (url.includes("/api/workflow/")) return { ok: true, json: async () => wfBody([seed]) } as any;
      return { ok: true, json: async () => execBody } as any;
    }) as any);
  }

  /** A completed run. The field is `operators`, not `operatorResults`; with the wrong key the
   *  result callback silently never fires and the assertions below all still look plausible. */
  const okExec = {
    success: true,
    state: "Completed",
    operators: {
      "op-1": {
        state: "Completed",
        inputTuples: 0,
        outputTuples: 1,
        resultMode: "table",
        totalRowCount: 1,
        result: [{ z: 9 }],
      },
    },
  };

  test("refreshes the workflow once, on the first turn only", async () => {
    dispatch(okExec);
    const model = textModel("ok", 1, 1);
    const agent = makeAgentWith(model);
    agent.setDelegateConfig({ userToken: "tok", workflowId: 7 });
    await agent.sendMessage("one");
    const retrieves = () => urls.filter(u => u.includes("/api/workflow/7")).length;
    expect(retrieves()).toBe(1);
    expect(
      agent
        .getWorkflowState()
        .getAllOperators()
        .map((o: any) => o.operatorID)
    ).toEqual(["op-1"]);
    expect(agent.getAllSteps()[0].beforeWorkflowContent?.operators.length).toBe(1);
    await agent.sendMessage("two");
    expect(retrieves()).toBe(1);
  });

  test("a failed refresh is swallowed", async () => {
    fetchSpy.mockImplementation((async (u: any) => {
      urls.push(String(u));
      return { ok: false, status: 500, statusText: "err", text: async () => "boom" } as any;
    }) as any);
    const model = textModel("still-ok", 1, 1);
    const agent = makeAgentWith(model);
    agent.getWorkflowState().addOperator(srcOp("local-op") as any);
    agent.setDelegateConfig({ userToken: "tok", workflowId: 7 });
    const res = await agent.sendMessage("hi");
    expect(res.response).toBe("still-ok");
    expect(
      agent
        .getWorkflowState()
        .getAllOperators()
        .map((o: any) => o.operatorID)
    ).toEqual(["local-op"]);
  });

  test("auto-executes after modifyOperator and keys the result at the agent step", async () => {
    dispatch(okExec);
    const vSpy = spyOn(WorkflowSystemMetadata.getInstance(), "validateOperatorProperties").mockReturnValue({
      isValid: true,
    } as any);
    try {
      let n = 0;
      const model = new MockLanguageModelV4({
        doGenerate: async () => {
          n++;
          if (n === 1)
            return {
              content: [
                {
                  type: "tool-call",
                  toolCallId: "c1",
                  toolName: "modifyOperator",
                  input: JSON.stringify({ operatorId: "op-1", summary: "renamed" }),
                },
              ],
              finishReason: finish("tool-calls"),
              usage: usage(1, 1),
              warnings: [],
            } as any;
          return {
            content: [{ type: "text", text: "d" }],
            finishReason: finish("stop"),
            usage: usage(1, 1),
            warnings: [],
          } as any;
        },
      });
      const agent = makeAgentWith(model);
      agent.setDelegateConfig({ userToken: "tok", workflowId: 7, workflowName: "w" });
      await agent.sendMessage("modify it");
      const steps = agent.getAllSteps();
      const txt2 = (model as any).doGenerateCalls[1].prompt[1].content[0].text;
      expect(urls.some(u => u.includes("/api/execution/7/0/run"))).toBe(true);
      expect((agent.getWorkflowResultState() as any).get("op-1").stepId).toBe(steps[1].id);
    } finally {
      // A leaked always-valid stub would let the rejected-modification test below pass validation
      // and execute, so the restore has to survive a failed assertion.
      vSpy.mockRestore();
    }
  });

  test("executeOperator tool keys its result at the current head (the user step)", async () => {
    // The contrast with the previous test is the point: an explicit executeOperator call has no agent
    // step of its own yet, so its result belongs at the head rather than at a step that follows it.
    dispatch(okExec);
    const vSpy = spyOn(WorkflowSystemMetadata.getInstance(), "validateOperatorProperties").mockReturnValue({
      isValid: true,
    } as any);
    try {
      let n = 0;
      const model = new MockLanguageModelV4({
        doGenerate: async () => {
          n++;
          if (n === 1)
            return {
              content: [
                {
                  type: "tool-call",
                  toolCallId: "c1",
                  toolName: "executeOperator",
                  input: JSON.stringify({ operatorId: "op-1" }),
                },
              ],
              finishReason: finish("tool-calls"),
              usage: usage(1, 1),
              warnings: [],
            } as any;
          return {
            content: [{ type: "text", text: "d" }],
            finishReason: finish("stop"),
            usage: usage(1, 1),
            warnings: [],
          } as any;
        },
      });
      const agent = makeAgentWith(model);
      agent.setDelegateConfig({ userToken: "tok", workflowId: 7, workflowName: "w" });
      await agent.sendMessage("run it");
      const steps = agent.getAllSteps();
      expect(urls.filter(u => u.includes("/api/execution/")).length).toBe(1);
      expect((agent.getWorkflowResultState() as any).get("op-1").stepId).toBe(steps[0].id);
    } finally {
      vSpy.mockRestore();
    }
  });

  test("a tool call missing operatorId does not trigger a whole-workflow run", async () => {
    // Without the guard an incomplete tool call falls through to a run of the entire workflow, which is
    // both expensive and not what was asked for.
    dispatch(okExec);
    let n = 0;
    const model = new MockLanguageModelV4({
      doGenerate: async () => {
        n++;
        if (n === 1)
          return {
            content: [{ type: "tool-call", toolCallId: "c1", toolName: "modifyOperator", input: JSON.stringify({}) }],
            finishReason: finish("tool-calls"),
            usage: usage(1, 1),
            warnings: [],
          } as any;
        return {
          content: [{ type: "text", text: "d" }],
          finishReason: finish("stop"),
          usage: usage(1, 1),
          warnings: [],
        } as any;
      },
    });
    const agent = makeAgentWith(model);
    agent.setDelegateConfig({ userToken: "tok", workflowId: 7 });
    await agent.sendMessage("bad call");
    const step = agent.getAllSteps()[1];
    expect(step.toolResults).toEqual([]);
    expect(urls.some(u => u.includes("/api/execution/"))).toBe(false);
  });

  test("a rejected modification suppresses the follow-up execution", async () => {
    // A modification the validator rejected did not change anything, so executing afterwards would run
    // the old workflow and report it as the result of the change.
    dispatch(okExec, srcOp("op-1", { fileName: "a.csv" }));
    const saved = (WorkflowSystemMetadata as any).instance;
    (WorkflowSystemMetadata as any).instance = undefined;
    try {
      WorkflowSystemMetadata.getInstance().loadFromMetadata({
        operators: [
          {
            operatorType: "CSVFileScan",
            jsonSchema: { type: "object", properties: { fileName: { type: "string" } }, required: ["fileName"] },
            additionalMetadata: { userFriendlyName: "CSV", operatorDescription: "csv" },
          },
        ],
      } as any);
      let n = 0;
      const model = new MockLanguageModelV4({
        doGenerate: async () => {
          n++;
          if (n === 1)
            return {
              content: [
                {
                  type: "tool-call",
                  toolCallId: "c1",
                  toolName: "modifyOperator",
                  input: JSON.stringify({ operatorId: "op-1", properties: { fileName: 123 }, summary: "s" }),
                },
              ],
              finishReason: finish("tool-calls"),
              usage: usage(1, 1),
              warnings: [],
            } as any;
          return {
            content: [{ type: "text", text: "d" }],
            finishReason: finish("stop"),
            usage: usage(1, 1),
            warnings: [],
          } as any;
        },
      });
      const agent = makeAgentWith(model);
      agent.setDelegateConfig({ userToken: "tok", workflowId: 7 });
      await agent.sendMessage("bad props");
      const step = agent.getAllSteps()[1];
      expect(String(step.toolResults?.[0]?.output)).toStartWith("[ERROR]");
      expect(urls.some(u => u.includes("/api/execution/"))).toBe(false);
    } finally {
      // The swap must be undone even on a failed assertion, or the stub metadata leaks into
      // every later test that touches the singleton.
      (WorkflowSystemMetadata as any).instance = saved;
    }
  });

  test("buildExecutionConfig projects the delegate config and live settings", async () => {
    const agent = makeAgentWith(textModel("x"));
    expect((agent as any).buildExecutionConfig()).toBeUndefined();
    (agent as any).delegateConfig = { userToken: "tok", workflowId: 5, computingUnitId: 2 };
    agent.updateSettings({
      executionTimeoutMs: 7000,
      maxOperatorResultCharLimit: 11,
      maxOperatorResultCellCharLimit: 13,
    });
    expect((agent as any).buildExecutionConfig()).toEqual({
      userToken: "tok",
      workflowId: 5,
      computingUnitId: 2,
      maxOperatorResultCharLimit: 11,
      maxOperatorResultCellCharLimit: 13,
      executionTimeoutMs: 7000,
    });
  });

  test("auto-persist coalesces a burst into one request under the delegate's name", async () => {
    // The debounce is what keeps a burst of edits from becoming a burst of writes.
    dispatch(okExec);
    const agent = makeAgentWith(textModel("x"));
    agent.setDelegateConfig({ userToken: "tok", workflowId: 7, workflowName: "My Flow" });
    agent.setDelegateConfig({ userToken: "tok", workflowId: 7, workflowName: "My Flow" });
    agent.getWorkflowState().addOperator(srcOp("o1") as any);
    agent.getWorkflowState().addOperator(srcOp("o2") as any);
    await new Promise(r => setTimeout(r, 700));
    const persists = fetchSpy.mock.calls.filter((c: any) => String(c[0]).includes("/api/workflow/persist"));
    expect(persists.length).toBe(1);
    expect(JSON.parse(persists[0][1].body).name).toBe("My Flow");
    expect(JSON.parse(JSON.parse(persists[0][1].body).content).operators.map((o: any) => o.operatorID)).toEqual([
      "o1",
      "o2",
    ]);
  });

  test("a failed auto-persist is logged, not thrown", async () => {
    const errs: any[] = [];
    const agent = makeAgentWith(textModel("x"));
    (agent as any).log = { error: (...a: any[]) => errs.push(a), debug: () => {}, warn: () => {}, info: () => {} };
    agent.setDelegateConfig({ userToken: "tok", workflowId: 7 });
    agent.getWorkflowState().addOperator(srcOp("o1") as any);
    await new Promise(r => setTimeout(r, 700));
    expect(errs.length).toBe(1);
    expect(errs[0][1]).toBe("failed to auto-persist workflow");
  });
});
