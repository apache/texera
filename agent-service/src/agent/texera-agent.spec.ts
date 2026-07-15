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

// Exercises revert/redo over REAL sendMessage() turns (driven by a mock model),
// so the step-tree, HEAD movement, per-step workflow snapshots, and redo-stack
// lifecycle are covered end to end rather than via hand-built internal state.

import { afterEach, describe, expect, mock, test } from "bun:test";
import { MockLanguageModelV3 } from "ai/test";
import { TexeraAgent } from "./texera-agent";
import { INITIAL_STEP_ID } from "../types/agent";

// Controllable backend doubles for the persistence/serialization tests below.
// Defaults are benign no-ops so the mock is invisible to every other test
// (none of which set a delegate config, so the real module is never needed).
let persistImpl: (...args: any[]) => Promise<void> = async () => {};
let retrieveImpl: () => Promise<any> = async () => ({ content: emptyContent() });

mock.module("../api/workflow-api", () => ({
  persistWorkflow: (...args: any[]) => persistImpl(...args),
  retrieveWorkflow: () => retrieveImpl(),
}));

function emptyContent(): any {
  return { operators: [], operatorPositions: {}, links: [], commentBoxes: [], settings: {} };
}

function contentWith(...operatorIds: string[]): any {
  return { ...emptyContent(), operators: operatorIds.map(operatorID => ({ operatorID })) };
}

function textModel(text: string): any {
  return new MockLanguageModelV3({
    doGenerate: async () =>
      ({
        content: [{ type: "text", text }],
        finishReason: "stop",
        usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        warnings: [],
      }) as any,
  });
}

function throwingModel(message: string): any {
  return new MockLanguageModelV3({
    doGenerate: async () => {
      throw new Error(message);
    },
  });
}

function makeAgent(model: any): TexeraAgent {
  return new TexeraAgent({ model, modelType: "mock", agentId: "test-agent" });
}

describe("TexeraAgent revert/redo over real turns", () => {
  test("a completed turn records a workflow snapshot on every step", async () => {
    const agent = makeAgent(textModel("done"));
    await agent.sendMessage("hello");

    const steps = agent.getAllSteps();
    expect(steps.length).toBeGreaterThan(0);
    for (const s of steps) {
      expect(s.afterWorkflowContent).toBeDefined();
    }
    // HEAD is the turn's last step and is on the visible path.
    expect(agent.getVisibleReActSteps().at(-1)?.id).toBe(agent.getHead());
  });

  test("a new prompt clears the redo stack (a branch invalidates redo)", async () => {
    const agent = makeAgent(textModel("done"));
    await agent.sendMessage("first");

    const userStep = agent.getAllSteps().find(s => s.role === "user");
    expect(userStep).toBeDefined();
    await agent.revertToTurnStart(userStep!.messageId);
    expect(agent.canRedo()).toBe(true);

    await agent.sendMessage("second"); // branch from the reverted HEAD
    expect(agent.canRedo()).toBe(false);
  });

  test("revert then redo round-trips HEAD across a real turn", async () => {
    const agent = makeAgent(textModel("done"));
    await agent.sendMessage("only turn");
    const leaf = agent.getHead();
    const userStep = agent.getAllSteps().find(s => s.role === "user")!;

    const reverted = await agent.revertToTurnStart(userStep.messageId);
    expect(agent.getHead()).toBe(reverted.headId);
    expect(agent.getHead()).not.toBe(leaf);

    const redone = await agent.redo();
    expect(redone.headId).toBe(leaf);
    expect(agent.getHead()).toBe(leaf);
    expect(agent.canRedo()).toBe(false);
  });

  test("a turn that errors still snapshots its final step (so it can be reverted/redone)", async () => {
    const agent = makeAgent(throwingModel("boom"));
    const result = await agent.sendMessage("hello");
    expect(result.error).toBeDefined();

    const headStep = agent.getStepsById().get(agent.getHead());
    expect(headStep?.afterWorkflowContent).toBeDefined();

    // The errored turn is still revertable + redoable.
    const userStep = agent.getAllSteps().find(s => s.role === "user")!;
    await agent.revertToTurnStart(userStep.messageId);
    const redone = await agent.redo();
    expect(redone.workflowContent).toBeDefined();
  });

  test("reverting a turn that is no longer on the HEAD path is rejected", async () => {
    const agent = makeAgent(textModel("done"));
    await agent.sendMessage("only turn");
    const userStep = agent.getAllSteps().find(s => s.role === "user")!;

    await agent.revertToTurnStart(userStep.messageId);
    // A second revert of the same turn (e.g. a stale client) must not push a
    // bogus redo entry or move HEAD again.
    await expect(agent.revertToTurnStart(userStep.messageId)).rejects.toThrow(
      "Turn is not on the current conversation path"
    );
    expect(agent.getHead()).toBe(userStep.parentId!);
    expect((agent as any).redoStack).toHaveLength(1);
  });

  test("revert restores the pre-turn workflow through a real turn's snapshots", async () => {
    const agent = makeAgent(textModel("done"));
    agent.getWorkflowState().setWorkflowContent(contentWith("opA"));
    await agent.sendMessage("turn 1"); // snapshots capture the opA workflow
    // The workflow drifts after the turn; revert must restore the snapshot,
    // not just move HEAD.
    agent.getWorkflowState().setWorkflowContent(contentWith("opA", "opB"));

    const userStep = agent.getAllSteps().find(s => s.role === "user")!;
    const reverted = await agent.revertToTurnStart(userStep.messageId);

    expect(reverted.workflowContent.operators.map((o: any) => o.operatorID)).toEqual(["opA"]);
    expect(
      agent
        .getWorkflowState()
        .getWorkflowContent()
        .operators.map((o: any) => o.operatorID)
    ).toEqual(["opA"]);
  });
});

describe("TexeraAgent revert/redo persistence & serialization", () => {
  const delegate = { userToken: "tok", workflowId: 42 };

  // NOTE: with a delegate config the run loop also calls the
  // workflow-compiling service; whether that fetch is refused (CI) or
  // rejected by a live service, compile-api returns null and the agent
  // proceeds without schemas, so it needs no mocking here.

  afterEach(() => {
    persistImpl = async () => {};
    retrieveImpl = async () => ({ content: emptyContent() });
  });

  test("a revert whose persistence fails is rolled back and surfaces an error", async () => {
    const agent = makeAgent(textModel("done"));
    agent.setDelegateConfig(delegate);
    await agent.sendMessage("turn 1");
    const leaf = agent.getHead();
    const leafContent = agent.getWorkflowState().getWorkflowContent();
    const userStep = agent.getAllSteps().find(s => s.role === "user")!;

    persistImpl = async () => {
      throw new Error("backend down");
    };
    await expect(agent.revertToTurnStart(userStep.messageId)).rejects.toThrow("Failed to save the restored workflow");

    // Nothing moved: a "reverted" reply with the backend still on the old
    // workflow would let the next prompt's refresh silently undo the revert.
    expect(agent.getHead()).toBe(leaf);
    expect(agent.canRedo()).toBe(false);
    expect(agent.getWorkflowState().getWorkflowContent()).toEqual(leafContent);
  });

  test("overlapping revert and redo run serialized, never concurrently", async () => {
    const agent = makeAgent(textModel("done"));
    agent.setDelegateConfig(delegate);
    await agent.sendMessage("turn 1");
    const leaf = agent.getHead();
    const userStep = agent.getAllSteps().find(s => s.role === "user")!;

    const events: string[] = [];
    let release!: () => void;
    const gate = new Promise<void>(resolve => (release = resolve));
    let gated = true;
    persistImpl = async () => {
      events.push(`persist-start@${agent.getHead()}`);
      if (gated) {
        gated = false;
        await gate; // hold the revert's persistence open
      }
      events.push("persist-end");
    };

    const revertP = agent.revertToTurnStart(userStep.messageId);
    const redoP = agent.redo(); // must queue behind the in-flight revert
    await new Promise(resolve => setTimeout(resolve, 10));
    // The redo has not touched anything while the revert persistence is open.
    expect(events).toEqual([`persist-start@${INITIAL_STEP_ID}`]);

    release();
    await revertP;
    await redoP;

    expect(events).toEqual([`persist-start@${INITIAL_STEP_ID}`, "persist-end", `persist-start@${leaf}`, "persist-end"]);
    expect(agent.getHead()).toBe(leaf);
    expect(agent.canRedo()).toBe(false);
  });

  test("a redo arriving during prompt startup is rejected instead of interleaving", async () => {
    const agent = makeAgent(textModel("done"));
    agent.setDelegateConfig(delegate);
    await agent.sendMessage("turn 1");
    const userStep = agent.getAllSteps().find(s => s.role === "user")!;
    await agent.revertToTurnStart(userStep.messageId); // HEAD -> initial, redo available

    // Gate the second prompt's backend refresh so its startup holds the lock
    // with the agent still reporting AVAILABLE — the original race window.
    let releaseRetrieve!: () => void;
    const retrieveGate = new Promise<void>(resolve => (releaseRetrieve = resolve));
    retrieveImpl = async () => {
      await retrieveGate;
      return { content: emptyContent() };
    };

    const promptP = agent.sendMessage("turn 2");
    const redoP = agent.redo(); // queued behind the prompt startup
    redoP.catch(() => {}); // it can only settle after the gate opens below
    await new Promise(resolve => setTimeout(resolve, 10));
    releaseRetrieve();

    // NOTE: bun's expect(promise).rejects spin-waits synchronously for the
    // promise to settle, so it must not be created while settlement still
    // depends on test code that runs later (the gate release above).
    await expect(redoP).rejects.toThrow("Cannot redo while the agent is busy");
    const result = await promptP;
    expect(result.error).toBeUndefined();
    // The prompt invalidated redo; the late redo neither restored the
    // abandoned branch nor left a stale redo entry behind.
    expect(agent.canRedo()).toBe(false);
  });
});
