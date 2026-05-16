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

import { describe, it, expect } from "vitest";
import {
  PROPOSE_OPERATOR_CHANGE_TOOL_NAME,
  PROPOSE_OPTIMIZATION_PLAN_TOOL_NAME,
  extractPlans,
  extractProposals,
  mergeProposalIntoProperties,
  summarizePropertyChanges,
} from "./agent-proposal";
import { ReActStep } from "./agent-types";

function makeStep(overrides: Partial<ReActStep> = {}): ReActStep {
  return {
    id: "step-1",
    messageId: "msg-1",
    stepId: 1,
    timestamp: new Date(),
    role: "agent",
    content: "",
    isBegin: true,
    isEnd: false,
    toolCalls: [],
    ...overrides,
  } as ReActStep;
}

function makeProposalCall(overrides: Record<string, any> = {}) {
  return {
    toolName: PROPOSE_OPERATOR_CHANGE_TOOL_NAME,
    toolCallId: "tc-1",
    input: {
      operatorId: "python-udf-1",
      propertyChanges: { workers: 4 },
      reasoning: "RUNTIME_OUTLIER and LOW_PARALLELISM_HOT_OP fired.",
      expectedImpact: "Cuts runtime via parallelism.",
      firingHints: ["RUNTIME_OUTLIER", "LOW_PARALLELISM_HOT_OP"],
      ...overrides,
    },
  };
}

describe("extractProposals", () => {
  it("returns [] for non-agent steps", () => {
    const step = makeStep({ role: "user", toolCalls: [makeProposalCall()] });
    expect(extractProposals(step)).toEqual([]);
  });

  it("returns [] when toolCalls is missing or empty", () => {
    expect(extractProposals(makeStep({ toolCalls: undefined }))).toEqual([]);
    expect(extractProposals(makeStep({ toolCalls: [] }))).toEqual([]);
  });

  it("ignores tool calls whose name does not match", () => {
    const step = makeStep({
      toolCalls: [
        { toolName: "getProfilerSummary", toolCallId: "tc-x", input: {} },
        { toolName: "modifyOperator", toolCallId: "tc-y", input: { operatorId: "op" } },
      ],
    });
    expect(extractProposals(step)).toEqual([]);
  });

  it("extracts a single well-formed proposal with all fields", () => {
    const step = makeStep({ toolCalls: [makeProposalCall()] });
    const out = extractProposals(step);
    expect(out).toHaveLength(1);
    expect(out[0]).toEqual({
      toolCallId: "tc-1",
      operatorId: "python-udf-1",
      propertyChanges: { workers: 4 },
      reasoning: "RUNTIME_OUTLIER and LOW_PARALLELISM_HOT_OP fired.",
      expectedImpact: "Cuts runtime via parallelism.",
      firingHints: ["RUNTIME_OUTLIER", "LOW_PARALLELISM_HOT_OP"],
    });
  });

  it("extracts multiple independent proposals from one step", () => {
    const step = makeStep({
      toolCalls: [
        makeProposalCall({ operatorId: "op-a" }),
        { ...makeProposalCall({ operatorId: "op-b" }), toolCallId: "tc-2" },
      ],
    });
    const out = extractProposals(step);
    expect(out.map(p => p.operatorId)).toEqual(["op-a", "op-b"]);
    expect(out.map(p => p.toolCallId)).toEqual(["tc-1", "tc-2"]);
  });

  it("makes firingHints undefined when omitted", () => {
    const call = makeProposalCall({ firingHints: undefined });
    const out = extractProposals(makeStep({ toolCalls: [call] }));
    expect(out[0].firingHints).toBeUndefined();
  });

  it("makes firingHints undefined when shape is invalid (non-array or mixed types)", () => {
    const a = extractProposals(makeStep({ toolCalls: [makeProposalCall({ firingHints: "RUNTIME_OUTLIER" })] }));
    expect(a[0].firingHints).toBeUndefined();
    const b = extractProposals(makeStep({ toolCalls: [makeProposalCall({ firingHints: ["ok", 42] })] }));
    expect(b[0].firingHints).toBeUndefined();
  });

  it.each([
    ["missing operatorId", { operatorId: undefined }],
    ["empty operatorId", { operatorId: "" }],
    ["non-string operatorId", { operatorId: 42 }],
    ["missing propertyChanges", { propertyChanges: undefined }],
    ["null propertyChanges", { propertyChanges: null }],
    ["array propertyChanges", { propertyChanges: [1, 2] }],
    ["missing reasoning", { reasoning: undefined }],
    ["non-string reasoning", { reasoning: 1 }],
    ["missing expectedImpact", { expectedImpact: undefined }],
  ])("silently drops a malformed proposal (%s)", (_label, override) => {
    const call = makeProposalCall();
    Object.assign(call.input, override);
    expect(extractProposals(makeStep({ toolCalls: [call] }))).toEqual([]);
  });

  it("skips a tool call with no toolCallId (cannot track UI state without one)", () => {
    const call = makeProposalCall();
    delete (call as any).toolCallId;
    expect(extractProposals(makeStep({ toolCalls: [call] }))).toEqual([]);
  });
});

describe("mergeProposalIntoProperties", () => {
  it("preserves unchanged keys", () => {
    const merged = mergeProposalIntoProperties({ workers: 1, mode: "stream", enabled: true }, { workers: 4 });
    expect(merged).toEqual({ workers: 4, mode: "stream", enabled: true });
  });

  it("handles undefined existing as empty object", () => {
    expect(mergeProposalIntoProperties(undefined, { x: 1 })).toEqual({ x: 1 });
  });

  it("changes take precedence over existing values", () => {
    expect(mergeProposalIntoProperties({ a: 1 }, { a: 99 })).toEqual({ a: 99 });
  });

  it("does not mutate inputs", () => {
    const existing = { a: 1, b: 2 };
    const changes = { b: 99 };
    mergeProposalIntoProperties(existing, changes);
    expect(existing).toEqual({ a: 1, b: 2 });
    expect(changes).toEqual({ b: 99 });
  });
});

describe("extractPlans", () => {
  function makePlanCall(overrides: Record<string, any> = {}) {
    return {
      toolName: PROPOSE_OPTIMIZATION_PLAN_TOOL_NAME,
      toolCallId: "plan-tc-1",
      input: {
        planTitle: "Reduce Python UDF load",
        planRationale: "SCAN_FULL_TABLE_NO_FILTER and LOW_PARALLELISM_HOT_OP feed the same hot path.",
        firingHints: ["SCAN_FULL_TABLE_NO_FILTER", "LOW_PARALLELISM_HOT_OP"],
        steps: [
          {
            operatorId: "filter-1",
            propertyChanges: { predicate: "is not null" },
            description: "Add a Filter upstream of the UDF",
            reasoning: "SCAN_FULL_TABLE_NO_FILTER",
            expectedImpact: "Drops rows before the UDF",
          },
          {
            operatorId: "python-udf-1",
            propertyChanges: { workers: 4 },
            description: "Bump UDF workers to 4",
            reasoning: "LOW_PARALLELISM_HOT_OP",
            expectedImpact: "Parallelizes the remaining work",
          },
        ],
        ...overrides,
      },
    };
  }

  it("returns [] for non-agent steps", () => {
    const step = makeStep({ role: "user", toolCalls: [makePlanCall()] });
    expect(extractPlans(step)).toEqual([]);
  });

  it("returns [] when toolCalls is missing or empty", () => {
    expect(extractPlans(makeStep({ toolCalls: undefined }))).toEqual([]);
    expect(extractPlans(makeStep({ toolCalls: [] }))).toEqual([]);
  });

  it("ignores tool calls whose name does not match", () => {
    const step = makeStep({
      toolCalls: [
        { toolName: PROPOSE_OPERATOR_CHANGE_TOOL_NAME, toolCallId: "x", input: {} },
        { toolName: "getProfilerSummary", toolCallId: "y", input: {} },
      ],
    });
    expect(extractPlans(step)).toEqual([]);
  });

  it("extracts a well-formed plan with all fields preserved + step ids", () => {
    const step = makeStep({ toolCalls: [makePlanCall()] });
    const out = extractPlans(step);
    expect(out).toHaveLength(1);
    const plan = out[0];
    expect(plan.toolCallId).toBe("plan-tc-1");
    expect(plan.planTitle).toBe("Reduce Python UDF load");
    expect(plan.firingHints).toEqual(["SCAN_FULL_TABLE_NO_FILTER", "LOW_PARALLELISM_HOT_OP"]);
    expect(plan.steps).toHaveLength(2);
    expect(plan.steps[0].stepId).toBe("plan-tc-1::0");
    expect(plan.steps[1].stepId).toBe("plan-tc-1::1");
    expect(plan.steps[0].operatorId).toBe("filter-1");
    expect(plan.steps[1].propertyChanges).toEqual({ workers: 4 });
  });

  it("preserves step order (steps are ordered, not a set)", () => {
    const call = makePlanCall({
      steps: [
        { operatorId: "a", propertyChanges: {}, description: "A", reasoning: "r", expectedImpact: "i" },
        { operatorId: "b", propertyChanges: {}, description: "B", reasoning: "r", expectedImpact: "i" },
        { operatorId: "c", propertyChanges: {}, description: "C", reasoning: "r", expectedImpact: "i" },
      ],
    });
    const plan = extractPlans(makeStep({ toolCalls: [call] }))[0];
    expect(plan.steps.map(s => s.operatorId)).toEqual(["a", "b", "c"]);
    expect(plan.steps.map(s => s.stepId)).toEqual(["plan-tc-1::0", "plan-tc-1::1", "plan-tc-1::2"]);
  });

  it("drops the plan when fewer than 2 steps are well-formed (after per-step validation)", () => {
    const call = makePlanCall({
      steps: [
        { operatorId: "a", propertyChanges: {}, description: "A", reasoning: "r", expectedImpact: "i" },
        // malformed: missing description
        { operatorId: "b", propertyChanges: {}, description: "", reasoning: "r", expectedImpact: "i" },
      ],
    });
    expect(extractPlans(makeStep({ toolCalls: [call] }))).toEqual([]);
  });

  it.each([
    ["missing planTitle", { planTitle: undefined }],
    ["empty planTitle", { planTitle: "" }],
    ["missing planRationale", { planRationale: undefined }],
    ["missing steps", { steps: undefined }],
    ["non-array steps", { steps: { length: 0 } }],
    ["empty steps", { steps: [] }],
  ])("silently drops a malformed plan (%s)", (_label, override) => {
    const call = makePlanCall();
    Object.assign(call.input, override);
    expect(extractPlans(makeStep({ toolCalls: [call] }))).toEqual([]);
  });

  it("makes firingHints undefined when omitted or mis-typed", () => {
    expect(extractPlans(makeStep({ toolCalls: [makePlanCall({ firingHints: undefined })] }))[0].firingHints).toBeUndefined();
    expect(extractPlans(makeStep({ toolCalls: [makePlanCall({ firingHints: "RUNTIME_OUTLIER" })] }))[0].firingHints).toBeUndefined();
    expect(extractPlans(makeStep({ toolCalls: [makePlanCall({ firingHints: ["ok", 1] })] }))[0].firingHints).toBeUndefined();
  });

  it("plan and standalone proposals from the same step are returned by their respective extractors", () => {
    const proposalCall = {
      toolName: PROPOSE_OPERATOR_CHANGE_TOOL_NAME,
      toolCallId: "tc-prop",
      input: {
        operatorId: "agg-1",
        propertyChanges: { x: 1 },
        reasoning: "r",
        expectedImpact: "i",
      },
    };
    const step = makeStep({ toolCalls: [proposalCall, makePlanCall()] });
    expect(extractProposals(step)).toHaveLength(1);
    expect(extractPlans(step)).toHaveLength(1);
  });
});

describe("summarizePropertyChanges", () => {
  it("formats a single numeric change", () => {
    expect(summarizePropertyChanges({ workers: 4 })).toBe("workers → 4");
  });

  it("formats a single string change with quotes", () => {
    expect(summarizePropertyChanges({ mode: "stream" })).toBe('mode → "stream"');
  });

  it("formats multiple changes joined with commas", () => {
    expect(summarizePropertyChanges({ workers: 4, enabled: true })).toBe("workers → 4, enabled → true");
  });

  it("returns the empty-changes sentinel for {}", () => {
    expect(summarizePropertyChanges({})).toBe("(no changes)");
  });

  it("handles null and object/array values via JSON stringify", () => {
    const s = summarizePropertyChanges({ a: null, b: { x: 1 }, c: [1, 2] });
    expect(s).toContain("a → null");
    expect(s).toContain('b → {"x":1}');
    expect(s).toContain("c → [1,2]");
  });
});
