/*
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
import { buildSystemPrompt } from "./prompts";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";

/**
 * Construct a fresh, empty metadata store. `buildAllowedOperatorSchemas` will
 * yield "No operators available." which is fine — we are only asserting on the
 * envelope sections (profiler guide, key principles, UDF guides), not on the
 * operator-schema block.
 */
function emptyMetadata(): WorkflowSystemMetadata {
  return new (WorkflowSystemMetadata as any)();
}

describe("buildSystemPrompt — base content (regression guard)", () => {
  test("includes the dataflow primer and key principles", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("What is Dataflow?");
    expect(prompt).toContain("Key Principles");
    expect(prompt).toContain("Available Operators");
  });

  test("renders 'No operators available.' when metadata is empty", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("No operators available.");
  });
});

describe("buildSystemPrompt — UDF gating", () => {
  test("includes Python UDF guide when Python is allowed (default = all)", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("## Python UDF Guide");
  });

  test("includes R UDF guide when R is allowed (default = all)", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("## R UDF Guide");
  });

  test("omits Python UDF guide when the allowed list excludes PythonUDFV2", () => {
    const prompt = buildSystemPrompt(emptyMetadata(), ["CSVFileScan"]);
    expect(prompt).not.toContain("## Python UDF Guide");
  });

  test("omits R UDF guide when the allowed list excludes RUDF", () => {
    const prompt = buildSystemPrompt(emptyMetadata(), ["CSVFileScan"]);
    expect(prompt).not.toContain("## R UDF Guide");
  });
});

describe("buildSystemPrompt — Profiler Guide (Phase 2)", () => {
  test("always includes the Profiler Guide section", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("## Profiler Guide");
  });

  test("Profiler Guide is included even when the allowed-operator list is restrictive", () => {
    // Profiler tools are always registered regardless of which builder operators
    // are exposed, so the guide must not be gated on the allow-list.
    const prompt = buildSystemPrompt(emptyMetadata(), ["CSVFileScan"]);
    expect(prompt).toContain("## Profiler Guide");
  });

  test("names every Phase-1 read-only profiler tool", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("getProfilerSummary");
    expect(prompt).toContain("listHotOperators");
    expect(prompt).toContain("getOperatorMetrics");
    expect(prompt).toContain("getOptimizationHints");
    expect(prompt).toContain("compareToBaseline");
  });

  test("names every rule id surfaced by the rule engine", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("SCAN_FULL_TABLE_NO_FILTER");
    expect(prompt).toContain("UPSTREAM_OVERPRODUCTION");
    expect(prompt).toContain("JOIN_HIGH_FANIN_LOW_FANOUT");
    expect(prompt).toContain("RUNTIME_OUTLIER");
    expect(prompt).toContain("IDLE_HEAVY");
    expect(prompt).toContain("LOW_PARALLELISM_HOT_OP");
  });

  test("instructs the agent to use the profiler proactively for performance questions", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Proactively");
    // Heuristic anchor: the guide must talk about slowness / bottlenecks
    expect(prompt.toLowerCase()).toContain("slowness");
    expect(prompt.toLowerCase()).toContain("bottleneck");
  });

  test("instructs the agent to surface proposals via proposeOperatorChange, not modifyOperator", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    // The load-bearing rule of Phase 3 — proposals go through the structured tool,
    // not direct mutation.
    expect(prompt).toContain("proposeOperatorChange");
    expect(prompt).toContain("Never call `modifyOperator`");
    expect(prompt).toContain("Apply / Reject card");
  });

  test("documents the required arguments for proposeOperatorChange", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("proposeOperatorChange — required arguments");
    expect(prompt).toContain("operatorId");
    expect(prompt).toContain("propertyChanges");
    expect(prompt).toContain("reasoning");
    expect(prompt).toContain("expectedImpact");
    expect(prompt).toContain("firingHints");
  });

  test("teaches the proactive-call-out example using proposeOperatorChange", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Example — proactive bottleneck call-out (Phase 3 flow)");
    expect(prompt).toContain("Apply or Reject");
  });

  test("permits multiple independent proposals in a single turn", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Example — multiple independent suggestions");
    expect(prompt).toContain("more than once");
    expect(prompt).toContain("Do NOT bundle");
  });

  test("teaches proposeOptimizationPlan for related multi-step optimizations (Phase 4)", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Multi-step optimization plans");
    expect(prompt).toContain("proposeOptimizationPlan");
    expect(prompt).toContain("Apply All");
  });

  test("explains when to use a plan vs single proposals vs multiple proposals", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("When to use a plan vs single proposals");
    // The three branches must each be named so the model can choose correctly.
    expect(prompt).toContain("RELATED and ORDERED");
    expect(prompt).toContain("INDEPENDENT");
    expect(prompt).toContain("exactly one change");
  });

  test("documents the required arguments for proposeOptimizationPlan", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("proposeOptimizationPlan — required arguments");
    expect(prompt).toContain("planTitle");
    expect(prompt).toContain("planRationale");
    expect(prompt).toContain("steps");
    expect(prompt).toContain("2–10");
  });

  test("includes a Phase 4 multi-step plan example", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Example — multi-step plan (Phase 4 flow)");
  });

  test("distinguishes direct user requests (use modifyOperator) from agent-initiated proposals", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Direct user requests are different");
    expect(prompt).toContain("Example — direct user request");
  });

  test("teaches the no-bottleneck case via a final example", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("Example — no bottleneck found");
  });

  test("tells the agent how to react to NO_DATA from the snapshot tools", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    expect(prompt).toContain("No profiler data available");
  });
});

describe("buildSystemPrompt — ordering invariants", () => {
  test("operator schemas appear before the appended guide sections", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    const operatorsIdx = prompt.indexOf("## Available Operators");
    const profilerIdx = prompt.indexOf("## Profiler Guide");
    expect(operatorsIdx).toBeGreaterThanOrEqual(0);
    expect(profilerIdx).toBeGreaterThan(operatorsIdx);
  });

  test("Profiler Guide comes after the UDF guides when both are present", () => {
    const prompt = buildSystemPrompt(emptyMetadata());
    const pyIdx = prompt.indexOf("## Python UDF Guide");
    const rIdx = prompt.indexOf("## R UDF Guide");
    const profilerIdx = prompt.indexOf("## Profiler Guide");
    expect(profilerIdx).toBeGreaterThan(pyIdx);
    expect(profilerIdx).toBeGreaterThan(rIdx);
  });
});
