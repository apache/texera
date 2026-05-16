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
import {
  createProposalTools,
  createProposeOperatorChangeTool,
  createProposeOptimizationPlanTool,
  TOOL_NAME_PROPOSE_OPERATOR_CHANGE,
  TOOL_NAME_PROPOSE_OPTIMIZATION_PLAN,
  type OperatorChangeProposal,
  type OptimizationPlanProposal,
} from "./proposal-tools";

describe("proposeOperatorChange", () => {
  test("returns a structured JSON proposal with all fields preserved", async () => {
    const t = createProposeOperatorChangeTool();
    const raw = (await t.execute!(
      {
        operatorId: "python-udf-1",
        propertyChanges: { workers: 4 },
        reasoning: "RUNTIME_OUTLIER and LOW_PARALLELISM_HOT_OP both fired on python-udf-1.",
        expectedImpact: "Should cut runtime via more parallelism.",
        firingHints: ["RUNTIME_OUTLIER", "LOW_PARALLELISM_HOT_OP"],
      } as any,
      {} as any
    )) as string;
    const parsed: OperatorChangeProposal = JSON.parse(raw);
    expect(parsed.kind).toBe("operator_change_proposal");
    expect(parsed.operatorId).toBe("python-udf-1");
    expect(parsed.propertyChanges).toEqual({ workers: 4 });
    expect(parsed.reasoning).toContain("RUNTIME_OUTLIER");
    expect(parsed.expectedImpact).toContain("parallelism");
    expect(parsed.firingHints).toEqual(["RUNTIME_OUTLIER", "LOW_PARALLELISM_HOT_OP"]);
  });

  test("omits firingHints from the JSON when not provided", async () => {
    const t = createProposeOperatorChangeTool();
    const raw = (await t.execute!(
      {
        operatorId: "agg-1",
        propertyChanges: { groupByKeys: ["k"] },
        reasoning: "UPSTREAM_OVERPRODUCTION on csv-scan-1 → agg-1.",
        expectedImpact: "Reduces shuffle.",
      } as any,
      {} as any
    )) as string;
    const parsed: OperatorChangeProposal = JSON.parse(raw);
    expect(parsed.firingHints).toBeUndefined();
  });

  test("accepts arbitrarily-shaped propertyChanges values (string / number / boolean / object / array)", async () => {
    const t = createProposeOperatorChangeTool();
    const raw = (await t.execute!(
      {
        operatorId: "op-x",
        propertyChanges: {
          workers: 4,
          enabled: true,
          mode: "stream",
          predicates: [{ attribute: "x", condition: "is not null" }],
          options: { batchSize: 100 },
        },
        reasoning: "r",
        expectedImpact: "i",
      } as any,
      {} as any
    )) as string;
    const parsed: OperatorChangeProposal = JSON.parse(raw);
    expect(parsed.propertyChanges).toMatchObject({
      workers: 4,
      enabled: true,
      mode: "stream",
    });
    expect(Array.isArray((parsed.propertyChanges as any).predicates)).toBe(true);
    expect((parsed.propertyChanges as any).options).toEqual({ batchSize: 100 });
  });
});

describe("proposeOptimizationPlan", () => {
  function validPlanArgs(overrides: Record<string, any> = {}) {
    return {
      planTitle: "Optimize the Python UDF bottleneck",
      planRationale:
        "RUNTIME_OUTLIER and LOW_PARALLELISM_HOT_OP both fire on python-udf-1; the upstream scan also has SCAN_FULL_TABLE_NO_FILTER.",
      firingHints: ["RUNTIME_OUTLIER", "LOW_PARALLELISM_HOT_OP", "SCAN_FULL_TABLE_NO_FILTER"],
      steps: [
        {
          operatorId: "filter-1",
          propertyChanges: { predicate: "col > 0" },
          description: "Push a Filter upstream of the UDF",
          reasoning: "SCAN_FULL_TABLE_NO_FILTER → smaller working set",
          expectedImpact: "Reduces rows entering the UDF",
        },
        {
          operatorId: "python-udf-1",
          propertyChanges: { workers: 4 },
          description: "Increase UDF workers to 4",
          reasoning: "LOW_PARALLELISM_HOT_OP",
          expectedImpact: "Parallelizes the remaining UDF work",
        },
      ],
      ...overrides,
    };
  }

  test("returns a structured JSON plan with all fields preserved", async () => {
    const t = createProposeOptimizationPlanTool();
    const raw = (await t.execute!(validPlanArgs() as any, {} as any)) as string;
    const parsed: OptimizationPlanProposal = JSON.parse(raw);
    expect(parsed.kind).toBe("optimization_plan_proposal");
    expect(parsed.planTitle).toContain("Python UDF bottleneck");
    expect(parsed.firingHints).toEqual([
      "RUNTIME_OUTLIER",
      "LOW_PARALLELISM_HOT_OP",
      "SCAN_FULL_TABLE_NO_FILTER",
    ]);
    expect(parsed.steps).toHaveLength(2);
    expect(parsed.steps[0].operatorId).toBe("filter-1");
    expect(parsed.steps[1].operatorId).toBe("python-udf-1");
    expect(parsed.steps[1].propertyChanges).toEqual({ workers: 4 });
  });

  test("preserves step order (steps are ordered, not a set)", async () => {
    const t = createProposeOptimizationPlanTool();
    const steps = [
      { operatorId: "a", propertyChanges: {}, description: "A", reasoning: "a", expectedImpact: "a" },
      { operatorId: "b", propertyChanges: {}, description: "B", reasoning: "b", expectedImpact: "b" },
      { operatorId: "c", propertyChanges: {}, description: "C", reasoning: "c", expectedImpact: "c" },
    ];
    const raw = (await t.execute!(validPlanArgs({ steps }) as any, {} as any)) as string;
    const parsed: OptimizationPlanProposal = JSON.parse(raw);
    expect(parsed.steps.map(s => s.operatorId)).toEqual(["a", "b", "c"]);
  });

  test("omits firingHints from the JSON when not provided", async () => {
    const t = createProposeOptimizationPlanTool();
    const args = validPlanArgs({ firingHints: undefined });
    const raw = (await t.execute!(args as any, {} as any)) as string;
    const parsed: OptimizationPlanProposal = JSON.parse(raw);
    expect(parsed.firingHints).toBeUndefined();
  });
});

describe("createProposalTools (factory)", () => {
  test("registers both proposal tools under their TOOL_NAME constants", () => {
    const tools = createProposalTools();
    expect(Object.keys(tools).sort()).toEqual(
      [TOOL_NAME_PROPOSE_OPERATOR_CHANGE, TOOL_NAME_PROPOSE_OPTIMIZATION_PLAN].sort()
    );
    expect(tools[TOOL_NAME_PROPOSE_OPERATOR_CHANGE]).toBeDefined();
    expect(tools[TOOL_NAME_PROPOSE_OPTIMIZATION_PLAN]).toBeDefined();
  });

  test("tool name constants match the agreed contract", () => {
    // Cross-project contract — frontend reads tool calls by name. Changing
    // either requires updating frontend/src/.../service/agent/agent-proposal.ts.
    expect(TOOL_NAME_PROPOSE_OPERATOR_CHANGE).toBe("proposeOperatorChange");
    expect(TOOL_NAME_PROPOSE_OPTIMIZATION_PLAN).toBe("proposeOptimizationPlan");
  });
});
