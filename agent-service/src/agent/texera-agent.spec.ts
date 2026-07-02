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
import { INITIAL_STEP_ID } from "../types/agent";
import { OperatorResultMode, OperatorState, type OperatorExecutionSummary } from "../types/execution";

function makeAgent(): TexeraAgent {
  return new TexeraAgent({
    // The model is never invoked in these tests.
    model: {} as any,
    modelType: "test-model",
    agentId: "agent-1",
  });
}

function makeSummary(tuplesCount: number): OperatorExecutionSummary {
  return {
    state: OperatorState.COMPLETED,
    errorMessages: [],
    resultSummary: { resultMode: OperatorResultMode.TABLE, sampleTuples: [], tuplesCount },
  };
}

describe("TexeraAgent.getFormattedResultsForDAG", () => {
  test("formats every result visible on the current step branch", () => {
    const agent = makeAgent();
    // Seed a result on the initial step, which is the head at construction time
    // so it sits on the ancestor path getAllVisible() walks.
    agent.getWorkflowResultState().set("op-1", INITIAL_STEP_ID, makeSummary(5));

    const formatted = (
      agent as unknown as { getFormattedResultsForDAG(): Map<string, string> }
    ).getFormattedResultsForDAG();

    expect(formatted.size).toBe(1);
    expect(formatted.has("op-1")).toBe(true);
    expect(typeof formatted.get("op-1")).toBe("string");
    expect(formatted.get("op-1")!.length).toBeGreaterThan(0);
  });

  test("returns an empty map when no results are visible", () => {
    const agent = makeAgent();
    const formatted = (
      agent as unknown as { getFormattedResultsForDAG(): Map<string, string> }
    ).getFormattedResultsForDAG();
    expect(formatted.size).toBe(0);
  });
});
