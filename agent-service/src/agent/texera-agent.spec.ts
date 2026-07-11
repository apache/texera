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
import type { LanguageModel } from "ai";
import { INITIAL_STEP_ID } from "../types/agent";
import { OperatorState } from "../types/execution";
import { TexeraAgent } from "./texera-agent";

function makeAgent(): TexeraAgent {
  return new TexeraAgent({
    model: {} as LanguageModel,
    modelType: "test-model",
    agentId: "agent-1",
  });
}

function getFormattedResultsForDAG(agent: TexeraAgent): Map<string, string> {
  return (agent as unknown as { getFormattedResultsForDAG(): Map<string, string> }).getFormattedResultsForDAG();
}

describe("TexeraAgent DAG result formatting", () => {
  test("formats results visible from the current step", () => {
    const agent = makeAgent();
    agent.getWorkflowResultState().set("op-1", INITIAL_STEP_ID, {
      state: OperatorState.COMPLETED,
      errorMessages: [],
    });

    expect(getFormattedResultsForDAG(agent)).toEqual(new Map([["op-1", "(no result data)"]]));
  });

  test("omits results outside the current step ancestry", () => {
    const agent = makeAgent();
    agent.getWorkflowResultState().set("op-1", "unrelated-step", {
      state: OperatorState.COMPLETED,
      errorMessages: [],
    });

    expect(getFormattedResultsForDAG(agent)).toEqual(new Map());
  });
});
