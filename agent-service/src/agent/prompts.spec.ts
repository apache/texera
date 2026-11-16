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
import { buildSystemPrompt } from "./prompts";

describe("buildSystemPrompt", () => {
  const prompt = buildSystemPrompt();

  test("sends the model to the two operator tools, and describes no operator type itself", () => {
    expect(prompt).toContain("`list_operator_types` names every operator type");
    expect(prompt).toContain("`get_operator_schema` gives the properties");
    // Neither a line per type nor a schema: both used to be written here.
    expect(prompt).not.toMatch(/^- [A-Za-z][A-Za-z0-9]*: /m);
    expect(prompt).not.toContain("```json");
    expect(prompt).not.toContain("{{");
  });

  test("stays within its budget, so no list of types or schemas grows back into it", () => {
    // The prompt was 27 KB with the schemas of 18 operator types in it, and 219 KB with all of them.
    expect(new TextEncoder().encode(prompt).length).toBeLessThan(10_000);
  });

  test("holds the guides for both UDF languages", () => {
    expect(prompt).toContain("## Python UDF Guide");
    expect(prompt).toContain("## R UDF Guide");
  });

  test("sends the model to the read tools for the workflow, and gives no format for the conversation", () => {
    // The conversation reaches the model as native messages and nothing else is written into it, so
    // the prompt describes only what get_workflow returns.
    expect(prompt).toContain("## Reading the Workflow");
    expect(prompt).toContain("Call `get_workflow` to see it");
    expect(prompt).toContain("### Operator `<operator_id>` (<operator_type>)");
    expect(prompt).toContain("Result: <whether the operator has a result, see below>");
    expect(prompt).toContain("`outdated, ...` when the operator or one it reads from changed after its last execution");
    expect(prompt).toContain("`get_operator_result` returns an operator's latest result");
    expect(prompt).not.toContain("Current Dataflow");
    expect(prompt).not.toContain("## Context Format");
    expect(prompt).not.toContain("# Completed Tasks");
    expect(prompt).not.toContain("# Ongoing Task");
    expect(prompt).not.toContain("### Turn");
  });
});
