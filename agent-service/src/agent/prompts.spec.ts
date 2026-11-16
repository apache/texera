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
import type { OperatorSchema } from "../api/backend-api";
import { buildSystemPrompt } from "./prompts";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";

function makeOperatorSchema(operatorType: string, description: string): OperatorSchema {
  return {
    operatorType,
    operatorVersion: "1.0",
    jsonSchema: {
      properties: {
        condition: { type: "string" },
      },
      required: ["condition"],
    },
    additionalMetadata: {
      userFriendlyName: operatorType,
      operatorGroupName: "Test",
      operatorDescription: description,
      inputPorts: [],
      outputPorts: [],
    },
  };
}

function makeMetadataStore(): WorkflowSystemMetadata {
  const metadataStore = new WorkflowSystemMetadata();
  metadataStore.loadFromMetadata({
    operators: [
      makeOperatorSchema("Filter", "Keeps rows that match a condition."),
      makeOperatorSchema("PythonUDFV2", "Runs user-defined Python code."),
      makeOperatorSchema("RUDF", "Runs user-defined R code."),
    ],
    groups: [],
  });
  return metadataStore;
}

describe("buildSystemPrompt", () => {
  test("renders only explicitly allowed operators with their descriptions and compact schemas", () => {
    const prompt = buildSystemPrompt(makeMetadataStore(), ["Filter"]);

    expect(prompt).toContain("## Filter");
    expect(prompt).toContain("Description: Keeps rows that match a condition.");
    expect(prompt).toMatch(/"condition":\s*\{\s*"type":\s*"string"/);
    expect(prompt).not.toContain("## PythonUDFV2");
    expect(prompt).not.toContain("## RUDF");
    expect(prompt).not.toContain("## Python UDF Guide");
    expect(prompt).not.toContain("## R UDF Guide");
  });

  test("adds guidance only for the UDF language in a restricted allowlist", () => {
    const metadataStore = makeMetadataStore();
    const pythonPrompt = buildSystemPrompt(metadataStore, ["PythonUDFV2"]);
    const rPrompt = buildSystemPrompt(metadataStore, ["RUDF"]);

    expect(pythonPrompt).toContain("## Python UDF Guide");
    expect(pythonPrompt).not.toContain("## R UDF Guide");
    expect(rPrompt).toContain("## R UDF Guide");
    expect(rPrompt).not.toContain("## Python UDF Guide");
  });

  test("uses all metadata operators and both UDF guides when no allowlist is supplied", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    expect(prompt).toContain("## Filter");
    expect(prompt).toContain("## PythonUDFV2");
    expect(prompt).toContain("## RUDF");
    expect(prompt).toContain("## Python UDF Guide");
    expect(prompt).toContain("## R UDF Guide");
  });

  test("tells the agent to ask before acting on a run error, under an operator or in the Last Run section", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    expect(prompt).toContain(`"Last Run Error (from the user's run)"`);
    expect(prompt).toContain('under an operator or in the "Last Run" section');
    expect(prompt).toContain("Ask before applying it.");
    expect(prompt).toContain("so if it does not match the operator's current properties, say so before diagnosing.");
    expect(prompt).not.toContain("has been edited since");
    expect(prompt).not.toContain("offer to run it again");
    expect(prompt).toContain('handled by the "Refine or fix operator in place" rule.');
    expect(prompt).not.toContain("handled by the next rule");
  });

  test("tells the agent that a run marked not finished is still in progress", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    expect(prompt).toContain(
      'A "Last Run" state marked "(not finished)" means the run is still in progress, so say so, treat its row counts as not final, and do not run, add or modify operators until the user agrees, because any of them starts a new run and ends the user\'s.'
    );
  });

  test("tells the agent what an error for an operator outside its workflow means", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    expect(prompt).toContain(
      'An error listed "for operators not in the workflow as you see it" belongs to an operator you cannot see or edit, so explain it and say that.'
    );
  });

  test("adds nothing to the rule for errors from the agent's own executions", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    expect(prompt).toContain("and fix it in place.\n- **Debug by isolating**");
  });

  test("reports that no operators are available when a restricted type is absent from metadata", () => {
    const prompt = buildSystemPrompt(makeMetadataStore(), ["MissingOperator"]);

    expect(prompt).toContain("No operators available.");
    expect(prompt).not.toContain("## Python UDF Guide");
    expect(prompt).not.toContain("## R UDF Guide");
  });
});
