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

  test("describes the Documentation Read section between the ongoing task and the dataflow", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    const bullets = [
      "- `# Ongoing Task` — the current task, including turns you've taken so far",
      "- `# Documentation Read` — the output of your searchDocs and readDoc calls in the current task (omitted if none)",
      "- `# Current Dataflow` — the live DAG: every operator's current state",
    ];
    expect(prompt).toContain(bullets.join("\n"));
    expect(prompt).not.toContain("three top-level sections");
  });

  test("shows the Documentation Read section in the layout the way the context renders it", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    expect(prompt).toContain(
      [
        "# Documentation Read",
        "Output of your searchDocs and readDoc calls in this task, oldest first.",
        "",
        '## Turn 1: searchDocs {"query":"<keywords>"}',
        "  <the tool's output>",
        "",
        "# Current Dataflow",
      ].join("\n")
    );
  });

  test("keeps the rule about reading documentation and adds the docs-tools rule right after it", () => {
    const prompt = buildSystemPrompt(makeMetadataStore());

    const kept =
      "- **Read documentation first**: When the task mentions abstract concepts, load documentation to understand exact definitions.";
    const added = "- **Answer questions about Texera from its documentation**:";
    expect(prompt).toContain(`${kept}\n${added}`);
    expect(prompt).toContain("call searchDocs first, then readDoc on the best match, and answer with the page link.");
    expect(prompt).toContain("Only cite links that these tools returned");
    expect(prompt).toContain("say so when the documentation does not cover the question");
    expect(prompt).toContain("Do not edit the workflow for a question.");
  });

  test("reports that no operators are available when a restricted type is absent from metadata", () => {
    const prompt = buildSystemPrompt(makeMetadataStore(), ["MissingOperator"]);

    expect(prompt).toContain("No operators available.");
    expect(prompt).not.toContain("## Python UDF Guide");
    expect(prompt).not.toContain("## R UDF Guide");
  });
});
