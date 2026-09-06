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
import { makeMetadataFixture } from "./util/metadata-fixture";
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";

describe("buildSystemPrompt", () => {
  test("renders the base template and every operator when no allow-list is given", () => {
    const prompt = buildSystemPrompt(makeMetadataFixture());
    expect(prompt).toContain("You are a data science Copilot");
    expect(prompt).toContain("## CSVFileScan");
    expect(prompt).toContain("Description: Load CSV data");
    expect(prompt).toContain("## Filter");
    expect(prompt).toContain("## PythonUDFV2");
  });

  test("includes both the Python and R UDF guides when all operators are allowed", () => {
    const prompt = buildSystemPrompt(makeMetadataFixture());
    expect(prompt).toContain("## Python UDF Guide");
    expect(prompt).toContain("## R UDF Guide");
  });

  test("restricts the operator section to the allow-list", () => {
    const prompt = buildSystemPrompt(makeMetadataFixture(), ["Filter"]);
    expect(prompt).toContain("## Filter");
    expect(prompt).not.toContain("## CSVFileScan");
    expect(prompt).not.toContain("## PythonUDFV2");
  });

  test("omits both UDF guides when the allow-list has no UDF operator", () => {
    const prompt = buildSystemPrompt(makeMetadataFixture(), ["Filter"]);
    expect(prompt).not.toContain("## Python UDF Guide");
    expect(prompt).not.toContain("## R UDF Guide");
  });

  test("includes only the Python guide when PythonUDFV2 is the sole allowed operator", () => {
    const prompt = buildSystemPrompt(makeMetadataFixture(), ["PythonUDFV2"]);
    expect(prompt).toContain("## Python UDF Guide");
    expect(prompt).not.toContain("## R UDF Guide");
  });

  test("falls back to a placeholder when no operator metadata is available", () => {
    const prompt = buildSystemPrompt(new WorkflowSystemMetadata());
    expect(prompt).toContain("No operators available.");
  });
});
