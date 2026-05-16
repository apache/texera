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
import { WorkflowSystemMetadata } from "./util/workflow-system-metadata";

describe("buildSystemPrompt", () => {
  test("includes both operator type and display name", () => {
    const metadata = new WorkflowSystemMetadata();
    metadata.loadFromMetadata({
      operators: [
        {
          operatorType: "SmartFileScan",
          operatorVersion: "1",
          jsonSchema: { properties: { fileName: { type: "string" } }, required: ["fileName"] },
          additionalMetadata: {
            userFriendlyName: "Smart Source",
            operatorGroupName: "Data Input",
            operatorDescription: "Auto-detects files and folders.",
            inputPorts: [],
            outputPorts: [{}],
          },
        },
      ],
      groups: [],
    });

    const prompt = buildSystemPrompt(metadata, ["SmartFileScan"]);

    expect(prompt).toContain("## SmartFileScan");
    expect(prompt).toContain("Display name: Smart Source");
    expect(prompt).toContain("Description: Auto-detects files and folders.");
  });
});
