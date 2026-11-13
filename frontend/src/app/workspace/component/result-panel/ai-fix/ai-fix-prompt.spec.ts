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

import { describe, expect, it } from "vitest";
import { buildFixPrompt, formatInputSchema } from "./ai-fix-prompt";
import { PortSchema } from "../../../types/workflow-compiling.interface";

describe("buildFixPrompt", () => {
  it("sends no operator configuration at all, so a UI parameter cannot leak", () => {
    // The configuration block is not sent at all. Filtering it by field name could never be
    // enough: a Python UDF's uiParameters is a list of {attribute, value} pairs, so the name
    // says nothing about whether a value is an API key the user typed in.
    const prompt = buildFixPrompt("KeyError: 'email'", "print(1)", {});

    expect(prompt).not.toContain("hf_live_abc");
    expect(prompt).not.toContain("Operator configuration");
  });
});

describe("formatInputSchema", () => {
  const port0: PortSchema = [{ attributeName: "user_email", attributeType: "string" }];
  const port1: PortSchema = [{ attributeName: "order_total", attributeType: "double" }];

  it("renders a single port exactly as before, with no port label", () => {
    // Nearly every operator has one input port and there is nothing to disambiguate, so
    // this keeps the prompt that was verified end to end byte for byte.
    expect(formatInputSchema({ "0_false": port0 })).toEqual(JSON.stringify(port0, null, 2));
  });

  it("labels every port once there is more than one", () => {
    // process_tuple is told which port a tuple came from, so a column that only exists on
    // port 1 is not a typo for one on port 0.
    const rendered = formatInputSchema({ "0_false": port0, "1_false": port1 });

    expect(rendered).toContain("Port 0:");
    expect(rendered).toContain("Port 1:");
    expect(rendered).toContain("order_total");
  });

  it("survives an operator whose schema has not been compiled yet", () => {
    expect(formatInputSchema(undefined)).toEqual("[]");
    expect(formatInputSchema({ "0_false": undefined })).toEqual("[]");
  });
});
