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

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { SchemaForm } from "./schema-form";
import type { OperatorPropertySchema } from "./schema";
afterEach(cleanup);
const source = readFileSync(
  resolve(
    "../common/workflow-operator/src/main/scala/org/apache/texera/amber/operator/metadata/annotations/UIWidget.java"
  ),
  "utf8"
);
const widgets = Object.fromEntries(
  Array.from(source.matchAll(/public static final String (\w+) = ("(?:\\.|[^"\\])*");/g), match => [
    match[1],
    JSON.parse(JSON.parse(match[2])) as OperatorPropertySchema,
  ])
);
describe("backend-emitted UIWidget corpus", () => {
  it("recognizes every widget annotation emitted by the actual backend catalog", () => {
    expect(Object.keys(widgets)).toEqual([
      "UIWidgetTextArea",
      "UIWidgetCharDelimiter",
      "UIWidgetRegexDelimiter",
      "UIWidgetPassword",
    ]);
    for (const [name, widget] of Object.entries(widgets)) {
      render(
        <SchemaForm
          schema={{ type: "object", properties: { value: { type: "string", title: name, ...widget } } }}
          value={{ value: "" }}
          onChange={() => {}}
        />
      );
      expect(screen.queryByText(/Unsupported field widget/)).toBeNull();
      cleanup();
    }
  });
  it("preserves backend textarea, password and delimiter metadata without invented widget extensions", () => {
    const { rerender } = render(
      <SchemaForm
        schema={{
          type: "object",
          properties: { value: { type: "string", title: "Value", ...widgets.UIWidgetTextArea } },
        }}
        value={{ value: "" }}
        onChange={() => {}}
      />
    );
    expect(screen.getByRole("textbox", { name: "Value" }).tagName).toBe("TEXTAREA");
    rerender(
      <SchemaForm
        schema={{
          type: "object",
          properties: { value: { type: "string", title: "Value", ...widgets.UIWidgetPassword } },
        }}
        value={{ value: "secret" }}
        onChange={() => {}}
      />
    );
    expect((screen.getByLabelText("Value") as HTMLInputElement).type).toBe("password");
    rerender(
      <SchemaForm
        schema={{
          type: "object",
          properties: { value: { type: "string", title: "Value", ...widgets.UIWidgetCharDelimiter } },
        }}
        value={{ value: "invalid" }}
        onChange={() => {}}
      />
    );
    expect(
      screen
        .getAllByRole("alert")
        .map(node => node.textContent)
        .join(" ")
    ).toContain("must NOT have more than 1 characters");
  });
});
