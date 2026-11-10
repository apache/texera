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

import { afterEach, describe, expect, it, vi } from "vitest";
import * as Y from "yjs";
import { UiUdfParametersParser, createUiUdfParameterSync } from "./udf-parameters-sync";
import type { UiUdfParameterValue } from "./ui-udf-parameters";

afterEach(() => vi.useRealTimers());
const inOpen = (body: string) => `class ProcessTupleOperator(UDFOperatorV2):\n    def open(self):\n        ${body}\n`;

describe("React UDF declaration synchronization", () => {
  it("reuses the real Python and R parsers and reports duplicate/invalid declarations", () => {
    const parser = new UiUdfParametersParser();
    expect(parser.parse(inOpen('self.UiParameter("数", AttributeType.INT)'), "python")[0].attribute).toEqual({
      attributeName: "数",
      attributeType: "integer",
    });
    expect(parser.parse('UiParameter("model", AttributeType$STRING, value=Resource$MODEL)', "r")[0].inputType).toBe(
      "model"
    );
    expect(() =>
      parser.parse(
        inOpen('self.UiParameter("k", AttributeType.INT)\n        self.UiParameter("k", AttributeType.INT)'),
        "python"
      )
    ).toThrow("declared more than once");
    expect(() => parser.computeInsertion(inOpen("pass"), "", "integer", "python")).toThrow("name is required");
    expect(() => parser.computeInsertion(inOpen("pass"), "x", "unsupported", "python")).toThrow("not supported");
  });

  it("debounces shared code, preserves values by name/resource kind and removes obsolete declarations", async () => {
    vi.useFakeTimers();
    const doc = new Y.Doc();
    const code = doc.getText("code");
    code.insert(0, inOpen('self.UiParameter("k", AttributeType.INT)'));
    let rows: UiUdfParameterValue[] = [{ attribute: { attributeName: "k", attributeType: "integer" }, value: "42" }];
    const onRows = vi.fn(next => {
      rows = next;
    });
    const onError = vi.fn();
    const sync = createUiUdfParameterSync({ code, language: "python", getRows: () => rows, onRows, onError });
    expect(onRows).not.toHaveBeenCalled();
    doc.transact(() => {
      code.delete(0, code.length);
      code.insert(0, inOpen('self.UiParameter("MODEL", AttributeType.STRING, value=Resource.MODEL)'));
    });
    expect(onRows).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(200);
    expect(rows).toEqual([
      { attribute: { attributeName: "MODEL", attributeType: "string" }, value: "", inputType: "model" },
    ]);
    rows[0].value = "/model/o/name/v1";
    doc.transact(() => {
      code.delete(0, code.length);
      code.insert(0, inOpen('self.UiParameter("MODEL", AttributeType.STRING, value=Resource.DATASET)'));
    });
    await vi.advanceTimersByTimeAsync(200);
    expect(rows[0]).toMatchObject({ inputType: "dataset", value: "" });
    sync.dispose();
    doc.destroy();
  });

  it("inserts declarations into Y.Text immediately and detaches all observers/timers on dispose", async () => {
    vi.useFakeTimers();
    const doc = new Y.Doc();
    const code = doc.getText("code");
    code.insert(0, inOpen("pass"));
    let rows: UiUdfParameterValue[] = [];
    const onRows = vi.fn(next => {
      rows = next;
    });
    const sync = createUiUdfParameterSync({ code, language: "python", getRows: () => rows, onRows });
    sync.addParameter("enabled", "boolean");
    expect(code.toString()).toContain('self.enabled = self.UiParameter(name="enabled", type=AttributeType.BOOL).value');
    expect(rows[0]).toMatchObject({ attribute: { attributeName: "enabled", attributeType: "boolean" } });
    expect(() => sync.addParameter("enabled", "boolean")).toThrow("declared already");
    const count = onRows.mock.calls.length;
    sync.dispose();
    code.insert(0, "# changed\n");
    await vi.advanceTimersByTimeAsync(500);
    expect(onRows).toHaveBeenCalledTimes(count);
    expect(vi.getTimerCount()).toBe(0);
    doc.destroy();
  });
});
