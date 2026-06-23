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

import { ReActStepDetailModalComponent } from "./react-step-detail-modal.component";

describe("ReActStepDetailModalComponent", () => {
  let component: ReActStepDetailModalComponent;

  beforeEach(() => {
    // The component has no injected dependencies and its display helpers are
    // pure, so it can be exercised directly.
    component = new ReActStepDetailModalComponent();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("closeModal", () => {
    it("hides the modal and emits the new visibility", () => {
      component.visible = true;
      const emitted: boolean[] = [];
      component.visibleChange.subscribe(v => emitted.push(v));

      component.closeModal();

      expect(component.visible).toBe(false);
      expect(emitted).toEqual([false]);
    });
  });

  describe("formatResult / formatJson", () => {
    it("returns a string result unchanged", () => {
      expect(component.formatResult("plain\ntext")).toBe("plain\ntext");
    });

    it("pretty-prints an object result", () => {
      expect(component.formatResult({ a: 1 })).toBe(JSON.stringify({ a: 1 }, null, 2));
    });

    it("formatJson always pretty-prints", () => {
      expect(component.formatJson({ a: 1 })).toBe(JSON.stringify({ a: 1 }, null, 2));
    });
  });

  describe("getToolResult", () => {
    it("returns null when there are no tool results", () => {
      expect(component.getToolResult({} as any, 0)).toBeNull();
    });

    it("returns null when the index is out of range", () => {
      expect(component.getToolResult({ toolResults: [{ output: "x" }] } as any, 5)).toBeNull();
    });

    it("prefers output, then result, then the raw entry", () => {
      const step = {
        toolResults: [{ output: "o" }, { result: "r" }, { foo: "bar" }],
      } as any;
      expect(component.getToolResult(step, 0)).toBe("o");
      expect(component.getToolResult(step, 1)).toBe("r");
      expect(component.getToolResult(step, 2)).toEqual({ foo: "bar" });
    });
  });

  describe("getToolOperatorAccess / hasOperatorAccess", () => {
    it("returns null when the step has no operator access", () => {
      expect(component.getToolOperatorAccess({} as any, 0)).toBeNull();
      expect(component.hasOperatorAccess({} as any)).toBe(false);
    });

    it("returns the access entry for a tool-call index", () => {
      const access = { viewedOperatorIds: ["v"], addedOperatorIds: [], modifiedOperatorIds: [] };
      const step = { operatorAccess: new Map([[0, access]]) } as any;
      expect(component.getToolOperatorAccess(step, 0)).toBe(access);
      expect(component.getToolOperatorAccess(step, 1)).toBeNull();
      expect(component.hasOperatorAccess(step)).toBe(true);
    });

    it("reports no access for an empty map", () => {
      expect(component.hasOperatorAccess({ operatorAccess: new Map() } as any)).toBe(false);
    });
  });

  describe("getMessageRoleColor", () => {
    it("maps known roles and falls back to default", () => {
      expect(component.getMessageRoleColor("user")).toBe("blue");
      expect(component.getMessageRoleColor("assistant")).toBe("orange");
      expect(component.getMessageRoleColor("tool")).toBe("green");
      expect(component.getMessageRoleColor("system")).toBe("default");
    });
  });

  describe("getTextFromMessage", () => {
    it("returns empty string when there is no content", () => {
      expect(component.getTextFromMessage(null)).toBe("");
      expect(component.getTextFromMessage({})).toBe("");
    });

    it("returns string content directly", () => {
      expect(component.getTextFromMessage({ content: "hello" })).toBe("hello");
    });

    it("joins the text parts of array content", () => {
      const msg = {
        content: [
          { type: "text", text: "line1" },
          { type: "tool-call", toolName: "x" },
          { type: "text", text: "line2" },
        ],
      };
      expect(component.getTextFromMessage(msg)).toBe("line1\nline2");
    });
  });

  describe("getToolCallSummaries", () => {
    it("returns an empty array for non-array content", () => {
      expect(component.getToolCallSummaries({ content: "x" })).toEqual([]);
    });

    it("summarizes tool-call parts and defaults the operatorId", () => {
      const msg = {
        content: [
          { type: "tool-call", toolName: "addOperator", args: { operatorId: "op1", k: "v" } },
          { type: "tool-call", toolName: "noOp", input: {} },
          { type: "text", text: "ignored" },
        ],
      };
      expect(component.getToolCallSummaries(msg)).toEqual([
        { toolName: "addOperator", operatorId: "op1", fullArgs: { operatorId: "op1", k: "v" } },
        { toolName: "noOp", operatorId: "", fullArgs: {} },
      ]);
    });
  });

  describe("getToolCallStrings (function-call formatting)", () => {
    it("renders tool calls as toolName(key=value, ...)", () => {
      const msg = {
        content: [{ type: "tool-call", toolName: "filter", args: { col: "age", n: 5 } }],
      };
      expect(component.getToolCallStrings(msg)).toEqual(['filter(col="age", n=5)']);
    });

    it("truncates long string and non-string argument values", () => {
      const longString = "a".repeat(80);
      const longArray = Array.from({ length: 40 }, (_, i) => i);
      const msg = {
        content: [{ type: "tool-call", toolName: "t", args: { s: longString, arr: longArray } }],
      };
      const [rendered] = component.getToolCallStrings(msg);
      expect(rendered).toContain(`s="${"a".repeat(60)}..."`);
      expect(rendered).toContain(`arr=${JSON.stringify(longArray).substring(0, 60)}...`);
    });
  });

  describe("getToolResultFullItems / getToolResultItems", () => {
    function toolMessage(): any {
      return {
        content: [
          { type: "tool-result", toolCallId: "tc1", result: "short result" },
          { type: "tool-result", toolCallId: "tc2", output: { rows: 3 } },
          { type: "tool-result", toolCallId: "tc3", content: "after context compaction" },
        ],
      };
    }

    beforeEach(() => {
      // inputMessages provides the toolCallId -> toolName map.
      component.step = {
        inputMessages: [
          {
            role: "assistant",
            content: [
              { type: "tool-call", toolCallId: "tc1", toolName: "addOperator" },
              { type: "tool-call", toolCallId: "tc2", toolName: "executeOperator" },
            ],
          },
        ],
      } as any;
    });

    it("returns [] for non-array content", () => {
      expect(component.getToolResultFullItems({ content: "x" })).toEqual([]);
      expect(component.getToolResultItems({ content: "x" })).toEqual([]);
    });

    it("resolves tool names, content, token count and the trimmed flag", () => {
      const items = component.getToolResultFullItems(toolMessage());

      // tc1: mapped name, raw string content, ~len/4 tokens, not trimmed
      expect(items[0]).toEqual({
        toolName: "addOperator",
        resultContent: "short result",
        tokenCount: Math.ceil("short result".length / 4),
        isTrimmed: false,
      });
      // tc2: object output is pretty-printed
      expect(items[1].toolName).toBe("executeOperator");
      expect(items[1].resultContent).toBe(JSON.stringify({ rows: 3 }, null, 2));
      // tc3: unmapped id falls back to the toolCallId; "context compaction" => trimmed
      expect(items[2].toolName).toBe("tc3");
      expect(items[2].isTrimmed).toBe(true);
    });

    it("getToolResultItems returns name, token count and trimmed flag", () => {
      const items = component.getToolResultItems(toolMessage());
      expect(items[0]).toEqual({
        toolName: "addOperator",
        tokenCount: Math.ceil("short result".length / 4),
        isTrimmed: false,
      });
      expect(items[2].isTrimmed).toBe(true);
    });
  });
});
