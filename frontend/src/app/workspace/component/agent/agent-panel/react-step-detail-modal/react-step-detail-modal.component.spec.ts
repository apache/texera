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

import { ApplicationRef } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NzModalService } from "ng-zorro-antd/modal";
import { ReActStepDetailModalComponent } from "./react-step-detail-modal.component";
import { ReActStep } from "../../../../service/agent/agent-types";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

describe("ReActStepDetailModalComponent", () => {
  let fixture: ComponentFixture<ReActStepDetailModalComponent>;
  let component: ReActStepDetailModalComponent;

  /** A step exercising every optional template section at once. */
  function maximalStep(): ReActStep {
    return {
      messageId: "msg-1",
      stepId: 3,
      timestamp: new Date("2026-06-11T12:34:56.789Z"),
      role: "agent",
      content: "Thinking about the workflow",
      isBegin: true,
      isEnd: false,
      toolCalls: [{ toolName: "addOperator", input: { operatorId: "op-1" } }],
      toolResults: [{ toolName: "addOperator", output: "operator added" }],
      usage: { inputTokens: 111, outputTokens: 22, totalTokens: 133, cachedInputTokens: 44 },
      inputMessages: [
        { role: "user", content: "add a csv scan" },
        {
          role: "assistant",
          content: [
            { type: "text", text: "adding" },
            { type: "tool-call", toolCallId: "tc1", toolName: "addOperator", args: { operatorId: "op-1" } },
          ],
        },
        { role: "tool", content: [{ type: "tool-result", toolCallId: "tc1", result: "ok" }] },
      ],
      operatorAccess: new Map([
        [0, { viewedOperatorIds: ["op-viewed"], addedOperatorIds: ["op-added"], modifiedOperatorIds: ["op-modified"] }],
      ]),
      id: "step-3",
      parentId: "step-2",
      beforeWorkflowContent: { operators: [] },
      afterWorkflowContent: { operators: [{}] },
    };
  }

  /** A step for which every optional section's *ngIf is false. */
  function minimalStep(): ReActStep {
    return {
      messageId: "msg-empty",
      stepId: 0,
      timestamp: new Date("2026-06-11T00:00:00.000Z"),
      role: "agent",
      content: "  ",
      isBegin: false,
      isEnd: false,
      id: "step-0",
    };
  }

  // The modal body renders inside the CDK overlay, which is a view attached to
  // ApplicationRef rather than to this fixture — tick() is what re-renders it.
  const tick = (): void => {
    fixture.detectChanges();
    TestBed.inject(ApplicationRef).tick();
  };

  const openWith = (step: ReActStep | null): void => {
    component.step = step;
    component.visible = true;
    tick();
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ReActStepDetailModalComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [
        // The declarative <nz-modal> in this component's template delegates
        // opening to NzModalService.create(), so the real service is required.
        NzModalService,
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ReActStepDetailModalComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    document.querySelectorAll(".cdk-overlay-container").forEach(el => el.remove());
  });

  it("should create", () => {
    fixture.detectChanges();
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

  describe("template rendering", () => {
    it("renders identification, content, usage, input messages, tool calls and operator access for a maximal step", () => {
      openWith(maximalStep());

      const body = document.querySelector(".ant-modal-body") as HTMLElement;
      expect(body).toBeTruthy();
      const text = body.textContent ?? "";

      // Step identification
      expect(text).toContain("Step Identification");
      expect(text).toContain("msg-1");
      expect(text).toContain("3");

      // Content + token usage
      expect(text).toContain("Thinking about the workflow");
      expect(text).toContain("Token Usage");
      expect(text).toContain("111");
      expect(text).toContain("22");
      expect(text).toContain("133");
      expect(text).toContain("44");

      // Input messages: one header per message, with per-role tag colors.
      expect(text).toContain("Input Messages (3)");
      expect(text).toContain("add a csv scan");
      const blueTags = Array.from(body.querySelectorAll(".ant-tag-blue")).map(el => el.textContent?.trim());
      expect(blueTags).toContain("user");
      const orangeTags = Array.from(body.querySelectorAll(".ant-tag-orange")).map(el => el.textContent?.trim());
      expect(orangeTags).toContain("assistant");
      const greenTags = Array.from(body.querySelectorAll(".ant-tag-green")).map(el => el.textContent?.trim());
      expect(greenTags).toContain("tool-result");
      // The tool message's result is ~1 token ("ok".length / 4 rounded up).
      expect(text).toContain("~1 tokens");

      // Tool calls: header, arguments JSON, result via the output alias.
      expect(text).toContain("Tool Calls (1)");
      expect(text).toContain("addOperator");
      expect(text).toContain('"operatorId": "op-1"');
      expect(text).toContain("operator added");

      // Operator access chips for tool-call index 0.
      expect(text).toContain("VIEWED:");
      expect(text).toContain("op-viewed");
      expect(text).toContain("ADDED:");
      expect(text).toContain("op-added");
      expect(text).toContain("MODIFIED:");
      expect(text).toContain("op-modified");

      // The empty-state branch must not render alongside real sections.
      expect(text).not.toContain("No additional details available for this step.");
    });

    it("shows the empty-state message when every optional section is absent", () => {
      openWith(minimalStep());

      const text = document.querySelector(".ant-modal-body")?.textContent ?? "";
      expect(text).toContain("No additional details available for this step.");
      expect(text).not.toContain("Token Usage");
      expect(text).not.toContain("Input Messages");
      expect(text).not.toContain("Tool Calls");
      // Whitespace-only content is treated as no content.
      expect(text).toContain("Step Identification");
    });

    it("hides the result and operator-access blocks without toolResults, and renders 'result' via formatResult", () => {
      const step = maximalStep();
      step.toolResults = undefined;
      step.operatorAccess = undefined;
      step.inputMessages = undefined;
      step.usage = undefined;
      openWith(step);

      let text = document.querySelector(".ant-modal-body")?.textContent ?? "";
      expect(text).toContain("Arguments:");
      expect(text).not.toContain("Result:");
      expect(text).not.toContain("Operator Access:");

      // A toolResult carrying only `result` (no `output`) goes through the
      // formatResult(JSON.stringify) path.
      component.step = { ...step, toolResults: [{ result: { rows: 3 } }] };
      tick();
      text = document.querySelector(".ant-modal-body")?.textContent ?? "";
      expect(text).toContain("Result:");
      expect(text).toContain('"rows": 3');
    });

    it("emits visibleChange(false) and closes when the modal close button is clicked", () => {
      const emitted: boolean[] = [];
      component.visibleChange.subscribe(v => emitted.push(v));
      openWith(minimalStep());

      const closeButton = document.querySelector(".ant-modal-close") as HTMLButtonElement;
      expect(closeButton).toBeTruthy();
      closeButton.click();
      tick();

      expect(emitted).toContain(false);
      expect(component.visible).toBe(false);
      expect(document.querySelector(".ant-modal")).toBeNull();
    });

    it("renders no overlay content while hidden, and an empty body when visible without a step", () => {
      component.step = maximalStep();
      component.visible = false;
      tick();
      expect(document.querySelector(".ant-modal")).toBeNull();
      expect(document.body.textContent).not.toContain("msg-1");

      // Visible but step-less: the modal chrome opens, the *ngIf="step" body stays out.
      component.step = null;
      component.visible = true;
      tick();
      expect(document.querySelector(".ant-modal")).toBeTruthy();
      expect(document.body.textContent).not.toContain("Step Identification");
    });
  });
});
