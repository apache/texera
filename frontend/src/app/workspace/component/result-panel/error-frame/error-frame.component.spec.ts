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

import { ComponentFixture, TestBed } from "@angular/core/testing";

import { ErrorFrameComponent } from "./error-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";

function fatalError(overrides: Partial<WorkflowFatalError> = {}): WorkflowFatalError {
  return {
    message: "msg",
    details: "details",
    operatorId: "op1",
    workerId: "w1",
    type: { name: "GENERAL" },
    timestamp: { nanos: 0, seconds: 0 },
    ...overrides,
  } as WorkflowFatalError;
}

describe("ErrorFrameComponent", () => {
  let component: ErrorFrameComponent;
  let fixture: ComponentFixture<ErrorFrameComponent>;
  let executeWorkflowService: ExecuteWorkflowService;
  let compilingService: WorkflowCompilingService;
  let actionService: WorkflowActionService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ErrorFrameComponent, HttpClientTestingModule, NzDropDownModule],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ErrorFrameComponent);
    component = fixture.componentInstance;
    executeWorkflowService = TestBed.inject(ExecuteWorkflowService);
    compilingService = TestBed.inject(WorkflowCompilingService);
    actionService = TestBed.inject(WorkflowActionService);
    fixture.detectChanges();
  });

  afterEach(() => vi.restoreAllMocks());

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("renderError", () => {
    it("groups the errors by their type name", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({ type: { name: "TYPE_A" } }),
        fatalError({ type: { name: "TYPE_A" } }),
        fatalError({ type: { name: "TYPE_B" } }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      expect([...component.categoryToErrorMapping.keys()]).toEqual(["TYPE_A", "TYPE_B"]);
      expect(component.categoryToErrorMapping.get("TYPE_A")).toHaveLength(2);
      expect(component.categoryToErrorMapping.get("TYPE_B")).toHaveLength(1);
    });

    it("appends the compilation errors from the compiling service", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({
        op1: fatalError({ type: { name: "COMPILATION_ERROR" }, message: "boom" }),
      });

      component.renderError();

      expect(component.categoryToErrorMapping.get("COMPILATION_ERROR")).toHaveLength(1);
    });

    it("keeps only the errors of the bound operator when operatorId is set", () => {
      component.operatorId = "op1";
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({ operatorId: "op1", type: { name: "X" } }),
        fatalError({ operatorId: "op2", type: { name: "X" } }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      expect(component.categoryToErrorMapping.get("X")).toHaveLength(1);
      expect(component.categoryToErrorMapping.get("X")![0].operatorId).toBe("op1");
    });

    it("strips the exception prefix and 'requirement failed:' for COMPILATION_ERROR", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({
          type: { name: "COMPILATION_ERROR" },
          message: "java.lang.RuntimeException: something broke",
          details: "requirement failed: bad input",
        }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      const error = component.categoryToErrorMapping.get("COMPILATION_ERROR")![0];
      expect(error.message).toBe("something broke");
      expect(error.details).toBe("bad input");
    });

    it("leaves the message untouched for non-formatted error types", () => {
      vi.spyOn(executeWorkflowService, "getErrorMessages").mockReturnValue([
        fatalError({ type: { name: "RESOURCE_ERROR" }, message: "java.lang.RuntimeException: keep me" }),
      ]);
      vi.spyOn(compilingService, "getWorkflowCompilationErrors").mockReturnValue({});

      component.renderError();

      expect(component.categoryToErrorMapping.get("RESOURCE_ERROR")![0].message).toBe(
        "java.lang.RuntimeException: keep me"
      );
    });
  });

  describe("onClickGotoButton", () => {
    it("highlights the offending operator via the workflow action service", () => {
      const highlight = vi.spyOn(actionService, "highlightOperators").mockImplementation(() => {});

      component.onClickGotoButton("op-42");

      expect(highlight).toHaveBeenCalledWith(false, "op-42");
    });
  });
  /**
   * The frame's template decides what an error list looks like: the all-operators banner, the empty
   * state, the grouping into categories, and — the part with real teeth — whether a "focus operator"
   * shortcut is offered at all. The suite above builds the category map and never renders it.
   */
  describe("rendered errors", () => {
    /** Renders the frame for the given errors, optionally scoped to one operator. */
    function render(errors: WorkflowFatalError[], scopedTo?: string): HTMLElement {
      component.operatorId = scopedTo;
      component.categoryToErrorMapping = errors.reduce((acc, e) => {
        const key = e.type.name;
        acc.set(key, [...(acc.get(key) ?? []), e]);
        return acc;
      }, new Map<string, WorkflowFatalError[]>());
      fixture.detectChanges();
      return fixture.nativeElement as HTMLElement;
    }

    function gotoIcons(): HTMLElement[] {
      return Array.from((fixture.nativeElement as HTMLElement).querySelectorAll<HTMLElement>(".goto-operator-icon"));
    }

    it("announces that it is showing every operator's errors", () => {
      const el = render([fatalError()]);

      expect(el.querySelector(".all-errors-notification")).not.toBeNull();
    });

    it("drops that banner once the frame is scoped to one operator", () => {
      const el = render([fatalError()], "op1");

      expect(el.querySelector(".all-errors-notification")).toBeNull();
    });

    it("says so when there is nothing to report, and only then", () => {
      const el = render([]);
      expect(el.textContent).toContain("No error to display.");

      render([fatalError()]);
      expect((fixture.nativeElement as HTMLElement).textContent).not.toContain("No error to display.");
    });

    it("groups the errors under their category headings", () => {
      const el = render([
        fatalError({ type: { name: "COMPILATION" } as any }),
        fatalError({ type: { name: "EXECUTION" } as any }),
      ]);

      const headings = Array.from(el.querySelectorAll(".error-category")).map(h => h.textContent?.trim());
      expect(headings).toEqual(["COMPILATION:", "EXECUTION:"]);
    });

    it("shows each error's message as the heading and its details in the body", () => {
      const el = render([fatalError({ message: "boom", details: "stack trace here" })]);

      expect(el.querySelector(".ant-collapse-header")?.textContent).toContain("boom");
      expect(el.querySelector(".error-message")?.textContent).toContain("stack trace here");
    });

    it("offers a jump to the operator that failed", () => {
      render([fatalError({ operatorId: "op-broken" })]);

      expect(gotoIcons().length).toBe(1);
    });

    it("offers no jump for an error with no operator behind it", () => {
      // "unknown operator" is the sentinel the backend sends for errors that belong to no operator;
      // offering the shortcut would navigate the canvas to nothing.
      render([fatalError({ operatorId: "unknown operator" })]);

      expect(gotoIcons()).toEqual([]);
    });

    it("offers no jump to the operator already being shown", () => {
      // Scoped to op1 and the error is op1's: the shortcut would be a no-op.
      render([fatalError({ operatorId: "op1" })], "op1");

      expect(gotoIcons()).toEqual([]);
    });

    it("jumps to the operator the error names", () => {
      const spy = vi.spyOn(component, "onClickGotoButton").mockImplementation(() => {});
      render([fatalError({ operatorId: "op-broken" })]);

      gotoIcons()[0].click();

      expect(spy).toHaveBeenCalledWith("op-broken");
    });

    it("does not toggle the panel when the jump is clicked", () => {
      // The icon lives in the collapse header, so without stopPropagation the panel would open or
      // close underneath the user on the way to another operator.
      vi.spyOn(component, "onClickGotoButton").mockImplementation(() => {});
      const el = render([fatalError({ operatorId: "op-broken" })]);
      const panelBefore = el.querySelectorAll(".ant-collapse-item-active").length;

      gotoIcons()[0].click();
      fixture.detectChanges();

      expect((fixture.nativeElement as HTMLElement).querySelectorAll(".ant-collapse-item-active").length).toBe(
        panelBefore
      );
    });
  });
});
