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
import { By } from "@angular/platform-browser";
import { BehaviorSubject } from "rxjs";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { AiFixFrameComponent } from "./ai-fix-frame.component";
import { AiWorkflowFixerService, FixState, UNSUPPORTED_MESSAGE } from "./ai-workflow-fixer.service";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowCompilingService } from "../../../service/compile-workflow/workflow-compiling.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { WorkflowFatalError } from "../../../types/workflow-websocket.interface";
import { ConsoleMessage } from "../../../types/workflow-common.interface";

const IDLE: FixState = { operatorId: "", errorMessage: "", errorType: "unsupported", status: "idle" };

const READY_CODE_FIX: FixState = {
  operatorId: "op1",
  errorMessage: "KeyError: 'email'",
  errorType: "missing_column",
  status: "ready",
  suggestedFix: {
    type: "code_change",
    original: "yield t['email']",
    suggested: "yield t['user_email']\nyield t",
    explanation: "the column is user_email",
    confidence: "high",
  },
};

const READY_PROPERTY_FIX: FixState = {
  ...READY_CODE_FIX,
  errorType: "model_not_found",
  suggestedFix: {
    type: "property_change",
    original: "gpt-4-turb",
    suggested: "gpt-4-turbo",
    explanation: "the model name has a typo",
    confidence: "medium",
    fieldName: "model",
  },
};

const fatalError = (over: Partial<WorkflowFatalError> = {}) =>
  ({
    message: "KeyError: 'email'",
    details: "Traceback ...",
    operatorId: "op1",
    workerId: "w1",
    type: { name: "EXECUTION_FAILURE" },
    timestamp: { nanos: 0, seconds: 0 },
    ...over,
  }) as WorkflowFatalError;

const consoleMessage = (name: string, message: string) =>
  ({
    workerId: "w1",
    timestamp: { nanos: 0, seconds: 0 },
    msgType: { name },
    source: "udf.py:process_tuple:7",
    title: "KeyError: 'email'",
    message,
  }) as ConsoleMessage;

describe("AiFixFrameComponent", () => {
  let component: AiFixFrameComponent;
  let fixture: ComponentFixture<AiFixFrameComponent>;
  let state$: BehaviorSubject<FixState>;
  let analyzeError: ReturnType<typeof vi.fn>;
  let applyFix: ReturnType<typeof vi.fn>;
  let discardFix: ReturnType<typeof vi.fn>;
  let getErrorMessages: ReturnType<typeof vi.fn>;
  let getConsoleMessages: ReturnType<typeof vi.fn>;
  let getPortInputSchema: ReturnType<typeof vi.fn>;
  let operatorProperties: Record<string, unknown>;

  const render = (state: FixState) => {
    state$.next(state);
    fixture.detectChanges();
  };
  const query = (css: string) => fixture.debugElement.query(By.css(css));
  // Angular surrounds an interpolation with layout whitespace, so compare the content itself.
  const texts = (css: string) =>
    fixture.debugElement.queryAll(By.css(css)).map(el => el.nativeElement.textContent.trim());

  beforeEach(async () => {
    state$ = new BehaviorSubject<FixState>(IDLE);
    analyzeError = vi.fn().mockResolvedValue(undefined);
    applyFix = vi.fn();
    discardFix = vi.fn();
    getErrorMessages = vi.fn().mockReturnValue([]);
    getConsoleMessages = vi.fn().mockReturnValue(undefined);
    getPortInputSchema = vi.fn().mockReturnValue([{ attributeName: "user_email", attributeType: "string" }]);
    operatorProperties = { code: "yield t['email']", workers: 1 };

    await TestBed.configureTestingModule({
      imports: [AiFixFrameComponent, HttpClientTestingModule],
      providers: [
        {
          provide: AiWorkflowFixerService,
          useValue: { getState$: () => state$.asObservable(), analyzeError, applyFix, discardFix },
        },
        { provide: ExecuteWorkflowService, useValue: { getErrorMessages } },
        {
          provide: WorkflowActionService,
          useValue: { getTexeraGraph: () => ({ getOperator: () => ({ operatorProperties }) }) },
        },
        { provide: WorkflowCompilingService, useValue: { getPortInputSchema } },
        { provide: WorkflowConsoleService, useValue: { getConsoleMessages } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(AiFixFrameComponent);
    component = fixture.componentInstance;
    component.operatorId = "op1";
    fixture.detectChanges();
  });

  afterEach(() => vi.restoreAllMocks());

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("the analyze button", () => {
    it("passes the fatal error, schema, code and properties to the service", () => {
      getErrorMessages.mockReturnValue([fatalError({ operatorId: "other" }), fatalError()]);

      query(".ai-fix-analyze").nativeElement.click();

      expect(analyzeError).toHaveBeenCalledWith(
        "op1",
        "KeyError: 'email'\nTraceback ...",
        [{ attributeName: "user_email", attributeType: "string" }],
        "yield t['email']",
        operatorProperties
      );
    });

    it("falls back to the Python traceback, which never becomes a fatal error", () => {
      getConsoleMessages.mockReturnValue([
        consoleMessage("PRINT", "hello"),
        consoleMessage("ERROR", "Traceback (most recent call last):\nKeyError: 'email'"),
      ]);

      query(".ai-fix-analyze").nativeElement.click();

      expect(analyzeError.mock.calls[0][1]).toEqual("Traceback (most recent call last):\nKeyError: 'email'");
    });

    it("does nothing when the operator has no error at all", () => {
      query(".ai-fix-analyze").nativeElement.click();
      expect(analyzeError).not.toHaveBeenCalled();
    });

    it("is disabled without an operator, and hidden once a suggestion is on screen", () => {
      component.operatorId = undefined;
      fixture.detectChanges();
      expect(query(".ai-fix-analyze").nativeElement.disabled).toBe(true);

      component.operatorId = "op1";
      render(READY_CODE_FIX);
      expect(query(".ai-fix-analyze")).toBeNull();
    });
  });

  it("shows a spinner while analyzing", () => {
    render({ ...READY_CODE_FIX, status: "analyzing", suggestedFix: undefined });

    expect(query("nz-spin")).toBeTruthy();
    expect(query(".ai-fix-status").nativeElement.textContent).toContain("Analyzing error with AI");
    expect(query(".ai-fix-actions")).toBeNull();
  });

  it("shows the not-supported message for an out-of-scope error", () => {
    render({ ...READY_CODE_FIX, errorType: "unsupported", suggestedFix: undefined });

    expect(query("nz-alert")).toBeTruthy();
    expect(fixture.nativeElement.textContent).toContain(UNSUPPORTED_MESSAGE);
    expect(query(".ai-fix-actions")).toBeNull();
  });

  it("renders the root cause, a confidence badge, and one diff line per line of the fix", () => {
    render(READY_CODE_FIX);

    expect(query(".ai-fix-explanation").nativeElement.textContent).toContain("the column is user_email");
    expect(query("nz-tag").nativeElement.textContent).toContain("high");
    expect(texts(".ai-fix-removed")).toEqual(["- yield t['email']"]);
    // The suggestion spans two lines, so it renders two added rows.
    expect(texts(".ai-fix-added")).toEqual(["+ yield t['user_email']", "+ yield t"]);
  });

  it("renders the field name and values instead of code for a property change", () => {
    render(READY_PROPERTY_FIX);

    expect(query(".ai-fix-field").nativeElement.textContent).toContain("model");
    expect(texts(".ai-fix-removed")).toEqual(["- gpt-4-turb"]);
    expect(texts(".ai-fix-added")).toEqual(["+ gpt-4-turbo"]);
  });

  it("delegates the apply and discard buttons to the service", () => {
    render(READY_CODE_FIX);

    query(".ai-fix-apply").nativeElement.click();
    query(".ai-fix-discard").nativeElement.click();

    expect(applyFix).toHaveBeenCalledTimes(1);
    expect(discardFix).toHaveBeenCalledTimes(1);
  });

  it("reports the applied state and offers a fresh analysis", () => {
    render({ ...READY_CODE_FIX, status: "applied" });

    expect(fixture.nativeElement.textContent).toContain("Fix applied");
    expect(query(".ai-fix-actions")).toBeNull();
    expect(query(".ai-fix-analyze")).toBeTruthy();
  });

  it("reports a failed analysis", () => {
    render({ ...READY_CODE_FIX, status: "error", suggestedFix: undefined });

    expect(fixture.nativeElement.textContent).toContain("could not suggest a fix");
  });

  it("maps confidence to a badge colour", () => {
    expect(component.confidenceColor("high")).toEqual("green");
    expect(component.confidenceColor("medium")).toEqual("gold");
    expect(component.confidenceColor("low")).toEqual("red");
  });
});
