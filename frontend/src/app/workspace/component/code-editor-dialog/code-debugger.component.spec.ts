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

import { CUSTOM_ELEMENTS_SCHEMA } from "@angular/core";
import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { CodeDebuggerComponent } from "./code-debugger.component";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { Subject } from "rxjs";
import * as Y from "yjs";
import { BreakpointInfo } from "../../types/workflow-common.interface";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import type { MonacoBreakpoint } from "monaco-breakpoints";
import type * as monaco from "monaco-editor";
describe("CodeDebuggerComponent", () => {
  let component: CodeDebuggerComponent;
  let fixture: ComponentFixture<CodeDebuggerComponent>;

  let mockWorkflowStatusService: Mocked<WorkflowStatusService>;
  let mockUdfDebugService: Mocked<UdfDebugService>;

  let statusUpdateStream: Subject<Record<string, OperatorStatistics>>;
  let debugState: Y.Map<BreakpointInfo>;

  const operatorId = "test-operator-id";

  beforeEach(async () => {
    // Initialize streams and spy objects
    statusUpdateStream = new Subject<Record<string, OperatorStatistics>>();
    // Y.Map observers only fire when the map is attached to a Y.Doc (the doc
    // owns the transaction lifecycle that drives observation). A standalone
    // `new Y.Map()` accepts `.set()` but never notifies observers — production
    // never hits this because `UdfDebugService` hands out maps from a real
    // doc, but the spec used to construct a detached map.
    debugState = new Y.Doc().getMap<BreakpointInfo>("debug");

    mockWorkflowStatusService = { getStatusUpdateStream: vi.fn() } as unknown as Mocked<WorkflowStatusService>;
    mockWorkflowStatusService.getStatusUpdateStream.mockReturnValue(statusUpdateStream.asObservable());

    mockUdfDebugService = {
      getDebugState: vi.fn(),
      doModifyBreakpoint: vi.fn(),
    } as unknown as Mocked<UdfDebugService>;
    mockUdfDebugService.getDebugState.mockReturnValue(debugState);

    await TestBed.configureTestingModule({
      imports: [CodeDebuggerComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
      providers: [
        { provide: WorkflowStatusService, useValue: mockWorkflowStatusService },
        { provide: UdfDebugService, useValue: mockUdfDebugService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CodeDebuggerComponent);
    component = fixture.componentInstance;

    // Set required input properties
    component.currentOperatorId = operatorId;
    component.monacoEditor = { dispose: vi.fn() } as unknown as monaco.editor.IStandaloneCodeEditor;

    // Trigger change detection to ensure view updates
    fixture.detectChanges();
  });

  afterEach(() => {
    // Clean up streams to prevent memory leaks
    statusUpdateStream.complete();
    component.monacoEditor?.dispose();
  });

  it("should create the component", () => {
    expect(component).toBeTruthy();
  });

  it("should setup monaco breakpoint methods when state is Running", fakeAsync(() => {
    // Stub the real implementations: setupMonacoBreakpointMethods constructs
    // a `MonacoBreakpoint` over a real monaco editor instance, which calls
    // editor.onMouseMove / onMouseDown — APIs the test's minimal
    // `monacoEditor` mock doesn't expose. The behavior under test is the
    // state-machine wiring, not the breakpoint plumbing itself.
    const setupSpy = vi.spyOn(component, "setupMonacoBreakpointMethods").mockImplementation(() => {});
    const rerenderSpy = vi.spyOn(component, "rerenderExistingBreakpoints").mockImplementation(() => {});

    // Emit a Running state event
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Running,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        outputPortMetrics: {},
      },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalled();
    expect(rerenderSpy).toHaveBeenCalled();

    // Emit the same state again (should not trigger setup again)
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Running,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        outputPortMetrics: {},
      },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call

    // Emit the paused state (should not trigger setup)
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Paused,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        outputPortMetrics: {},
      },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call

    // Emit the running state once more (should not trigger setup)
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Paused,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        outputPortMetrics: {},
      },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call
  }));

  it("should remove monaco breakpoint methods when state changes to Uninitialized", () => {
    const removeSpy = vi.spyOn(component, "removeMonacoBreakpointMethods");

    // Emit an Uninitialized state event
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Uninitialized,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        outputPortMetrics: {},
      },
    });

    fixture.detectChanges(); // Trigger change detection

    expect(removeSpy).toHaveBeenCalled();

    // Emit the same state again (should not trigger removal again)
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Uninitialized,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
        inputPortMetrics: {},
        outputPortMetrics: {},
      },
    });

    expect(removeSpy).toHaveBeenCalledTimes(1); // No additional call
  });

  it("should call doModifyBreakpoint on left click", () => {
    // Simulate a left click on line 1
    component["onMouseLeftClick"](1);

    // Verify that the mock service was called with the correct arguments
    expect(mockUdfDebugService.doModifyBreakpoint).toHaveBeenCalledWith(operatorId, 1);
  });

  it("should set breakpoint condition input on right click", () => {
    // Mock a valid decoration map
    component.monacoBreakpoint = {
      lineNumberAndDecorationIdMap: new Map([
        [1, "breakpoint1"],
        [2, "breakpoint2"],
      ]),
    } as unknown as MonacoBreakpoint;

    // Simulate a right click on line 1, it should switch to 1
    component["onMouseRightClick"](1);
    expect(component.breakpointConditionLine).toBe(1);

    // Simulate a right click on line 3, which does not have a breakpoint. no changes should occur
    component["onMouseRightClick"](3);
    expect(component.breakpointConditionLine).toBe(1);

    // Simulate a right click on line 2, it should switch to 2
    component["onMouseRightClick"](2);
    expect(component.breakpointConditionLine).toBe(2);

    // Simulate a right click on line 1, it should switch to 1
    component["onMouseRightClick"](1);
    expect(component.breakpointConditionLine).toBe(1);
  });

  it("should reset the breakpoint condition input when closed", () => {
    // Set a condition line and close it
    component.breakpointConditionLine = 1;
    component.closeBreakpointConditionInput();

    expect(component.breakpointConditionLine).toBeUndefined();
  });

  describe("registerBreakpointRenderingHandler", () => {
    // Stand-in for the MonacoBreakpoint instance set up in
    // setupMonacoBreakpointMethods. The observer in
    // registerBreakpointRenderingHandler reaches into these as bracket
    // properties, so we wire up the four it touches: createSpecifyDecoration,
    // removeSpecifyDecoration, setLineHighlight, removeHighlight, plus the
    // lineNumberAndDecorationIdMap reads by removeBreakpointDecoration.
    let monacoBreakpointStub: {
      createSpecifyDecoration: ReturnType<typeof vi.fn>;
      removeSpecifyDecoration: ReturnType<typeof vi.fn>;
      setLineHighlight: ReturnType<typeof vi.fn>;
      removeHighlight: ReturnType<typeof vi.fn>;
      lineNumberAndDecorationIdMap: Map<number, string>;
      mouseDownDisposable: { dispose: ReturnType<typeof vi.fn> };
      dispose: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
      monacoBreakpointStub = {
        createSpecifyDecoration: vi.fn(),
        removeSpecifyDecoration: vi.fn(),
        setLineHighlight: vi.fn(),
        removeHighlight: vi.fn(),
        lineNumberAndDecorationIdMap: new Map<number, string>(),
        mouseDownDisposable: { dispose: vi.fn() },
        dispose: vi.fn(),
      };
      component.monacoBreakpoint = monacoBreakpointStub as unknown as MonacoBreakpoint;
    });

    it("draws a decoration when a debug-state entry with breakpointId is added", () => {
      debugState.set("3", { breakpointId: 3, condition: "", hit: false } as BreakpointInfo);

      expect(monacoBreakpointStub.createSpecifyDecoration).toHaveBeenCalledWith({
        startLineNumber: 3,
        startColumn: 1,
        endLineNumber: 3,
        endColumn: 1,
      });
    });

    it("does not draw a decoration when the added entry has no breakpointId", () => {
      debugState.set("4", { breakpointId: undefined, condition: "", hit: false } as BreakpointInfo);

      expect(monacoBreakpointStub.createSpecifyDecoration).not.toHaveBeenCalled();
    });

    it("removes the decoration when a debug-state entry with breakpointId is deleted", () => {
      debugState.set("5", { breakpointId: 5, condition: "", hit: false } as BreakpointInfo);
      // Mirror the lookup that removeBreakpointDecoration performs.
      monacoBreakpointStub.lineNumberAndDecorationIdMap.set(5, "dec-5");
      monacoBreakpointStub.createSpecifyDecoration.mockClear();

      debugState.delete("5");

      expect(monacoBreakpointStub.removeSpecifyDecoration).toHaveBeenCalledWith("dec-5", 5);
    });

    it("does not call removeSpecifyDecoration when the deleted entry had no breakpointId", () => {
      // Y.Map.observe surfaces a delete with oldValue.breakpointId === undefined
      // when the entry never had a breakpoint to begin with.
      debugState.set("6", { breakpointId: undefined, condition: "", hit: false } as BreakpointInfo);
      debugState.delete("6");

      expect(monacoBreakpointStub.removeSpecifyDecoration).not.toHaveBeenCalled();
    });

    it("calls setLineHighlight when an entry's hit flag flips on", () => {
      debugState.set("7", { breakpointId: 7, condition: "", hit: false } as BreakpointInfo);

      debugState.set("7", { breakpointId: 7, condition: "", hit: true } as BreakpointInfo);

      expect(monacoBreakpointStub.setLineHighlight).toHaveBeenCalledWith(7);
    });

    it("calls removeHighlight when an entry's hit flag flips off", () => {
      debugState.set("8", { breakpointId: 8, condition: "", hit: true } as BreakpointInfo);

      debugState.set("8", { breakpointId: 8, condition: "", hit: false } as BreakpointInfo);

      expect(monacoBreakpointStub.removeHighlight).toHaveBeenCalled();
    });

    it("re-creates the decoration when the condition string changes on an existing entry", () => {
      debugState.set("9", { breakpointId: 9, condition: "", hit: false } as BreakpointInfo);
      monacoBreakpointStub.lineNumberAndDecorationIdMap.set(9, "dec-9");
      monacoBreakpointStub.createSpecifyDecoration.mockClear();
      monacoBreakpointStub.removeSpecifyDecoration.mockClear();

      debugState.set("9", { breakpointId: 9, condition: "x > 0", hit: false } as BreakpointInfo);

      expect(monacoBreakpointStub.removeSpecifyDecoration).toHaveBeenCalledWith("dec-9", 9);
      expect(monacoBreakpointStub.createSpecifyDecoration).toHaveBeenCalledWith({
        startLineNumber: 9,
        startColumn: 1,
        endLineNumber: 9,
        endColumn: 1,
      });
    });
  });

  describe("rerenderExistingBreakpoints", () => {
    let monacoBreakpointStub: {
      createSpecifyDecoration: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
      monacoBreakpointStub = { createSpecifyDecoration: vi.fn() };
      component.monacoBreakpoint = monacoBreakpointStub as unknown as MonacoBreakpoint;
    });

    it("re-creates a decoration for every existing entry that has a breakpointId", () => {
      debugState.set("10", { breakpointId: 10, condition: "", hit: false } as BreakpointInfo);
      debugState.set("11", { breakpointId: undefined, condition: "", hit: false } as BreakpointInfo);
      debugState.set("12", { breakpointId: 12, condition: "y > 0", hit: false } as BreakpointInfo);
      monacoBreakpointStub.createSpecifyDecoration.mockClear();

      component.rerenderExistingBreakpoints();

      // Only entries 10 and 12 carry a breakpointId; 11 is filtered out by the
      // early-return inside rerenderExistingBreakpoints.
      const lines = monacoBreakpointStub.createSpecifyDecoration.mock.calls.map(c => c[0].startLineNumber);
      expect(lines.sort()).toEqual([10, 12]);
    });
  });

  describe("removeMonacoBreakpointMethods", () => {
    it("disposes the mouseDownDisposable and the breakpoint instance when defined", () => {
      const disposeMouseDown = vi.fn();
      const disposeBreakpoint = vi.fn();
      component.monacoBreakpoint = {
        mouseDownDisposable: { dispose: disposeMouseDown },
        dispose: disposeBreakpoint,
      } as unknown as MonacoBreakpoint;

      component.removeMonacoBreakpointMethods();

      expect(disposeMouseDown).toHaveBeenCalledOnce();
      expect(disposeBreakpoint).toHaveBeenCalledOnce();
    });

    it("returns early without touching anything when monacoBreakpoint is undefined", () => {
      component.monacoBreakpoint = undefined;
      // Should simply no-op.
      expect(() => component.removeMonacoBreakpointMethods()).not.toThrow();
    });
  });
});
