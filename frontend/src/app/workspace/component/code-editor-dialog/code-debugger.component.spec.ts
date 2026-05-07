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
describe("CodeDebuggerComponent", () => {
  let component: CodeDebuggerComponent;
  let fixture: ComponentFixture<CodeDebuggerComponent>;

  let mockWorkflowStatusService: any;
  let mockUdfDebugService: any;

  let statusUpdateStream: Subject<Record<string, OperatorStatistics>>;
  let debugState: Y.Map<BreakpointInfo>;

  const operatorId = "test-operator-id";

  beforeEach(async () => {
    // Initialize streams and spy objects
    statusUpdateStream = new Subject<Record<string, OperatorStatistics>>();
    debugState = new Y.Map<BreakpointInfo>();

    mockWorkflowStatusService = { getStatusUpdateStream: vi.fn() };
    mockWorkflowStatusService.getStatusUpdateStream.mockReturnValue(statusUpdateStream.asObservable());

    mockUdfDebugService = { getDebugState: vi.fn(), doModifyBreakpoint: vi.fn() };
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
    component.monacoEditor = { dispose: vi.fn() } as any;

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
    } as any;

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
});
