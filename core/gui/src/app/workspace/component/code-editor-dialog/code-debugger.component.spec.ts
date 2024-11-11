import { ComponentFixture, fakeAsync, TestBed, tick } from "@angular/core/testing";
import { CodeDebuggerComponent } from "./code-debugger.component";
import { WorkflowStatusService } from "../../service/workflow-status/workflow-status.service";
import { UdfDebugService } from "../../service/operator-debug/udf-debug.service";
import { Subject } from "rxjs";
import * as Y from "yjs";
import { BreakpointInfo } from "../../types/workflow-common.interface";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import * as monaco from "monaco-editor";

describe("CodeDebuggerComponent", () => {
  let component: CodeDebuggerComponent;
  let fixture: ComponentFixture<CodeDebuggerComponent>;

  let mockWorkflowStatusService: jasmine.SpyObj<WorkflowStatusService>;
  let mockUdfDebugService: jasmine.SpyObj<UdfDebugService>;

  let statusUpdateStream: Subject<Record<string, OperatorStatistics>>;
  let debugState: Y.Map<BreakpointInfo>;

  const operatorId = "test-operator-id";

  beforeEach(async () => {
    // Initialize streams and spy objects
    statusUpdateStream = new Subject<Record<string, OperatorStatistics>>();
    debugState = new Y.Map<BreakpointInfo>();

    mockWorkflowStatusService = jasmine.createSpyObj("WorkflowStatusService", ["getStatusUpdateStream"]);
    mockWorkflowStatusService.getStatusUpdateStream.and.returnValue(statusUpdateStream.asObservable());

    mockUdfDebugService = jasmine.createSpyObj("UdfDebugService", ["getDebugState", "doModifyBreakpoint"]);
    mockUdfDebugService.getDebugState.and.returnValue(debugState);

    await TestBed.configureTestingModule({
      declarations: [CodeDebuggerComponent],
      providers: [
        { provide: WorkflowStatusService, useValue: mockWorkflowStatusService },
        { provide: UdfDebugService, useValue: mockUdfDebugService },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CodeDebuggerComponent);
    component = fixture.componentInstance;

    // Set required input properties
    component.currentOperatorId = operatorId;
    // Create and attach a <div> for Monaco editor
    const editorElement = document.createElement("div");
    editorElement.id = "editor-container";
    editorElement.style.width = "800px";
    editorElement.style.height = "600px";
    document.body.appendChild(editorElement); // Attach to document body

    // Initialize the Monaco editor with the created element
    component.monacoEditor = monaco.editor.create(editorElement, {
      value: "function hello() {\n\tconsole.log(\"Hello, world!\");\n}",
      language: "javascript",
    });

    // Trigger change detection to ensure view updates
    fixture.detectChanges();
  });

  afterEach(() => {
    // Clean up streams to prevent memory leaks
    statusUpdateStream.complete();
    component.monacoEditor.dispose();
  });

  it("should create the component", () => {
    expect(component).toBeTruthy();
  });

  it("should setup monaco breakpoint methods when state is Running", fakeAsync(() => {
    const setupSpy = spyOn(component, "setupMonacoBreakpointMethods").and.callThrough();
    const rerenderSpy = spyOn(component, "rerenderExistingBreakpoints").and.callThrough();

    // Emit a Running state event
    statusUpdateStream.next({
      [operatorId]: { operatorState: OperatorState.Running, aggregatedOutputRowCount: 0, aggregatedInputRowCount: 0 },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalled();
    expect(rerenderSpy).toHaveBeenCalled();

    // Emit the same state again (should not trigger setup again)
    statusUpdateStream.next({
      [operatorId]: { operatorState: OperatorState.Running, aggregatedOutputRowCount: 0, aggregatedInputRowCount: 0 },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call

    // Emit the paused state (should not trigger setup)
    statusUpdateStream.next({
      [operatorId]: { operatorState: OperatorState.Paused, aggregatedOutputRowCount: 0, aggregatedInputRowCount: 0 },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call

    // Emit the running state once more (should not trigger setup)
    statusUpdateStream.next({
      [operatorId]: { operatorState: OperatorState.Paused, aggregatedOutputRowCount: 0, aggregatedInputRowCount: 0 },
    });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call
  }));

  it("should remove monaco breakpoint methods when state changes to Uninitialized", () => {
    const removeSpy = spyOn(component, "removeMonacoBreakpointMethods").and.callThrough();

    // Emit an Uninitialized state event
    statusUpdateStream.next({
      [operatorId]: {
        operatorState: OperatorState.Uninitialized,
        aggregatedOutputRowCount: 0,
        aggregatedInputRowCount: 0,
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
