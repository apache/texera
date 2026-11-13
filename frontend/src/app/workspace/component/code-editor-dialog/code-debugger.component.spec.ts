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
import { BreakpointInfo, ConsoleUpdateEvent } from "../../types/workflow-common.interface";
import { OperatorState } from "../../types/execute-workflow.interface";
import { commonTestProviders } from "../../../common/testing/test-utils";
import type { Mocked } from "vitest";
import type { MonacoBreakpoint } from "monaco-breakpoints";
import type * as monaco from "monaco-editor";
import * as monacoNs from "monaco-editor";
import { WorkflowWebsocketService } from "../../service/workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { mockPoint, mockPythonUDFPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
describe("CodeDebuggerComponent", () => {
  let component: CodeDebuggerComponent;
  let fixture: ComponentFixture<CodeDebuggerComponent>;

  let mockWorkflowStatusService: Mocked<WorkflowStatusService>;
  let mockUdfDebugService: Mocked<UdfDebugService>;

  let stateUpdateStream: Subject<Record<string, OperatorState>>;
  let debugState: Y.Map<BreakpointInfo>;

  const operatorId = "test-operator-id";

  beforeEach(async () => {
    // Initialize streams and spy objects
    stateUpdateStream = new Subject<Record<string, OperatorState>>();
    // Y.Map observers only fire when the map is attached to a Y.Doc (the doc
    // owns the transaction lifecycle that drives observation). A standalone
    // `new Y.Map()` accepts `.set()` but never notifies observers — production
    // never hits this because `UdfDebugService` hands out maps from a real
    // doc, but the spec used to construct a detached map.
    debugState = new Y.Doc().getMap<BreakpointInfo>("debug");

    mockWorkflowStatusService = { getStateUpdateStream: vi.fn() } as unknown as Mocked<WorkflowStatusService>;
    mockWorkflowStatusService.getStateUpdateStream.mockReturnValue(stateUpdateStream.asObservable());

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
    stateUpdateStream.complete();
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
    stateUpdateStream.next({ [operatorId]: OperatorState.Running });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalled();
    expect(rerenderSpy).toHaveBeenCalled();

    // Emit the same state again (should not trigger setup again)
    stateUpdateStream.next({ [operatorId]: OperatorState.Running });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call

    // Emit the paused state (should not trigger setup)
    stateUpdateStream.next({ [operatorId]: OperatorState.Paused });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call

    // Emit the running state once more (should not trigger setup)
    stateUpdateStream.next({ [operatorId]: OperatorState.Paused });

    tick();
    fixture.detectChanges(); // Trigger change detection

    expect(setupSpy).toHaveBeenCalledTimes(1); // No additional call
    expect(rerenderSpy).toHaveBeenCalledTimes(1); // No additional call
  }));

  it("should remove monaco breakpoint methods when state changes to Uninitialized", () => {
    const removeSpy = vi.spyOn(component, "removeMonacoBreakpointMethods");

    // Emit an Uninitialized state event
    stateUpdateStream.next({ [operatorId]: OperatorState.Uninitialized });

    fixture.detectChanges(); // Trigger change detection

    expect(removeSpy).toHaveBeenCalled();

    // Emit the same state again (should not trigger removal again)
    stateUpdateStream.next({ [operatorId]: OperatorState.Uninitialized });

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

    it("draws a breakpoint when the paused position gains an id without changing its condition", () => {
      debugState.set("3", { breakpointId: undefined, condition: "", hit: true });

      debugState.set("3", { breakpointId: 1, condition: "", hit: true });

      expect(monacoBreakpointStub.createSpecifyDecoration).toHaveBeenCalledExactlyOnceWith({
        startLineNumber: 3,
        startColumn: 1,
        endLineNumber: 3,
        endColumn: 1,
      });
      expect(monacoBreakpointStub.removeSpecifyDecoration).not.toHaveBeenCalled();
      expect(monacoBreakpointStub.setLineHighlight).toHaveBeenCalledWith(3);
      expect(monacoBreakpointStub.removeHighlight).not.toHaveBeenCalled();
    });

    it("removes the breakpoint when its id disappears but keeps the paused highlight", () => {
      debugState.set("3", { breakpointId: 1, condition: "", hit: true });
      monacoBreakpointStub.lineNumberAndDecorationIdMap.set(3, "dec-3");
      monacoBreakpointStub.createSpecifyDecoration.mockClear();

      debugState.set("3", { breakpointId: undefined, condition: "", hit: true });

      expect(monacoBreakpointStub.removeSpecifyDecoration).toHaveBeenCalledExactlyOnceWith("dec-3", 3);
      expect(monacoBreakpointStub.createSpecifyDecoration).not.toHaveBeenCalled();
      expect(monacoBreakpointStub.setLineHighlight).toHaveBeenCalledWith(3);
      expect(monacoBreakpointStub.removeHighlight).not.toHaveBeenCalled();
    });

    it("replaces the existing decoration when the breakpoint id changes", () => {
      debugState.set("3", { breakpointId: 1, condition: "", hit: true });
      monacoBreakpointStub.lineNumberAndDecorationIdMap.set(3, "dec-3");
      monacoBreakpointStub.createSpecifyDecoration.mockClear();

      debugState.set("3", { breakpointId: 2, condition: "", hit: true });

      expect(monacoBreakpointStub.removeSpecifyDecoration).toHaveBeenCalledExactlyOnceWith("dec-3", 3);
      expect(monacoBreakpointStub.createSpecifyDecoration).toHaveBeenCalledTimes(1);
      expect(monacoBreakpointStub.setLineHighlight).toHaveBeenCalledWith(3);
    });

    it("does not draw a phantom breakpoint when an id-less position's condition changes", () => {
      debugState.set("3", { breakpointId: undefined, condition: "x > 5", hit: true });

      debugState.set("3", { breakpointId: undefined, condition: "", hit: true });

      expect(monacoBreakpointStub.createSpecifyDecoration).not.toHaveBeenCalled();
      expect(monacoBreakpointStub.removeSpecifyDecoration).not.toHaveBeenCalled();
      expect(monacoBreakpointStub.setLineHighlight).toHaveBeenCalledWith(3);
    });

    it("does not redraw an existing breakpoint when its id and condition are unchanged", () => {
      debugState.set("3", { breakpointId: 1, condition: "", hit: true });
      monacoBreakpointStub.createSpecifyDecoration.mockClear();

      debugState.set("3", { breakpointId: 1, condition: "", hit: true });

      expect(monacoBreakpointStub.createSpecifyDecoration).not.toHaveBeenCalled();
      expect(monacoBreakpointStub.removeSpecifyDecoration).not.toHaveBeenCalled();
      expect(monacoBreakpointStub.setLineHighlight).toHaveBeenCalledWith(3);
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

describe("CodeDebuggerComponent paused-line breakpoint flow", () => {
  const operatorId = mockPythonUDFPredicate.operatorID;
  const workerId = "worker1";
  let fixture: ComponentFixture<CodeDebuggerComponent>;
  let service: UdfDebugService;
  let debugState: Y.Map<BreakpointInfo>;
  let consoleEvents: Subject<ConsoleUpdateEvent>;
  let stateUpdates: Subject<Record<string, OperatorState>>;
  let websocket: Mocked<WorkflowWebsocketService>;
  let model: monaco.editor.ITextModel;
  let mouseDown: (event: monaco.editor.IEditorMouseEvent) => void;

  function pdbReply(title: string) {
    consoleEvents.next({
      operatorId,
      messages: [
        {
          workerId,
          timestamp: { seconds: 0, nanos: 0 },
          source: "(Pdb)",
          msgType: { name: "DEBUGGER" },
          title,
          message: "",
        },
      ],
    });
  }

  function gutterClick() {
    mouseDown({
      event: { leftButton: true },
      target: {
        type: monacoNs.editor.MouseTargetType.GUTTER_GLYPH_MARGIN,
        detail: { isAfterLines: false },
        position: { lineNumber: 2 },
      },
    } as unknown as monaco.editor.IEditorMouseEvent);
  }

  function command(cmd: string) {
    return ["DebugCommandRequest", { operatorId, workerId, cmd }];
  }

  function breakpointDecorations() {
    return model
      .getAllDecorations()
      .filter(decoration =>
        ["monaco-breakpoint", "monaco-conditional-breakpoint"].includes(decoration.options.glyphMarginClassName ?? "")
      );
  }

  function highlightedLines() {
    return model
      .getAllDecorations()
      .filter(decoration => decoration.options.className === "monaco-line-highlight")
      .map(decoration => decoration.range.startLineNumber);
  }

  function expectPausedBreakpoint(breakpointId: number) {
    expect(service.getDebugState(operatorId)).toBe(debugState);
    expect(debugState.get("2")).toEqual({ breakpointId, condition: "", hit: true });
    expect(breakpointDecorations()).toHaveLength(1);
    expect(breakpointDecorations()[0].range.startLineNumber).toBe(2);
    expect(breakpointDecorations()[0].options.glyphMarginClassName).toBe("monaco-breakpoint");
    expect(highlightedLines()).toEqual([2]);
    expect(fixture.componentInstance.monacoBreakpoint!.getBreakpoints()).toEqual([2]);
  }

  beforeEach(async () => {
    consoleEvents = new Subject<ConsoleUpdateEvent>();
    stateUpdates = new Subject<Record<string, OperatorState>>();
    websocket = {
      send: vi.fn(),
      subscribeToEvent: vi.fn(() => consoleEvents.asObservable()),
    } as unknown as Mocked<WorkflowWebsocketService>;

    await TestBed.configureTestingModule({
      imports: [CodeDebuggerComponent],
      providers: [
        UdfDebugService,
        WorkflowActionService,
        { provide: WorkflowWebsocketService, useValue: websocket },
        {
          provide: WorkflowStatusService,
          useValue: { getStateUpdateStream: () => stateUpdates.asObservable(), getCurrentState: () => ({}) },
        },
        { provide: ExecuteWorkflowService, useValue: { getWorkerIds: () => [workerId] } },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    TestBed.inject(WorkflowActionService).addOperator(mockPythonUDFPredicate, mockPoint);
    service = TestBed.inject(UdfDebugService);
    debugState = service.getDebugState(operatorId);
    model = monacoNs.editor.createModel("first\nsecond\nthird", "plaintext");
    const disposable = () => ({ dispose: vi.fn() });
    // Use the real controller/model; only editor event delivery and transport are mocked.
    const editor = {
      getModel: () => model,
      onMouseMove: vi.fn(disposable),
      onMouseDown: vi.fn((handler: typeof mouseDown) => {
        mouseDown = handler;
        return disposable();
      }),
      onDidChangeModelContent: vi.fn(disposable),
      onDidChangeCursorPosition: vi.fn(disposable),
    } as unknown as monaco.editor.IStandaloneCodeEditor;
    fixture = TestBed.createComponent(CodeDebuggerComponent);
    fixture.componentInstance.currentOperatorId = operatorId;
    fixture.componentInstance.monacoEditor = editor;
    fixture.detectChanges();
    stateUpdates.next({ [operatorId]: OperatorState.Running });
  });

  afterEach(() => {
    stateUpdates.complete();
    consoleEvents.complete();
    fixture?.componentInstance.removeMonacoBreakpointMethods();
    fixture?.destroy();
    model?.dispose();
  });

  it.each(["gutter", "Console"])("keeps a stepped line paused and draws the %s-added breakpoint", input => {
    pdbReply("> /tmp/udf.py(1)process_tuple()");
    service.doStep(operatorId, workerId);
    pdbReply("> /tmp/udf.py(2)process_tuple()");
    expect(highlightedLines()).toEqual([2]);
    expect(breakpointDecorations()).toHaveLength(0);

    // Console commands bypass this component; their acknowledgement enters the same service.
    if (input === "gutter") gutterClick();
    expect(websocket.send.mock.calls).toEqual(
      input === "gutter" ? [command("next"), command("break 2")] : [command("next")]
    );
    const sentBeforeAck = [...websocket.send.mock.calls];

    pdbReply("Breakpoint 1 at /tmp/udf.py:2");
    expectPausedBreakpoint(1);
    pdbReply("Breakpoint 1 at /tmp/udf.py:2");
    expectPausedBreakpoint(1);
    expect(websocket.send.mock.calls).toEqual(sentBeforeAck);

    service.doContinue(operatorId, workerId);
    expect(highlightedLines()).toEqual([]);
    expect(breakpointDecorations()).toHaveLength(1);
    expect(debugState.get("2")?.hit).toBe(false);
    expect(websocket.send.mock.calls).toEqual([...sentBeforeAck, command("continue")]);
  });

  it.each(["", "x > 5"])("clears and re-adds the hit breakpoint with prior condition %j", condition => {
    pdbReply("Breakpoint 1 at /tmp/udf.py:2");
    pdbReply("> /tmp/udf.py(2)process_tuple()");
    service.doUpdateBreakpointCondition(operatorId, 2, condition);
    expect(breakpointDecorations()).toHaveLength(1);
    expect(breakpointDecorations()[0].options.glyphMarginClassName).toBe(
      condition ? "monaco-conditional-breakpoint" : "monaco-breakpoint"
    );
    websocket.send.mockClear();

    gutterClick();
    pdbReply("Deleted breakpoint 1 at /tmp/udf.py:2");
    expect(breakpointDecorations()).toHaveLength(0);
    expect(highlightedLines()).toEqual([2]);
    gutterClick();
    pdbReply("Breakpoint 2 at /tmp/udf.py:2");

    expectPausedBreakpoint(2);
    expect(websocket.send.mock.calls).toEqual([command("clear 1"), command("break 2")]);
  });

  it("removes a collaborator-cleared breakpoint while keeping its line paused", () => {
    pdbReply("Breakpoint 1 at /tmp/udf.py:2");
    pdbReply("> /tmp/udf.py(2)process_tuple()");
    expectPausedBreakpoint(1);
    websocket.send.mockClear();

    const otherEditor = new Y.Doc();
    try {
      const localDoc = debugState.doc!;
      Y.applyUpdate(otherEditor, Y.encodeStateAsUpdate(localDoc));
      const otherDebugState = otherEditor.getMap<Y.Map<BreakpointInfo>>("debugActions").get(operatorId)!;
      otherDebugState.delete("2");
      otherDebugState.set("2", { breakpointId: undefined, condition: "", hit: true });

      // Both remote transactions arrive together as an update to the existing entry.
      Y.applyUpdate(localDoc, Y.encodeStateAsUpdate(otherEditor, Y.encodeStateVector(localDoc)));

      expect(debugState.get("2")).toEqual({ breakpointId: undefined, condition: "", hit: true });
      expect(breakpointDecorations()).toHaveLength(0);
      expect(highlightedLines()).toEqual([2]);
      expect(websocket.send).not.toHaveBeenCalled();
    } finally {
      otherEditor.destroy();
    }
  });

  it("draws a running addition and preserves its automatic-continue handshake", () => {
    gutterClick();
    pdbReply("Breakpoint 1 at /tmp/udf.py:2");

    expect(websocket.send.mock.calls).toEqual([command("break 2"), command("continue")]);
    expect(debugState.get("2")).toEqual({ breakpointId: 1, condition: "", hit: false });
    expect(breakpointDecorations()).toHaveLength(1);
    expect(highlightedLines()).toEqual([]);
  });
});
/**
 * setupMonacoBreakpointMethods is the one uncovered block in the component: the suite above stubs it
 * out because the minimal editor mock cannot back a real MonacoBreakpoint. It decides which gutter
 * glyph a line gets and what a gutter click does, so it is worth driving directly.
 */
describe("CodeDebuggerComponent breakpoint gutter", () => {
  const GUTTER = monacoNs.editor.MouseTargetType.GUTTER_GLYPH_MARGIN;
  const CONTENT_TEXT = monacoNs.editor.MouseTargetType.CONTENT_TEXT;

  let component: CodeDebuggerComponent;
  let fixture: ComponentFixture<CodeDebuggerComponent>;
  let debugService: {
    getDebugState: ReturnType<typeof vi.fn>;
    doModifyBreakpoint: ReturnType<typeof vi.fn>;
    getCondition: ReturnType<typeof vi.fn>;
  };
  let editor: any;
  let mouseDown: ((evt: any) => void) | undefined;
  let firstMouseDownDisposable: { dispose: ReturnType<typeof vi.fn> } | undefined;

  const operatorId = "op-under-debug";

  /**
   * A stand-in editor exposing only what MonacoBreakpoint and the override touch. Every listener
   * hands back its own spy disposable so the dispose-before-reregister step is observable.
   */
  function makeEditor() {
    const disposable = () => ({ dispose: vi.fn() });
    const model = {
      deltaDecorations: vi.fn(() => [] as string[]),
      getLineCount: vi.fn(() => 100),
      getValue: vi.fn(() => ""),
      onDidChangeContent: vi.fn(disposable),
      uri: { toString: () => "inmemory://model/1" },
    };
    let downCount = 0;
    const base: Record<string, unknown> = {
      onMouseDown: vi.fn((h: (evt: any) => void) => {
        downCount += 1;
        const d = disposable();
        if (downCount === 1) {
          // The MonacoBreakpoint constructor registers first; the override replaces it.
          firstMouseDownDisposable = d;
        } else {
          mouseDown = h;
        }
        return d;
      }),
      getModel: vi.fn(() => model),
      deltaDecorations: vi.fn(() => [] as string[]),
      createDecorationsCollection: vi.fn(() => ({ set: vi.fn(), clear: vi.fn() })),
      dispose: vi.fn(),
    };
    // MonacoBreakpoint subscribes to a handful of editor events whose exact set is its own business;
    // anything named on* that is not explicitly stubbed above yields an inert disposable.
    const editor = new Proxy(base, {
      get(target, prop) {
        if (prop in target) {
          return target[prop as string];
        }
        if (typeof prop === "string" && prop.startsWith("on")) {
          return () => disposable();
        }
        return undefined;
      },
      has() {
        return true;
      },
    });
    return { model, editor };
  }

  function gutterClick(lineNumber: number, opts: { leftButton?: boolean; isAfterLines?: boolean; type?: number } = {}) {
    return {
      target: {
        type: opts.type ?? GUTTER,
        detail: { isAfterLines: opts.isAfterLines ?? false },
        position: { lineNumber },
      },
      event: { leftButton: opts.leftButton ?? true },
    };
  }

  /** Calls the installed override the way monaco-breakpoints would. exists=true means Exist. */
  function glyphFor(lineNumber: number, exists: boolean): string {
    const range = { startLineNumber: lineNumber, endLineNumber: lineNumber + 5 } as any;
    const decoration = (component.monacoBreakpoint as any)["createBreakpointDecoration"](range, exists ? 0 : 1);
    return decoration.options.glyphMarginClassName;
  }

  beforeEach(async () => {
    mouseDown = undefined;
    firstMouseDownDisposable = undefined;
    debugService = {
      getDebugState: vi.fn(() => new Y.Doc().getMap<BreakpointInfo>("debug")),
      doModifyBreakpoint: vi.fn(),
      getCondition: vi.fn(() => undefined),
    };

    TestBed.resetTestingModule();
    await TestBed.configureTestingModule({
      imports: [CodeDebuggerComponent],
      schemas: [CUSTOM_ELEMENTS_SCHEMA],
      providers: [
        {
          provide: WorkflowStatusService,
          useValue: { getStateUpdateStream: vi.fn(() => new Subject().asObservable()) },
        },
        { provide: UdfDebugService, useValue: debugService },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CodeDebuggerComponent);
    component = fixture.componentInstance;
    component.currentOperatorId = operatorId;

    const fake = makeEditor();
    editor = fake.editor;
    component.monacoEditor = editor;
    component.setupMonacoBreakpointMethods(editor);
  });

  afterEach(() => {
    // This suite drives a real MonacoBreakpoint, so tear it down the way the component does. Left
    // alive, its editor listeners would outlive the test that registered them.
    component.removeMonacoBreakpointMethods();
    component.monacoEditor?.dispose();
  });

  describe("gutter glyph", () => {
    it("marks a plain breakpoint", () => {
      expect(glyphFor(3, true)).toBe("monaco-breakpoint");
    });

    it("marks a breakpoint that carries a condition", () => {
      debugService.getCondition.mockReturnValue("count > 1");

      expect(glyphFor(3, true)).toBe("monaco-conditional-breakpoint");
    });

    it("does not treat a blank condition as a condition", () => {
      // A condition input left as whitespace must render as an ordinary breakpoint, otherwise the
      // gutter claims a condition the debugger will not apply.
      debugService.getCondition.mockReturnValue("   ");

      expect(glyphFor(3, true)).toBe("monaco-breakpoint");
    });

    it("marks a line that is only being hovered", () => {
      expect(glyphFor(3, false)).toBe("monaco-hover-breakpoint");
    });

    it("looks the condition up on the line the glyph starts at", () => {
      // The range spans several lines; reading endLineNumber would attribute another line's
      // condition to this glyph.
      glyphFor(7, true);

      expect(debugService.getCondition).toHaveBeenCalledWith(operatorId, 7);
    });

    it("keys the lookup to the operator being debugged", () => {
      glyphFor(2, true);

      expect(debugService.getCondition).toHaveBeenCalledWith(operatorId, expect.anything());
    });
  });

  describe("gutter clicks", () => {
    it("replaces the library's own mouse-down handler rather than adding to it", () => {
      // Two live handlers would both toggle, so a single click would add and immediately remove a
      // breakpoint.
      expect(firstMouseDownDisposable!.dispose).toHaveBeenCalledTimes(1);
      expect(editor.onMouseDown).toHaveBeenCalledTimes(2);
    });

    it("toggles the breakpoint on a left click in the gutter", () => {
      mouseDown!(gutterClick(12));

      expect(debugService.doModifyBreakpoint).toHaveBeenCalledWith(operatorId, 12);
    });

    it("ignores a click below the last line", () => {
      mouseDown!(gutterClick(12, { isAfterLines: true }));

      expect(debugService.doModifyBreakpoint).not.toHaveBeenCalled();
    });

    it("ignores a click that is not on the gutter", () => {
      mouseDown!(gutterClick(12, { type: CONTENT_TEXT }));

      expect(debugService.doModifyBreakpoint).not.toHaveBeenCalled();
    });

    it("does not toggle on a right click", () => {
      // The right button opens the condition input instead; toggling as well would remove the
      // breakpoint the user is trying to annotate.
      mouseDown!(gutterClick(12, { leftButton: false }));

      expect(debugService.doModifyBreakpoint).not.toHaveBeenCalled();
    });

    it("opens the condition input for a right click on an existing breakpoint", () => {
      (component.monacoBreakpoint as any)["lineNumberAndDecorationIdMap"] = new Map([[12, "dec-1"]]);

      mouseDown!(gutterClick(12, { leftButton: false }));

      expect(component.breakpointConditionLine).toBe(12);
    });

    it("opens nothing for a right click on a line with no breakpoint", () => {
      (component.monacoBreakpoint as any)["lineNumberAndDecorationIdMap"] = new Map();

      mouseDown!(gutterClick(12, { leftButton: false }));

      expect(component.breakpointConditionLine).toBeUndefined();
    });
  });
});
