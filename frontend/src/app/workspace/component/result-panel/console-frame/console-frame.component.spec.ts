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

import { ComponentFixture, TestBed, fakeAsync, flush, tick } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { Subject } from "rxjs";
import { ConsoleFrameComponent } from "./console-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowConsoleService } from "../../../service/workflow-console/workflow-console.service";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UdfDebugService } from "../../../service/operator-debug/udf-debug.service";
import { ExecutionState } from "../../../types/execute-workflow.interface";
import { ConsoleMessage } from "../../../types/workflow-common.interface";

function consoleMessage(name: string): ConsoleMessage {
  return {
    workerId: "w",
    timestamp: { nanos: 0, seconds: 0 },
    msgType: { name },
    source: "src",
    title: "title",
    message: "message",
  };
}

type StateEvent = { previous: { state: ExecutionState }; current: { state: ExecutionState } };

describe("ConsoleFrameComponent", () => {
  let component: ConsoleFrameComponent;
  let fixture: ComponentFixture<ConsoleFrameComponent>;

  let getWorkerIds: ReturnType<typeof vi.fn>;
  let getConsoleMessages: ReturnType<typeof vi.fn>;
  let skipTuples: ReturnType<typeof vi.fn>;
  let retryExecution: ReturnType<typeof vi.fn>;
  let send: ReturnType<typeof vi.fn>;
  let notifyError: ReturnType<typeof vi.fn>;
  let doStep: ReturnType<typeof vi.fn>;
  let doContinue: ReturnType<typeof vi.fn>;
  let executionStateStream: Subject<StateEvent>;
  let consoleUpdateStream: Subject<void>;

  beforeEach(async () => {
    getWorkerIds = vi.fn().mockReturnValue([]);
    getConsoleMessages = vi.fn().mockReturnValue([]);
    skipTuples = vi.fn();
    retryExecution = vi.fn();
    send = vi.fn();
    notifyError = vi.fn();
    doStep = vi.fn();
    doContinue = vi.fn();
    executionStateStream = new Subject<StateEvent>();
    consoleUpdateStream = new Subject<void>();

    await TestBed.configureTestingModule({
      imports: [ConsoleFrameComponent, HttpClientTestingModule, NzDropDownModule],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        {
          provide: ExecuteWorkflowService,
          useValue: {
            getExecutionStateStream: () => executionStateStream.asObservable(),
            getWorkerIds,
            skipTuples,
            retryExecution,
          },
        },
        {
          provide: WorkflowConsoleService,
          useValue: { getConsoleMessageUpdateStream: () => consoleUpdateStream.asObservable(), getConsoleMessages },
        },
        { provide: WorkflowWebsocketService, useValue: { send } },
        { provide: NotificationService, useValue: { error: notifyError } },
        { provide: UdfDebugService, useValue: { doStep, doContinue } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ConsoleFrameComponent);
    component = fixture.componentInstance;
    fixture.detectChanges(); // runs ngOnInit -> registerAutoConsoleRerender
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("pure helpers", () => {
    it("getWorkerIndex parses the trailing numeric token", () => {
      expect(component.getWorkerIndex("worker-op-3")).toBe(3);
      expect(component.getWorkerIndex("W-0-12")).toBe(12);
      expect(component.getWorkerIndex("")).toBe(0);
    });

    it("workerIdToAbbr prefixes the worker index with 'W'", () => {
      expect(component.workerIdToAbbr("worker-op-3")).toBe("W3");
    });

    it("getWorkerColor returns a deterministic hex color", () => {
      const color = component.getWorkerColor(0);
      expect(color).toMatch(/^#[0-9a-fA-F]{6}$/);
      expect(component.getWorkerColor(0)).toBe(color); // deterministic
      expect(component.getWorkerColor(5)).toMatch(/^#[0-9a-fA-F]{6}$/);
    });

    it("getMessageLabel maps the message type to its tag color", () => {
      expect(component.getMessageLabel(consoleMessage("PRINT"))).toBe("default");
      expect(component.getMessageLabel(consoleMessage("COMMAND"))).toBe("processing");
      expect(component.getMessageLabel(consoleMessage("DEBUGGER"))).toBe("warning");
      expect(component.getMessageLabel(consoleMessage("ERROR"))).toBe("error");
      expect(component.getMessageLabel(consoleMessage("UNKNOWN"))).toBe("");
    });
  });

  describe("console rendering", () => {
    it("clearConsole empties the message list", () => {
      component.consoleMessages = [consoleMessage("PRINT")];
      component.clearConsole();
      expect(component.consoleMessages).toEqual([]);
    });

    it("displayConsoleMessages loads the operator's messages from the service", () => {
      const messages = [consoleMessage("PRINT"), consoleMessage("ERROR")];
      getConsoleMessages.mockReturnValue(messages);

      component.displayConsoleMessages("op1");

      expect(getConsoleMessages).toHaveBeenCalledWith("op1");
      expect(component.consoleMessages).toEqual(messages);
    });

    it("renderConsole loads the worker ids and messages when an operator is set", () => {
      component.operatorId = "op1";
      getWorkerIds.mockReturnValue(["w-1", "w-2"]);
      getConsoleMessages.mockReturnValue([consoleMessage("PRINT")]);

      component.renderConsole();

      expect(component.workerIds).toEqual(["w-1", "w-2"]);
      expect(getConsoleMessages).toHaveBeenCalledWith("op1");
    });

    it("renderConsole is a no-op without an operator id", () => {
      component.operatorId = "";
      getWorkerIds.mockClear();
      component.renderConsole();
      expect(getWorkerIds).not.toHaveBeenCalled();
    });

    it("displayConsoleMessages falls back to an empty list for an operator the service has never seen", () => {
      // WorkflowConsoleService.getConsoleMessages returns undefined until the
      // operator has produced its first message; the frame must not render undefined.
      component.consoleMessages = [consoleMessage("PRINT")];
      getConsoleMessages.mockReturnValue(undefined);

      component.displayConsoleMessages("op-never-run");

      expect(component.consoleMessages).toEqual([]);
    });

    it("ngOnChanges adopts the newly bound operator id and re-renders for it", () => {
      // ResultPanelComponent recreates this frame with {operatorId, consoleInputEnabled}
      // inputs, so the operator the console follows arrives through ngOnChanges.
      getWorkerIds.mockReturnValue(["w-op2-1"]);
      getConsoleMessages.mockReturnValue([consoleMessage("COMMAND")]);

      component.ngOnChanges({
        operatorId: { currentValue: "op2", previousValue: "op1", firstChange: false, isFirstChange: () => false },
      });

      expect(component.operatorId).toBe("op2");
      expect(getWorkerIds).toHaveBeenCalledWith("op2");
      expect(getConsoleMessages).toHaveBeenCalledWith("op2");
      expect(component.workerIds).toEqual(["w-op2-1"]);
      expect(component.consoleMessages).toEqual([consoleMessage("COMMAND")]);
    });
  });

  describe("debug controls", () => {
    it("onClickContinue continues every worker via the debug service", () => {
      component.operatorId = "op1";
      component.workerIds = ["w-1", "w-2"];

      component.onClickContinue();

      expect(doContinue).toHaveBeenCalledTimes(2);
      expect(doContinue).toHaveBeenCalledWith("op1", "w-1");
      expect(doContinue).toHaveBeenCalledWith("op1", "w-2");
    });

    it("onClickStep steps every worker via the debug service", () => {
      component.operatorId = "op1";
      component.workerIds = ["w-1"];

      component.onClickStep();

      expect(doStep).toHaveBeenCalledWith("op1", "w-1");
    });

    it("onClickSkipTuples forwards the worker ids and surfaces failures", () => {
      component.workerIds = ["w-1", "w-2"];
      component.onClickSkipTuples();
      expect(skipTuples).toHaveBeenCalledWith(["w-1", "w-2"]);

      skipTuples.mockImplementation(() => {
        throw new Error("skip failed");
      });
      component.onClickSkipTuples();
      expect(notifyError).toHaveBeenCalledWith("skip failed");
    });

    it("onClickRetryTuples forwards the worker ids and surfaces failures", () => {
      component.workerIds = ["w-1"];
      component.onClickRetryTuples();
      expect(retryExecution).toHaveBeenCalledWith(["w-1"]);

      retryExecution.mockImplementation(() => {
        throw new Error("retry failed");
      });
      component.onClickRetryTuples();
      expect(notifyError).toHaveBeenCalledWith("retry failed");
    });

    it("submitDebugCommand sends the command to every worker when All Workers is targeted", () => {
      component.operatorId = "op1";
      component.workerIds = ["w-1", "w-2"];
      component.targetWorker = component.ALL_WORKERS;
      component.command = "break";

      component.submitDebugCommand();

      expect(send).toHaveBeenCalledTimes(2);
      expect(send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op1", workerId: "w-1", cmd: "break" });
      expect(send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op1", workerId: "w-2", cmd: "break" });
      expect(component.command).toBe(""); // input cleared after sending
    });

    it("submitDebugCommand sends only to the selected worker", () => {
      component.operatorId = "op1";
      component.workerIds = ["w-1", "w-2"];
      component.targetWorker = "w-2";
      component.command = "continue";

      component.submitDebugCommand();

      expect(send).toHaveBeenCalledTimes(1);
      expect(send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op1", workerId: "w-2", cmd: "continue" });
    });

    it("submitDebugCommand does nothing without an operator id", () => {
      component.workerIds = ["w-1"];
      component.command = "break";
      // operatorId is left undefined
      component.submitDebugCommand();
      expect(send).not.toHaveBeenCalled();
    });
  });

  describe("registerAutoConsoleRerender", () => {
    it("clears the console when execution transitions from Initializing to Running", () => {
      component.consoleMessages = [consoleMessage("PRINT")];

      executionStateStream.next({
        previous: { state: ExecutionState.Initializing },
        current: { state: ExecutionState.Running },
      });

      expect(component.consoleMessages).toEqual([]);
    });

    it("re-renders the console on any other execution state change", () => {
      component.operatorId = "op1";
      getWorkerIds.mockReturnValue(["w-9"]);
      getConsoleMessages.mockReturnValue([consoleMessage("DEBUGGER")]);

      executionStateStream.next({
        previous: { state: ExecutionState.Running },
        current: { state: ExecutionState.Paused },
      });

      expect(component.workerIds).toEqual(["w-9"]);
      expect(component.consoleMessages).toEqual([consoleMessage("DEBUGGER")]);
    });

    it("re-renders the console when a console message update arrives", () => {
      component.operatorId = "op1";
      getWorkerIds.mockReturnValue(["w-5"]);
      getConsoleMessages.mockReturnValue([consoleMessage("PRINT")]);

      consoleUpdateStream.next();

      expect(component.workerIds).toEqual(["w-5"]);
      expect(component.consoleMessages).toEqual([consoleMessage("PRINT")]);
    });
  });

  // The tests above drive the class directly; these render the template so its
  // *ngFor / *ngIf / (click) / [(ngModel)] branches actually execute.
  describe("template rendering", () => {
    // A message with a body (renders the collapse panel) that carries a worker id,
    // and one with an empty body (renders the plain title branch) and no worker.
    const withBody: ConsoleMessage = {
      ...consoleMessage("PRINT"),
      message: "hello body",
      title: "header A",
      workerId: "w-0",
      source: "srcA",
    };
    const noBody: ConsoleMessage = {
      ...consoleMessage("ERROR"),
      message: "",
      title: "plain B",
      workerId: "",
      source: "srcB",
    };

    it("renders one row per message with its body, source, timestamp and worker tags", () => {
      component.consoleMessages = [withBody, noBody];
      component.showSource = true;
      component.showTimestamp = true;
      fixture.detectChanges();

      const rows = fixture.debugElement.queryAll(By.css(".console-message-entry"));
      expect(rows.length).toBe(2);

      // non-empty message -> collapse header; empty message -> plain title
      const text = fixture.nativeElement.textContent as string;
      const collapseHeader = fixture.debugElement.query(By.css(".collapse-message-header"));
      expect(collapseHeader).toBeTruthy();
      expect(collapseHeader.nativeElement.textContent).toContain("header A");
      expect(text).toContain("plain B");

      // both rows show a source tag; both show a timestamp tag (the rendered date
      // string is intentionally NOT asserted — it is timezone-dependent)
      expect(fixture.debugElement.queryAll(By.css(".source-tag")).length).toBe(2);
      expect(fixture.debugElement.queryAll(By.css(".timestamp-tag")).length).toBe(2);
      // only the message with a worker id renders the worker tag
      expect(fixture.debugElement.queryAll(By.css(".worker-tag")).length).toBe(1);
    });

    it("hides the source and timestamp tags when the toggles are off", () => {
      component.consoleMessages = [withBody, noBody];
      component.showSource = false;
      component.showTimestamp = false;
      fixture.detectChanges();

      expect(fixture.debugElement.queryAll(By.css(".console-message-entry")).length).toBe(2);
      expect(fixture.debugElement.queryAll(By.css(".source-tag")).length).toBe(0);
      expect(fixture.debugElement.queryAll(By.css(".timestamp-tag")).length).toBe(0);
    });

    it("does not render the debug input group when console input is disabled", () => {
      component.consoleInputEnabled = false;
      fixture.detectChanges();
      expect(fixture.debugElement.query(By.css(".console-input-container"))).toBeNull();
    });

    it("renders the debug input group and wires its buttons and command input when enabled", () => {
      component.operatorId = "op1";
      component.workerIds = ["w-0", "w-1"];
      component.targetWorker = component.ALL_WORKERS;
      component.consoleInputEnabled = true;
      fixture.detectChanges();

      expect(fixture.debugElement.query(By.css(".console-input-container"))).toBeTruthy();

      // clicking each action button reaches its handler / service
      const buttons = fixture.debugElement.queryAll(By.css(".console-input-container button"));
      expect(buttons.length).toBe(4);
      buttons.forEach(button => button.triggerEventHandler("click", null));
      expect(skipTuples).toHaveBeenCalled();
      expect(retryExecution).toHaveBeenCalled();
      expect(doStep).toHaveBeenCalled();
      expect(doContinue).toHaveBeenCalled();

      // entering a command and pressing enter submits it through the websocket
      // (target input[nz-input] specifically — the nz-select renders its own input too)
      component.command = "break";
      fixture.debugElement.query(By.css("input[nz-input]")).triggerEventHandler("keyup.enter", null);
      // targetWorker defaults to ALL_WORKERS, so the command is broadcast to every worker id
      expect(send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op1", workerId: "w-0", cmd: "break" });
      expect(send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op1", workerId: "w-1", cmd: "break" });
    });

    it("sends the text typed into the command box, then clears it", fakeAsync(() => {
      component.operatorId = "op1";
      component.workerIds = ["w-7"];
      component.consoleInputEnabled = true;
      fixture.detectChanges();

      // Drive the input the way a user does — set the DOM value and let the
      // DefaultValueAccessor push it back through [(ngModel)]. Nothing here
      // assigns component.command, so a broken view-to-model binding would send
      // the empty string instead of the typed text.
      const input = fixture.debugElement.query(By.css("input[nz-input]")).nativeElement as HTMLInputElement;
      input.value = "print(row)";
      input.dispatchEvent(new Event("input", { bubbles: true }));
      fixture.detectChanges();

      input.dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));
      fixture.detectChanges();

      expect(send).toHaveBeenCalledWith("DebugCommandRequest", {
        operatorId: "op1",
        workerId: "w-7",
        cmd: "print(row)",
      });
      // the box is emptied after submitting, so the next command starts clean
      expect(input.value).toBe("");
      flush();
    }));

    it("narrows the command to the worker picked in the target-worker dropdown", fakeAsync(() => {
      component.operatorId = "op1";
      component.workerIds = ["w-7", "w-8"];
      component.consoleInputEnabled = true;
      fixture.detectChanges();

      // open the nz-select and pick the *second* worker (W8), so a binding that
      // ignored the selection would still be sitting on W7 / All Workers.
      const select = fixture.debugElement.query(By.css("nz-select")).nativeElement as HTMLElement;
      select.click();
      // The option list renders into a CDK overlay, i.e. a view that hangs off
      // ApplicationRef rather than off this fixture. Render the open state first, then
      // flush, so the options are there whether or not Angular's automatic tick has
      // run — it stops firing for the rest of a Vitest worker once a spec that mounts
      // Monaco has gone before this one, and the suite runs with isolate: false.
      fixture.detectChanges();
      flush();
      fixture.detectChanges();

      const options = Array.from(document.querySelectorAll("nz-option-item"));
      expect(options.map(option => option.textContent?.trim())).toEqual(["W7", "W8", "All Workers"]);
      (options[1] as HTMLElement).click();
      tick(500);
      fixture.detectChanges();

      // the closed select shows the picked worker
      expect(fixture.debugElement.query(By.css("nz-select-item")).nativeElement.textContent.trim()).toBe("W8");

      const input = fixture.debugElement.query(By.css("input[nz-input]")).nativeElement as HTMLInputElement;
      input.value = "where";
      input.dispatchEvent(new Event("input", { bubbles: true }));
      fixture.detectChanges();
      input.dispatchEvent(new KeyboardEvent("keyup", { key: "Enter", bubbles: true }));

      // exactly one send, to w-8 only — not broadcast, and not to w-7
      expect(send).toHaveBeenCalledTimes(1);
      expect(send).toHaveBeenCalledWith("DebugCommandRequest", { operatorId: "op1", workerId: "w-8", cmd: "where" });
      flush();
    }));
  });
});

/**
 * The settings dropdown (the little gear above the console) hosts the
 * "Show Timestamp" / "Show Source" switches. Its menu is projected into a CDK
 * overlay that only attaches once the trigger is hovered, and the trigger
 * pipeline is wired in ngAfterViewInit behind an auditTime, so the fixture has
 * to be created *inside* fakeAsync for tick() to drive it. That is why this
 * lives in its own describe rather than reusing the fixture above.
 */
describe("ConsoleFrameComponent settings dropdown", () => {
  const message: ConsoleMessage = {
    workerId: "w-3",
    timestamp: { nanos: 0, seconds: 1 },
    msgType: { name: "PRINT" },
    source: "operator-a:main",
    title: "title",
    message: "body",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ConsoleFrameComponent, HttpClientTestingModule, NzDropDownModule],
      providers: [
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        {
          provide: ExecuteWorkflowService,
          useValue: {
            getExecutionStateStream: () => new Subject<StateEvent>().asObservable(),
            getWorkerIds: () => [],
            skipTuples: () => {},
            retryExecution: () => {},
          },
        },
        {
          provide: WorkflowConsoleService,
          useValue: {
            getConsoleMessageUpdateStream: () => new Subject<void>().asObservable(),
            getConsoleMessages: () => [],
          },
        },
        { provide: WorkflowWebsocketService, useValue: { send: () => {} } },
        { provide: NotificationService, useValue: { error: () => {} } },
        { provide: UdfDebugService, useValue: { doStep: () => {}, doContinue: () => {} } },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  it("toggles the timestamp and source tags independently from the settings menu", fakeAsync(() => {
    const fixture = TestBed.createComponent(ConsoleFrameComponent);
    fixture.componentInstance.consoleMessages = [message];
    fixture.detectChanges();

    const timestampTags = () => fixture.debugElement.queryAll(By.css(".timestamp-tag")).length;
    const sourceTags = () => fixture.debugElement.queryAll(By.css(".source-tag")).length;

    // both switches default to on, so both tags start out rendered
    expect(timestampTags()).toBe(1);
    expect(sourceTags()).toBe(1);

    // hovering the gear attaches the dropdown overlay
    fixture.debugElement
      .query(By.css("a[nz-dropdown]"))
      .nativeElement.dispatchEvent(new MouseEvent("mouseenter", { bubbles: true }));
    tick(300);
    fixture.detectChanges();

    const menuItems = Array.from(document.querySelectorAll("li[nz-menu-item]"));
    expect(menuItems.map(item => item.textContent?.trim())).toEqual(["Show Timestamp", "Show Source"]);
    const switches = Array.from(document.querySelectorAll("nz-switch button.ant-switch")) as HTMLElement[];
    expect(switches.length).toBe(2);
    expect(switches[0].classList.contains("ant-switch-checked")).toBe(true);

    // flip only "Show Timestamp": the timestamp tag goes away and the source tag stays.
    // Asserting the two independently is what distinguishes the bindings — turning
    // both off at once would look the same if the two switches were swapped.
    switches[0].click();
    tick(300);
    fixture.detectChanges();

    expect(switches[0].classList.contains("ant-switch-checked")).toBe(false);
    expect(timestampTags()).toBe(0);
    expect(sourceTags()).toBe(1);

    // now flip "Show Source" as well
    switches[1].click();
    tick(300);
    fixture.detectChanges();

    expect(switches[1].classList.contains("ant-switch-checked")).toBe(false);
    expect(sourceTags()).toBe(0);
    expect(timestampTags()).toBe(0);

    fixture.destroy();
    flush();
  }));
});
