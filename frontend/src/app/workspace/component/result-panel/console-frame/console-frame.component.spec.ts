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
});
