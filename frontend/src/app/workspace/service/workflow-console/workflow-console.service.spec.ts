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

import { TestBed } from "@angular/core/testing";
import { Subject } from "rxjs";
import { WorkflowConsoleService } from "./workflow-console.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { ConsoleMessage, ConsoleUpdateEvent } from "../../types/workflow-common.interface";

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

describe("WorkflowConsoleService", () => {
  let service: WorkflowConsoleService;
  // The service subscribes to these websocket events in its constructor, so the
  // doubles must be in place before it is injected.
  let consoleUpdateEvent$: Subject<ConsoleUpdateEvent>;
  let workflowStateEvent$: Subject<{ state: ExecutionState }>;

  beforeEach(() => {
    consoleUpdateEvent$ = new Subject<ConsoleUpdateEvent>();
    workflowStateEvent$ = new Subject<{ state: ExecutionState }>();
    const subscribeToEvent = vi.fn((eventType: string) => {
      if (eventType === "ConsoleUpdateEvent") return consoleUpdateEvent$.asObservable();
      if (eventType === "WorkflowStateEvent") return workflowStateEvent$.asObservable();
      return new Subject().asObservable();
    });

    TestBed.configureTestingModule({
      providers: [
        WorkflowConsoleService,
        { provide: WorkflowWebsocketService, useValue: { subscribeToEvent } },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(WorkflowConsoleService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("buffers messages from a ConsoleUpdateEvent and exposes them per operator", () => {
    let updates = 0;
    service.getConsoleMessageUpdateStream().subscribe(() => updates++);
    const messages = [consoleMessage("PRINT"), consoleMessage("ERROR")];

    consoleUpdateEvent$.next({ operatorId: "op1", messages });

    expect(service.hasConsoleMessages("op1")).toBe(true);
    expect(service.getConsoleMessages("op1")).toEqual(messages);
    expect(service.getConsoleMessages("missing")).toBeUndefined();
    expect(service.hasConsoleMessages("missing")).toBe(false);
    expect(updates).toBe(1);
  });

  it("clearConsoleMessages() removes all messages and notifies subscribers", () => {
    consoleUpdateEvent$.next({ operatorId: "op1", messages: [consoleMessage("PRINT")] });
    expect(service.hasConsoleMessages("op1")).toBe(true);

    let notified = false;
    service.getConsoleMessageUpdateStream().subscribe(() => (notified = true));

    service.clearConsoleMessages();

    expect(service.hasConsoleMessages("op1")).toBe(false);
    expect(notified).toBe(true);
  });

  it("clears the console store when the workflow re-initializes (WorkflowStateEvent)", () => {
    consoleUpdateEvent$.next({ operatorId: "op1", messages: [consoleMessage("PRINT")] });
    expect(service.hasConsoleMessages("op1")).toBe(true);

    workflowStateEvent$.next({ state: ExecutionState.Initializing });

    expect(service.hasConsoleMessages("op1")).toBe(false);
  });

  it("leaves the store untouched for a non-initializing workflow state event", () => {
    consoleUpdateEvent$.next({ operatorId: "op1", messages: [consoleMessage("PRINT")] });

    workflowStateEvent$.next({ state: ExecutionState.Running });

    expect(service.hasConsoleMessages("op1")).toBe(true);
  });
});
