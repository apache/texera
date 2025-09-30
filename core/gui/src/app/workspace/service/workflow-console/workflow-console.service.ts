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

import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { ConsoleMessage, ConsoleUpdateEvent } from "../../types/workflow-common.interface";
import { Subject } from "rxjs";
import { Observable } from "rxjs";
import { RingBuffer } from "ring-buffer-ts";
import { ExecutionState } from "../../types/execute-workflow.interface";
import { GuiConfigService } from "../../../common/service/gui-config.service";

@Injectable({
  providedIn: "root",
})
export class WorkflowConsoleService {
  private consoleMessages: Map<string, RingBuffer<ConsoleMessage>> = new Map();
  private consoleMessagesUpdateStream = new Subject<void>();

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private config: GuiConfigService
  ) {
    this.registerAutoClearConsoleMessages();
    this.registerPythonConsoleUpdateEventHandler();
  }

  registerPythonConsoleUpdateEventHandler() {
    this.workflowWebsocketService
      .subscribeToEvent("ConsoleUpdateEvent")
      .subscribe((pythonConsoleUpdateEvent: ConsoleUpdateEvent) => {
        const operatorId = pythonConsoleUpdateEvent.operatorId;
        const messages =
          this.consoleMessages.get(operatorId) ||
          new RingBuffer<ConsoleMessage>(this.config.env.operatorConsoleMessageBufferSize);
        messages.add(...pythonConsoleUpdateEvent.messages);
        this.consoleMessages.set(operatorId, messages);
        this.consoleMessagesUpdateStream.next();
      });
  }

  registerAutoClearConsoleMessages() {
    this.workflowWebsocketService.subscribeToEvent("WorkflowStateEvent").subscribe(event => {
      if (event.state === ExecutionState.Initializing) {
        this.consoleMessages.clear();
      }
    });
  }

  getConsoleMessages(operatorId: string): ReadonlyArray<ConsoleMessage> | undefined {
    return this.consoleMessages.get(operatorId)?.toArray();
  }

  hasConsoleMessages(operatorId: string): boolean {
    return this.consoleMessages.has(operatorId);
  }

  getConsoleMessageUpdateStream(): Observable<void> {
    return this.consoleMessagesUpdateStream.asObservable();
  }
}
