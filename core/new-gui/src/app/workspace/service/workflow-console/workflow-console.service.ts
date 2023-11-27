import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { ConsoleMessage, ConsoleUpdateEvent } from "../../types/workflow-common.interface";
import { Subject } from "rxjs";
import { Observable } from "rxjs";
import { RingBuffer } from "ring-buffer-ts";
import { ExecutionState } from "../../types/execute-workflow.interface";

export const CONSOLE_BUFFER_SIZE = 100;

@Injectable({
  providedIn: "root",
})
export class WorkflowConsoleService {
  private consoleMessages: Map<string, RingBuffer<ConsoleMessage>> = new Map();
  private consoleMessagesUpdateStream = new Subject<void>();

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.registerAutoClearConsoleMessages();
    this.registerPythonConsoleUpdateEventHandler();
  }

  registerPythonConsoleUpdateEventHandler() {
    this.workflowWebsocketService
      .subscribeToEvent("ConsoleUpdateEvent")
      .subscribe((pythonConsoleUpdateEvent: ConsoleUpdateEvent) => {
        const operatorId = pythonConsoleUpdateEvent.operatorId;
        const messages = this.consoleMessages.get(operatorId) || new RingBuffer<ConsoleMessage>(CONSOLE_BUFFER_SIZE);
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
