import { Injectable } from '@angular/core';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { PythonPrintTriggerInfo } from '../../types/workflow-common.interface';

@Injectable({
  providedIn: 'root'
})
export class WorkflowConsoleService {
  consoleMessages: Map<string, string[]> = new Map();

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.registerAutoClearConsoleMessages();
    this.registerPythonPrintEventHandler();
  }

  private registerPythonPrintEventHandler() {
    this.workflowWebsocketService.subscribeToEvent('PythonPrintTriggeredEvent')
      .subscribe((a: PythonPrintTriggerInfo) => {
        const operatorID = a.operatorID;
        let messages = this.consoleMessages.get(operatorID);
        if (!messages) {
          messages = [];
        }
        messages.push(a.message);
        this.consoleMessages.set(operatorID, messages);
      });
  }

  private registerAutoClearConsoleMessages() {
    this.workflowWebsocketService.subscribeToEvent('WorkflowStartedEvent').subscribe(_ => {
      this.consoleMessages.clear();
    });
  }
}
