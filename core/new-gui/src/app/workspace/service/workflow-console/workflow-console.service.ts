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
      .subscribe((pythonPrintTriggerInfo: PythonPrintTriggerInfo) => {
        const operatorID = pythonPrintTriggerInfo.operatorID;
        let messages = this.consoleMessages.get(operatorID) || [];
        messages = messages.concat(pythonPrintTriggerInfo.message.split('\n'));
        this.consoleMessages.set(operatorID, messages);
      });
  }

  private registerAutoClearConsoleMessages() {
    this.workflowWebsocketService.subscribeToEvent('WorkflowStartedEvent').subscribe(_ => {
      this.consoleMessages.clear();
    });
  }
}
