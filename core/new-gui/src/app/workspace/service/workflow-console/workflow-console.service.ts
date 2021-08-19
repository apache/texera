import { Injectable } from '@angular/core';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { PythonPrintTriggerInfo } from '../../types/workflow-common.interface';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

@Injectable({
  providedIn: 'root'
})
export class WorkflowConsoleService {
  consoleMessages: Map<string, string[]> = new Map();

  consoleMessagesUpdateStream = new Subject<void>();

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.registerAutoClearConsoleMessages();
    this.registerPythonPrintEventHandler();
  }

  getConsoleMessageUpdateStream(): Observable<void> {
    return this.consoleMessagesUpdateStream.asObservable();
  }

  private registerPythonPrintEventHandler() {
    this.workflowWebsocketService.subscribeToEvent('PythonPrintTriggeredEvent')
      .subscribe((pythonPrintTriggerInfo: PythonPrintTriggerInfo) => {
        const operatorID = pythonPrintTriggerInfo.operatorID;
        let messages = this.consoleMessages.get(operatorID) || [];
        messages = messages.concat(pythonPrintTriggerInfo.message.split('\n'));
        this.consoleMessages.set(operatorID, messages);
        this.consoleMessagesUpdateStream.next();
      });
  }

  private registerAutoClearConsoleMessages() {
    this.workflowWebsocketService.subscribeToEvent('WorkflowStartedEvent').subscribe(_ => {
      this.consoleMessages.clear();
    });
  }
}
