import { Injectable } from '@angular/core';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { PythonPrintTriggerInfo } from '../../types/workflow-common.interface';
import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';

@Injectable({
  providedIn: 'root'
})
export class WorkflowConsoleService {

  private consoleMessages: Map<string, ReadonlyArray<string>> = new Map();
  private consoleMessagesUpdateStream = new Subject<void>();

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
    this.registerAutoClearConsoleMessages();
    this.registerPythonPrintEventHandler();
  }

  registerPythonPrintEventHandler() {
    this.workflowWebsocketService.subscribeToEvent('PythonPrintTriggeredEvent')
      .subscribe((pythonPrintTriggerInfo: PythonPrintTriggerInfo) => {
        const operatorID = pythonPrintTriggerInfo.operatorID;
        let messages = this.consoleMessages.get(operatorID) || [];
        messages = messages.concat(pythonPrintTriggerInfo.message.split('\n'));
        this.consoleMessages.set(operatorID, messages);
        this.consoleMessagesUpdateStream.next();
      });
  }

  registerAutoClearConsoleMessages() {
    this.workflowWebsocketService.subscribeToEvent('WorkflowStartedEvent').subscribe(_ => {
      this.consoleMessages.clear();
    });
  }

  getConsoleMessages(operatorID: string): ReadonlyArray<string> | undefined {
    return this.consoleMessages.get(operatorID);
  }

  getConsoleMessageUpdateStream(): Observable<void> {
    return this.consoleMessagesUpdateStream.asObservable();
  }

}
