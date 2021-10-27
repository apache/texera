import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { environment } from "../../../../environments/environment";
import { OperatorStatistics } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";

@Injectable({
  providedIn: "root",
})
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private statusSubject = new Subject<Record<string, OperatorStatistics>>();
  private currentStatus: Record<string, OperatorStatistics> = {};

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {
    if (!environment.executionStatusEnabled) {
      return;
    }
    this.getStatusUpdateStream().subscribe(event => (this.currentStatus = event));

    this.workflowWebsocketService.websocketEvent().subscribe(event => {
      if (event.type !== "OperatorStatisticsUpdateEvent") {
        return;
      }
      this.statusSubject.next(event.operatorStatistics);
    });
  }

  public getStatusUpdateStream(): Observable<Record<string, OperatorStatistics>> {
    return this.statusSubject.asObservable();
  }

  public getCurrentStatus(): Record<string, OperatorStatistics> {
    return this.currentStatus;
  }
}
