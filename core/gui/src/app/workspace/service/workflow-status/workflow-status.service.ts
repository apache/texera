import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { environment } from "../../../../environments/environment";
import { OperatorState, OperatorStatistics } from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";

@Injectable({
  providedIn: "root",
})
export class WorkflowStatusService {
  // status is responsible for passing websocket responses to other components
  private statusSubject = new Subject<Record<string, OperatorStatistics>>();
  private currentStatus: Record<string, OperatorStatistics> = {};

  constructor(private workflowWebsocketService: WorkflowWebsocketService) {
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

  public resetStatus(): void {
    const initStatus: Record<string, OperatorStatistics> = Object.keys(this.currentStatus).reduce(
      (accumulator, operatorId) => {
        accumulator[operatorId] = {
          operatorState: OperatorState.Uninitialized,
          aggregatedInputRowCount: 0,
          aggregatedOutputRowCount: 0,
        };
        return accumulator;
      },
      {} as Record<string, OperatorStatistics>
    );
    this.statusSubject.next(initStatus);
  }
}
