import { Injectable } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { merge } from "rxjs";
import { ResultExportResponse } from "../../types/workflow-websocket.interface";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState, isNotInExecution } from "../../types/execute-workflow.interface";
import { filter } from "rxjs/operators";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";

@Injectable({
  providedIn: "root",
})
export class WorkflowResultExportService {
  hasResultToExport: boolean = false;
  exportExecutionResultEnabled: boolean = environment.exportExecutionResultEnabled;

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService
  ) {
    this.registerResultExportResponseHandler();
    this.registerResultToExportUpdateHandler();
  }

  registerResultExportResponseHandler() {
    this.workflowWebsocketService
      .subscribeToEvent("ResultExportResponse")
      .subscribe((response: ResultExportResponse) => {
        if (response.status === "success") {
          this.notificationService.success(response.message);
        } else {
          this.notificationService.error(response.message);
        }
      });
  }

  registerResultToExportUpdateHandler() {
    merge(
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(filter(({ previous, current }) => current.state === ExecutionState.Completed)),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream()
    ).subscribe(() => {
      this.hasResultToExport =
        isNotInExecution(this.executeWorkflowService.getExecutionState().state) &&
        this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs()
          .filter(operatorId => this.workflowResultService.hasAnyResult(operatorId)).length > 0;
    });
  }

  /**
   * export the workflow execution result according the export type
   */
  exportWorkflowExecutionResult(
    exportType: string,
    workflowName: string,
    datasetIds: ReadonlyArray<number> = [],
    rowIndex: number,
    columnIndex: number,
    filename: string
  ): void {
    if (!environment.exportExecutionResultEnabled || !this.hasResultToExport) {
      return;
    }

    const workflowId = this.workflowActionService.getWorkflow().wid;
    if (!workflowId) {
      return;
    }

    this.notificationService.loading("exporting...");
    this.workflowActionService
      .getJointGraphWrapper()
      .getCurrentHighlightedOperatorIDs()
      .forEach(operatorId => {
        if (!this.workflowResultService.hasAnyResult(operatorId)) {
          return;
        }
        const operator = this.workflowActionService.getTexeraGraph().getOperator(operatorId);
        const operatorName = operator.customDisplayName ?? operator.operatorType;
        this.workflowWebsocketService.send("ResultExportRequest", {
          exportType,
          workflowId,
          workflowName,
          operatorId,
          operatorName,
          datasetIds,
          rowIndex,
          columnIndex,
          filename,
        });
      });
  }
}
