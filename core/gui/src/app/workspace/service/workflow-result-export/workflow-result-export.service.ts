import * as Papa from "papaparse";
import { Injectable } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { BehaviorSubject, EMPTY, expand, finalize, merge, Observable, of } from "rxjs";
import { PaginatedResultEvent, ResultExportResponse } from "../../types/workflow-websocket.interface";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState, isNotInExecution } from "../../types/execute-workflow.interface";
import { filter } from "rxjs/operators";
import { OperatorResultService, WorkflowResultService } from "../workflow-result/workflow-result.service";
import { DownloadService } from "../../../dashboard/service/user/download/download.service";
import { HttpResponse } from "@angular/common/http";
import { ExportWorkflowJsonResponse } from "../../../dashboard/service/user/download/download.service";

@Injectable({
  providedIn: "root",
})
export class WorkflowResultExportService {
  hasResultToExportOnHighlightedOperators: boolean = false;
  exportExecutionResultEnabled: boolean = environment.exportExecutionResultEnabled;
  hasResultToExportOnAllOperators = new BehaviorSubject<boolean>(false);
  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private downloadService: DownloadService
  ) {
    this.registerResultToExportUpdateHandler();
  }

  registerResultToExportUpdateHandler() {
    merge(
      this.executeWorkflowService
        .getExecutionStateStream()
        .pipe(filter(({ previous, current }) => current.state === ExecutionState.Completed)),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream()
    ).subscribe(() => {
      // check if there are any results to export on highlighted operators (either paginated or snapshot)
      this.hasResultToExportOnHighlightedOperators =
        isNotInExecution(this.executeWorkflowService.getExecutionState().state) &&
        this.workflowActionService
          .getJointGraphWrapper()
          .getCurrentHighlightedOperatorIDs()
          .filter(
            operatorId =>
              this.workflowResultService.hasAnyResult(operatorId) ||
              this.workflowResultService.getResultService(operatorId)?.getCurrentResultSnapshot() !== undefined
          ).length > 0;

      // check if there are any results to export on all operators (either paginated or snapshot)
      let staticHasResultToExportOnAllOperators =
        isNotInExecution(this.executeWorkflowService.getExecutionState().state) &&
        this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .map(operator => operator.operatorID)
          .filter(
            operatorId =>
              this.workflowResultService.hasAnyResult(operatorId) ||
              this.workflowResultService.getResultService(operatorId)?.getCurrentResultSnapshot() !== undefined
          ).length > 0;

      // Notify subscribers of changes
      this.hasResultToExportOnAllOperators.next(staticHasResultToExportOnAllOperators);
    });
  }

  /**
   * export the workflow execution result according the export type
   */
  exportWorkflowExecutionResult(
    exportType: string,
    workflowName: string,
    datasetIds: number[],
    rowIndex: number,
    columnIndex: number,
    filename: string,
    exportAll: boolean = false, // if the user click export button on the top bar (a.k.a menu),
    // we should export all operators, otherwise, only highlighted ones
    // which means export button is selected from context-menu
    destination: "dataset" | "local" = "dataset" // default to dataset
  ): void {
    if (!environment.exportExecutionResultEnabled) {
      return;
    }

    const workflowId = this.workflowActionService.getWorkflow().wid;
    if (!workflowId) {
      return;
    }

    // gather operator IDs
    const operatorIds = exportAll
      ? this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .map(operator => operator.operatorID)
      : [...this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()];

    if (operatorIds.length === 0) {
      return;
    }

    // show loading
    this.notificationService.loading("Exporting...");

    // Make request
    this.downloadService
      .exportWorkflowResult(
        exportType,
        workflowId,
        workflowName,
        operatorIds,
        [...datasetIds],
        rowIndex,
        columnIndex,
        filename,
        destination
      )
      .subscribe({
        next: response => {
          if (destination === "local") {
            // "local" => response is a blob
            // We can parse the file name from header or use fallback
            this.downloadService.saveBlobFile(response, filename);
            this.notificationService.info("Files downloaded successfully");
          } else {
            // "dataset" => response is JSON
            // The server should return a JSON with {status, message}
            const jsonResponse = response as HttpResponse<ExportWorkflowJsonResponse>;
            const responseBody = jsonResponse.body;
            if (responseBody && responseBody.status === "success") {
              this.notificationService.success("Result exported successfully");
            } else {
              this.notificationService.error(responseBody?.message || "An error occurred during export");
            }
          }
        },
        error: (err: unknown) => {
          const errorMessage = (err as any)?.error?.message || (err as any)?.error || err;
          this.notificationService.error(`An error happened in exporting operator results: ${errorMessage}`);
        },
      });
  }

  /**
   * Reset flags if the user leave workspace
   */
  public resetFlags(): void {
    this.hasResultToExportOnHighlightedOperators = false;
    this.hasResultToExportOnAllOperators = new BehaviorSubject<boolean>(false);
  }

  getExportOnAllOperatorsStatusStream(): Observable<boolean> {
    return this.hasResultToExportOnAllOperators.asObservable();
  }
}
