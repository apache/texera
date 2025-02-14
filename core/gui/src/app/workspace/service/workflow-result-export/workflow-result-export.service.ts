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
import { OperatorPaginationResultService } from "../workflow-result/workflow-result.service";
import { DownloadService } from "../../../dashboard/service/user/download/download.service";

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
   * Export the operator results as files.
   * If multiple operatorIds are provided, results are zipped into a single file.
   */
  exportOperatorsResultToLocal(exportAll: boolean = true): void {
    let operatorIds: string[];
    if (!exportAll)
      operatorIds = [...this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()];
    else
      operatorIds = this.workflowActionService
        .getTexeraGraph()
        .getAllOperators()
        .map(operator => operator.operatorID);

    const resultObservables: Observable<any>[] = [];

    operatorIds.forEach(operatorId => {
      const resultService = this.workflowResultService.getResultService(operatorId);
      const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);

      if (paginatedResultService) {
        const observable = this.fetchAllPaginatedResultsAsCSV(paginatedResultService, operatorId);
        resultObservables.push(observable);
      } else if (resultService) {
        const observable = this.fetchVisualizationResultsAsHTML(resultService, operatorId);
        resultObservables.push(observable);
      }
    });

    if (resultObservables.length === 0) {
      return;
    }

    this.downloadService
      .downloadOperatorsResult(resultObservables, this.workflowActionService.getWorkflow())
      .subscribe({
        error: (error: unknown) => {
          console.error("Error exporting operator results:", error);
        },
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
    let operatorIds: string[] = [];
    if (!exportAll) {
      operatorIds = [...this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()];
    } else {
      operatorIds = this.workflowActionService
        .getTexeraGraph()
        .getAllOperators()
        .map(operator => operator.operatorID);
    }

    if (operatorIds.length === 0) {
      console.log("No operators selected to export");
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
            this.notificationService.info("File downloaded successfully");
          } else {
            // "dataset" => response is JSON
            // The server should return a JSON with {status, message}
            const responseBody = response.body;
            if (responseBody && responseBody.status === "success") {
              this.notificationService.success(responseBody.message);
            } else {
              this.notificationService.error(responseBody?.message || "An error occurred during export");
            }
          }
        },
        error: (err: unknown) => {
          const errorMessage = (err as any)?.error?.error || (err as any)?.error || err;
          this.notificationService.error(`An error happened in exporting operator results: ${errorMessage}`);
        },
      });
  }

  /**
   * Helper method to fetch all paginated results and convert them to a CSV Blob.
   */
  private fetchAllPaginatedResultsAsCSV(
    paginatedResultService: OperatorPaginationResultService,
    operatorId: string
  ): Observable<{ filename: string; blob: Blob }[]> {
    return new Observable(observer => {
      const results: any[] = [];
      let currentPage = 1;
      const pageSize = 10;

      paginatedResultService
        .selectPage(currentPage, pageSize)
        .pipe(
          expand((pageData: PaginatedResultEvent) => {
            results.push(...pageData.table);
            if (pageData.table.length === pageSize) {
              currentPage++;
              return paginatedResultService.selectPage(currentPage, pageSize);
            } else {
              return EMPTY;
            }
          }),
          finalize(() => {
            const { filename, blob } = this.createCSVBlob(results, operatorId);
            observer.next([{ filename, blob }]);
            observer.complete();
          })
        )
        .subscribe();
    });
  }

  /**
   * Helper method to fetch visualization results and convert them to HTML Blobs.
   */
  private fetchVisualizationResultsAsHTML(
    resultService: OperatorResultService,
    operatorId: string
  ): Observable<{ filename: string; blob: Blob }[]> {
    return new Observable(observer => {
      const snapshot = resultService.getCurrentResultSnapshot();
      const files: { filename: string; blob: Blob }[] = [];

      snapshot?.forEach((s: any, index: number) => {
        const fileContent = Object(s)["html-content"];
        const blob = new Blob([fileContent], { type: "text/html;charset=utf-8" });
        const filename = `result_${operatorId}_${index + 1}.html`;
        files.push({ filename, blob });
      });

      observer.next(files);
      observer.complete();
    });
  }

  /**
   * Convert the results array into CSV format and create a Blob.
   */
  private createCSVBlob(results: any[], operatorId: string): { filename: string; blob: Blob } {
    const csv = Papa.unparse(results); // Convert array of objects to CSV
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
    const filename = `result_${operatorId}.csv`;
    return { filename, blob };
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
