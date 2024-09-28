import { Injectable } from "@angular/core";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { EMPTY, expand, finalize, merge } from "rxjs";
import { ResultExportResponse } from "../../types/workflow-websocket.interface";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState, isNotInExecution } from "../../types/execute-workflow.interface";
import { filter } from "rxjs/operators";
import { WorkflowResultService } from "../workflow-result/workflow-result.service";
import { FileSaverService } from "../../../dashboard/service/user/file/file-saver.service";

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
    private workflowResultService: WorkflowResultService,
    private fileSaverService: FileSaverService
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
   * export the operator result as a file.
   * If its a paginated result, export as CSV.
   * If its a visualization result, export as .html
   */
  exportOperatorAsFile(operatorId: string): void {
    const resultService = this.workflowResultService.getResultService(operatorId);
    const paginatedResultService = this.workflowResultService.getPaginatedResultService(operatorId);

    if (paginatedResultService) {
      const results: any[] = [];
      let currentPage = 1;
      const pageSize = 10;

      paginatedResultService
        .selectPage(currentPage, pageSize)
        .pipe(
          expand(pageData => {
            // Process the current page data
            results.push(...pageData.table);

            // Check if there are more pages
            if (pageData.table.length === pageSize) {
              currentPage++;
              // Fetch the next page
              return paginatedResultService.selectPage(currentPage, pageSize);
            } else {
              // No more pages; complete the observable
              return EMPTY;
            }
          }),
          finalize(() => {
            // Proceed to convert results to CSV and download
            this.downloadResultsAsCSV(results, operatorId);
          })
        )
        .subscribe();
    }

    if (resultService) {
      const snapshot = resultService.getCurrentResultSnapshot();
      const filesString: string[] = [];

      snapshot?.forEach(s => filesString.push(Object(s)["html-content"]));

      // Convert filesString into Blob objects and download them as HTML files
      filesString.forEach((fileContent, index) => {
        const blob = new Blob([fileContent], { type: "text/html;charset=utf-8" });
        const filename = `result_${operatorId}_${index + 1}.html`;
        this.fileSaverService.saveAs(blob, filename);
      });
    }
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

  /**
   * Convert the results array into CSV format and trigger download.
   */
  private downloadResultsAsCSV(results: any[], operatorId: string): void {
    // Extract all unique keys from the results
    const allKeys = new Set<string>();
    results.forEach(record => {
      Object.keys(record).forEach(key => allKeys.add(key));
    });
    const headers = Array.from(allKeys);

    // Build CSV content
    let csvContent = "";
    // Add headers
    csvContent += headers.join(",") + "\n";

    // Add data rows
    results.forEach(record => {
      const row = headers.map(header => {
        let cell = record[header];
        if (cell === null || cell === undefined) {
          cell = "";
        } else if (typeof cell === "object") {
          // If the cell is an object (e.g., Date), convert it to string
          cell = JSON.stringify(cell);
        }
        // Escape double quotes and commas in cell
        if (typeof cell === "string" && (cell.includes(",") || cell.includes("\""))) {
          cell = "\"" + cell.replace(/"/g, "\"\"") + "\"";
        }
        return cell;
      });
      csvContent += row.join(",") + "\n";
    });

    // Create Blob and download
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8" });
    const filename = `result_${operatorId}.csv`;
    this.fileSaverService.saveAs(blob, filename);
  }
}
