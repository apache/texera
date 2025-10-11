/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import * as Papa from "papaparse";
import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { BehaviorSubject, EMPTY, expand, finalize, merge, Observable, of } from "rxjs";
import { PaginatedResultEvent, ResultExportResponse } from "../../types/workflow-websocket.interface";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { ExecutionState, isNotInExecution } from "../../types/execute-workflow.interface";
import { catchError, filter, map, take, tap } from "rxjs/operators";
import { OperatorResultService, WorkflowResultService } from "../workflow-result/workflow-result.service";
import { DownloadService } from "../../../dashboard/service/user/download/download.service";
import { HttpResponse } from "@angular/common/http";
import { ExportWorkflowJsonResponse } from "../../../dashboard/service/user/download/download.service";
import { DashboardWorkflowComputingUnit } from "../../types/workflow-computing-unit";
import { GuiConfigService } from "../../../common/service/gui-config.service";

/**
 * Result of workflow result downloadability analysis.
 * Contains information about which operators are restricted from exporting
 * due to non-downloadable dataset dependencies.
 */
export class WorkflowResultDownloadability {
  /**
   * Map of operator IDs to sets of blocking dataset labels.
   * Key: Operator ID
   * Value: Set of human-readable dataset labels (e.g., "dataset_name (owner@email.com)")
   *        that are blocking this operator from being exported
   *
   * An operator appears in this map if it directly uses or depends on (through data flow)
   * one or more datasets that the current user is not allowed to download.
   */
  restrictedOperatorMap: Map<string, Set<string>>;

  constructor(restrictedOperatorMap: Map<string, Set<string>>) {
    this.restrictedOperatorMap = restrictedOperatorMap;
  }

  /**
   * Filters operator IDs to return only those that are not restricted by dataset access controls.
   *
   * @param operatorIds Array of operator IDs to filter
   * @returns Array of operator IDs that can be exported
   */
  getExportableOperatorIds(operatorIds: readonly string[]): string[] {
    return operatorIds.filter(operatorId => !this.restrictedOperatorMap.has(operatorId));
  }

  /**
   * Filters operator IDs to return only those that are restricted by dataset access controls.
   *
   * @param operatorIds Array of operator IDs to filter
   * @returns Array of operator IDs that are blocked from export
   */
  getBlockedOperatorIds(operatorIds: readonly string[]): string[] {
    return operatorIds.filter(operatorId => this.restrictedOperatorMap.has(operatorId));
  }

  /**
   * Gets the list of dataset labels that are blocking export for the given operators.
   * Used to display user-friendly error messages about which datasets are causing restrictions.
   *
   * @param operatorIds Array of operator IDs to check
   * @returns Array of dataset labels (e.g., "Dataset1 (user@example.com)")
   */
  getBlockingDatasets(operatorIds: readonly string[]): string[] {
    const labels = new Set<string>();
    operatorIds.forEach(operatorId => {
      const datasets = this.restrictedOperatorMap.get(operatorId);
      datasets?.forEach(label => labels.add(label));
    });
    return Array.from(labels);
  }
}

@Injectable({
  providedIn: "root",
})
export class WorkflowResultExportService {
  hasResultToExportOnHighlightedOperators: boolean = false;
  hasResultToExportOnAllOperators = new BehaviorSubject<boolean>(false);
  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowResultService: WorkflowResultService,
    private downloadService: DownloadService,
    private config: GuiConfigService
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
      this.updateExportAvailabilityFlags();
    });
  }

  /**
   * Computes restriction analysis by calling the backend API.
   *
   * The backend analyzes the workflow to identify operators that are restricted from export
   * due to non-downloadable dataset dependencies. The restriction propagates through the
   * workflow graph via data flow.
   *
   * @returns Observable that emits the restriction analysis result
   */
  public computeRestrictionAnalysis(): Observable<WorkflowResultDownloadability> {
    const workflowId = this.workflowActionService.getWorkflow().wid;
    if (!workflowId) {
      return of(new WorkflowResultDownloadability(new Map<string, Set<string>>()));
    }

    return this.downloadService.getWorkflowResultDownloadability(workflowId).pipe(
      map(backendResponse => {
        // Convert backend format to Map<operatorId, Set<datasetLabel>>
        const restrictedOperatorMap = new Map<string, Set<string>>();
        Object.entries(backendResponse).forEach(([operatorId, datasetLabels]) => {
          restrictedOperatorMap.set(operatorId, new Set(datasetLabels));
        });
        return new WorkflowResultDownloadability(restrictedOperatorMap);
      }),
      catchError(() => {
        return of(new WorkflowResultDownloadability(new Map<string, Set<string>>()));
      })
    );
  }

  /**
   * Updates UI flags that control export button visibility and availability.
   *
   * Checks execution state and result availability to determine:
   * - hasResultToExportOnHighlightedOperators: for context menu export button
   * - hasResultToExportOnAllOperators: for top menu export button
   *
   * Export is only available when execution is idle and operators have results.
   */
  private updateExportAvailabilityFlags(): void {
    const executionIdle = isNotInExecution(this.executeWorkflowService.getExecutionState().state);

    const highlightedOperators = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();

    const highlightedHasResult = highlightedOperators.some(
      operatorId =>
        this.workflowResultService.hasAnyResult(operatorId) ||
        this.workflowResultService.getResultService(operatorId)?.getCurrentResultSnapshot() !== undefined
    );

    this.hasResultToExportOnHighlightedOperators = executionIdle && highlightedHasResult;

    const allOperatorIds = this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .map(operator => operator.operatorID);

    const hasAnyResult =
      executionIdle &&
      allOperatorIds.some(
        operatorId =>
          this.workflowResultService.hasAnyResult(operatorId) ||
          this.workflowResultService.getResultService(operatorId)?.getCurrentResultSnapshot() !== undefined
      );

    this.hasResultToExportOnAllOperators.next(hasAnyResult);
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
    destination: "dataset" | "local" = "dataset", // default to dataset
    unit: DashboardWorkflowComputingUnit | null // computing unit for cluster setting
  ): void {
    this.computeRestrictionAnalysis()
      .pipe(take(1))
      .subscribe(restrictionResult =>
        this.performExport(
          exportType,
          workflowName,
          datasetIds,
          rowIndex,
          columnIndex,
          filename,
          exportAll,
          destination,
          unit,
          restrictionResult
        )
      );
  }

  /**
   * Performs the actual export operation with restriction validation.
   *
   * This method handles the core export logic:
   * 1. Validates configuration and computing unit availability
   * 2. Determines operator scope (all vs highlighted)
   * 3. Applies restriction filtering with user feedback
   * 4. Makes the export API call
   * 5. Handles response and shows appropriate notifications
   *
   * Shows error messages if all operators are blocked, warning messages if some are blocked.
   *
   * @param downloadability Downloadability analysis result containing restriction information
   */
  private performExport(
    exportType: string,
    workflowName: string,
    datasetIds: number[],
    rowIndex: number,
    columnIndex: number,
    filename: string,
    exportAll: boolean,
    destination: "dataset" | "local",
    unit: DashboardWorkflowComputingUnit | null,
    downloadability: WorkflowResultDownloadability
  ): void {
    // Validates configuration and computing unit availability
    if (!this.config.env.exportExecutionResultEnabled) {
      return;
    }
    if (unit === null) {
      this.notificationService.error("Cannot export result: computing unit is not available");
      return;
    }

    const workflowId = this.workflowActionService.getWorkflow().wid;
    if (!workflowId) {
      this.notificationService.error("Cannot export result: workflow ID is not available");
      return;
    }

    // Determines operator scope
    const operatorIds = exportAll
      ? this.workflowActionService
          .getTexeraGraph()
          .getAllOperators()
          .map(operator => operator.operatorID)
      : [...this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs()];

    if (operatorIds.length === 0) {
      return;
    }

    // Applies restriction filtering with user feedback
    const exportableOperatorIds = downloadability.getExportableOperatorIds(operatorIds);

    if (exportableOperatorIds.length === 0) {
      const datasets = downloadability.getBlockingDatasets(operatorIds);
      const suffix = datasets.length > 0 ? `: ${datasets.join(", ")}` : "";
      this.notificationService.error(
        `Cannot export result: selection depends on dataset(s) that are not downloadable${suffix}`
      );
      return;
    }

    if (exportableOperatorIds.length < operatorIds.length) {
      const datasets = downloadability.getBlockingDatasets(operatorIds);
      const suffix = datasets.length > 0 ? ` (${datasets.join(", ")})` : "";
      this.notificationService.warning(
        `Some operators were skipped because their results depend on dataset(s) that are not downloadable${suffix}`
      );
    }

    const operatorArray = exportableOperatorIds.map(operatorId => ({
      id: operatorId,
      outputType: this.workflowResultService.determineOutputExtension(operatorId, exportType),
    }));

    // show loading
    this.notificationService.loading("Exporting...");

    // Make request
    this.downloadService
      .exportWorkflowResult(
        exportType,
        workflowId,
        workflowName,
        operatorArray,
        [...datasetIds],
        rowIndex,
        columnIndex,
        filename,
        destination,
        unit
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
