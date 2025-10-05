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
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { parseFilePathToDatasetFile } from "../../../common/type/dataset-file";

export interface RestrictionAnalysisResult {
  restrictedOperatorMap: Map<string, Set<string>>;
  datasetDownloadableMap: Map<string, boolean>;
  datasetLabelMap: Map<string, string>;
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
    private datasetService: DatasetService,
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
   * Computes restriction analysis on-demand by fetching dataset metadata and analyzing workflow graph.
   *
   * Fetches all accessible datasets and their permissions, then:
   * - Builds datasetDownloadableMap: tracks which datasets are downloadable
   * - Builds datasetLabelMap: stores human-readable dataset labels
   * - Performs restriction analysis to identify operators blocked by dataset access controls
   *
   * A dataset is considered downloadable if either:
   * - The dataset's isDownloadable flag is true, OR
   * - The current user is the dataset owner
   *
   * @returns Observable that emits the restriction analysis result
   */
  public computeRestrictionAnalysis(): Observable<RestrictionAnalysisResult> {
    return this.datasetService.retrieveAccessibleDatasets().pipe(
      take(1),
      map(datasets => {
        const datasetDownloadableMap = new Map<string, boolean>();
        const datasetLabelMap = new Map<string, string>();

        datasets.forEach(dataset => {
          const key = this.buildDatasetKey(dataset.ownerEmail, dataset.dataset.name);
          const isDownloadable = dataset.dataset.isDownloadable || dataset.isOwner;
          datasetDownloadableMap.set(key, isDownloadable);
          datasetLabelMap.set(key, `${dataset.dataset.name} (${dataset.ownerEmail})`);
        });

        const restrictedOperatorMap = this.runRestrictionAnalysis(datasetDownloadableMap, datasetLabelMap);

        return { restrictedOperatorMap, datasetDownloadableMap, datasetLabelMap };
      }),
      catchError(() => {
        return of({
          restrictedOperatorMap: new Map<string, Set<string>>(),
          datasetDownloadableMap: new Map<string, boolean>(),
          datasetLabelMap: new Map<string, string>(),
        });
      })
    );
  }

  /**
   * Builds a normalized key for dataset lookup in caches.
   * Converts both email and dataset name to lowercase for case-insensitive matching.
   *
   * @param ownerEmail The dataset owner's email
   * @param datasetName The dataset name
   * @returns Normalized key in format "email::dataset"
   */
  private buildDatasetKey(ownerEmail: string, datasetName: string): string {
    return `${ownerEmail.toLowerCase()}::${datasetName.toLowerCase()}`;
  }

  /**
   * Extracts dataset information from an operator's fileName property.
   *
   * Parses file paths in the expected format and validates that the dataset
   * exists in the provided dataset maps.
   *
   * @param fileName The fileName property from operator properties
   * @param datasetDownloadableMap Map tracking which datasets are downloadable
   * @param datasetLabelMap Map storing human-readable dataset labels
   * @returns Object with dataset key and label, or null if invalid/not found
   */
  private extractDatasetInfo(
    fileName: unknown,
    datasetDownloadableMap: Map<string, boolean>,
    datasetLabelMap: Map<string, string>
  ): { key: string; label: string } | null {
    if (typeof fileName !== "string") {
      return null;
    }
    const trimmed = fileName.trim();
    if (!trimmed.startsWith("/")) {
      return null;
    }
    try {
      const { ownerEmail, datasetName } = parseFilePathToDatasetFile(trimmed);
      if (!ownerEmail || !datasetName) {
        return null;
      }
      const key = this.buildDatasetKey(ownerEmail, datasetName);
      if (!datasetDownloadableMap.has(key)) {
        return null;
      }
      const label = datasetLabelMap.get(key) ?? `${datasetName} (${ownerEmail})`;
      return { key, label };
    } catch {
      return null;
    }
  }

  /**
   * Performs client-side restriction analysis to mirror backend validation.
   *
   * This function:
   * 1. Identifies operators using non-downloadable datasets
   * 2. Builds a workflow dependency graph from operator links
   * 3. Uses BFS to propagate restrictions through the graph
   * 4. Returns a map of restricted operators
   *
   * The analysis considers only enabled operators and ignores disabled ones.
   * Restrictions flow downstream through operator dependencies.
   *
   * @param datasetDownloadableMap Map tracking which datasets are downloadable
   * @param datasetLabelMap Map storing human-readable dataset labels
   * @returns Map of operator IDs to sets of blocking dataset labels
   */
  private runRestrictionAnalysis(
    datasetDownloadableMap: Map<string, boolean>,
    datasetLabelMap: Map<string, string>
  ): Map<string, Set<string>> {
    const texeraGraph = this.workflowActionService.getTexeraGraph();
    const allOperators = texeraGraph.getAllOperators();
    const operatorById = new Map(allOperators.map(op => [op.operatorID, op] as const));
    const enabledOperators = allOperators.filter(operator => !operator.isDisabled);
    const datasetSources: Array<{ operatorId: string; label: string }> = [];

    // Identify source operators that use non-downloadable datasets
    enabledOperators.forEach(operator => {
      const datasetInfo = this.extractDatasetInfo(
        operator.operatorProperties?.fileName,
        datasetDownloadableMap,
        datasetLabelMap
      );
      if (!datasetInfo) {
        return;
      }
      const isDownloadable = datasetDownloadableMap.get(datasetInfo.key);
      if (isDownloadable === false) {
        datasetSources.push({ operatorId: operator.operatorID, label: datasetInfo.label });
      }
    });

    const restrictions = new Map<string, Set<string>>();

    if (datasetSources.length === 0) {
      return restrictions;
    }

    // Build Workflow Dependency Graph
    const adjacency = new Map<string, string[]>();
    texeraGraph.getAllLinks().forEach(link => {
      const sourceId = link.source.operatorID;
      const targetId = link.target.operatorID;
      const sourceOperator = operatorById.get(sourceId);
      const targetOperator = operatorById.get(targetId);
      if (!sourceOperator || !targetOperator) {
        return;
      }
      if (sourceOperator.isDisabled || targetOperator.isDisabled) {
        return;
      }
      const neighbors = adjacency.get(sourceId);
      if (neighbors) {
        neighbors.push(targetId);
      } else {
        adjacency.set(sourceId, [targetId]);
      }
    });

    // BFS
    const queue: Array<{ operatorId: string; datasets: Set<string> }> = [];
    datasetSources.forEach(source => {
      queue.push({ operatorId: source.operatorId, datasets: new Set([source.label]) });
    });

    while (queue.length > 0) {
      const current = queue.shift()!;
      const existing = restrictions.get(current.operatorId) ?? new Set<string>();
      let updated = false;
      current.datasets.forEach(label => {
        if (!existing.has(label)) {
          existing.add(label);
          updated = true;
        }
      });
      if (updated || !restrictions.has(current.operatorId)) {
        restrictions.set(current.operatorId, existing);
        const neighbors = adjacency.get(current.operatorId) ?? [];
        neighbors.forEach(nextOperatorId => {
          queue.push({ operatorId: nextOperatorId, datasets: new Set(existing) });
        });
      }
    }

    return restrictions;
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
   * Filters operator IDs to return only those that are not restricted by dataset access controls.
   *
   * @param operatorIds Array of operator IDs to filter
   * @param restrictedOperatorMap Map of restricted operators to blocking dataset labels
   * @returns Array of operator IDs that can be exported
   */
  public getExportableOperatorIds(
    operatorIds: readonly string[],
    restrictedOperatorMap: Map<string, Set<string>>
  ): string[] {
    return operatorIds.filter(operatorId => !restrictedOperatorMap.has(operatorId));
  }

  /**
   * Filters operator IDs to return only those that are restricted by dataset access controls.
   *
   * @param operatorIds Array of operator IDs to filter
   * @param restrictedOperatorMap Map of restricted operators to blocking dataset labels
   * @returns Array of operator IDs that are blocked from export
   */
  public getBlockedOperatorIds(
    operatorIds: readonly string[],
    restrictedOperatorMap: Map<string, Set<string>>
  ): string[] {
    return operatorIds.filter(operatorId => restrictedOperatorMap.has(operatorId));
  }

  /**
   * Gets the list of dataset labels that are blocking export for the given operators.
   * Used to display user-friendly error messages about which datasets are causing restrictions.
   *
   * @param operatorIds Array of operator IDs to check
   * @param restrictedOperatorMap Map of restricted operators to blocking dataset labels
   * @returns Array of dataset labels (e.g., "Dataset1 (user@example.com)")
   */
  public getBlockingDatasets(
    operatorIds: readonly string[],
    restrictedOperatorMap: Map<string, Set<string>>
  ): string[] {
    const labels = new Set<string>();
    operatorIds.forEach(operatorId => {
      const datasets = restrictedOperatorMap.get(operatorId);
      datasets?.forEach(label => labels.add(label));
    });
    return Array.from(labels);
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
          restrictionResult.restrictedOperatorMap
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
   * @param restrictedOperatorMap Map of restricted operators from restriction analysis
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
    restrictedOperatorMap: Map<string, Set<string>>
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
    const exportableOperatorIds = this.getExportableOperatorIds(operatorIds, restrictedOperatorMap);

    if (exportableOperatorIds.length === 0) {
      const datasets = this.getBlockingDatasets(operatorIds, restrictedOperatorMap);
      const suffix = datasets.length > 0 ? `: ${datasets.join(", ")}` : "";
      this.notificationService.error(
        `Cannot export result: selection depends on dataset(s) that are not downloadable${suffix}`
      );
      return;
    }

    if (exportableOperatorIds.length < operatorIds.length) {
      const datasets = this.getBlockingDatasets(operatorIds, restrictedOperatorMap);
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
