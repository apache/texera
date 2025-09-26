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

@Injectable({
  providedIn: "root",
})
export class WorkflowResultExportService {
  hasResultToExportOnHighlightedOperators: boolean = false;
  hasResultToExportOnAllOperators = new BehaviorSubject<boolean>(false);
  private datasetDownloadableMap = new Map<string, boolean>();
  private datasetLabelMap = new Map<string, string>();
  private restrictedOperatorMap = new Map<string, Set<string>>();
  private datasetListLoaded = false;
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
    this.registerRestrictionRecomputeTriggers();
    this.refreshDatasetMetadata().subscribe();
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

  private registerRestrictionRecomputeTriggers(): void {
    const texeraGraph = this.workflowActionService.getTexeraGraph();
    merge(
      texeraGraph.getOperatorAddStream(),
      texeraGraph.getOperatorDeleteStream(),
      texeraGraph.getOperatorPropertyChangeStream(),
      texeraGraph.getLinkAddStream(),
      texeraGraph.getLinkDeleteStream(),
      texeraGraph.getDisabledOperatorsChangedStream()
    ).subscribe(() => {
      this.runRestrictionAnalysis();
    });
  }

  public refreshDatasetMetadata(): Observable<void> {
    this.datasetListLoaded = false;
    return this.datasetService.retrieveAccessibleDatasets().pipe(
      take(1),
      tap(datasets => {
        this.datasetDownloadableMap.clear();
        this.datasetLabelMap.clear();
        datasets.forEach(dataset => {
          const key = this.buildDatasetKey(dataset.ownerEmail, dataset.dataset.name);
          const isDownloadable = dataset.dataset.isDownloadable || dataset.isOwner;
          this.datasetDownloadableMap.set(key, isDownloadable);
          this.datasetLabelMap.set(key, `${dataset.dataset.name} (${dataset.ownerEmail})`);
        });
        this.datasetListLoaded = true;
        this.runRestrictionAnalysis();
      }),
      map(() => undefined),
      catchError(() => {
        this.datasetDownloadableMap.clear();
        this.datasetLabelMap.clear();
        this.datasetListLoaded = true;
        this.runRestrictionAnalysis();
        return of(undefined);
      })
    );
  }

  private buildDatasetKey(ownerEmail: string, datasetName: string): string {
    return `${ownerEmail.toLowerCase()}::${datasetName.toLowerCase()}`;
  }

  private extractDatasetInfo(fileName: unknown): { key: string; label: string } | null {
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
      if (!this.datasetDownloadableMap.has(key)) {
        return null;
      }
      const label = this.datasetLabelMap.get(key) ?? `${datasetName} (${ownerEmail})`;
      return { key, label };
    } catch {
      return null;
    }
  }

  private runRestrictionAnalysis(): void {
    if (!this.datasetListLoaded) {
      this.restrictedOperatorMap.clear();
      this.updateExportAvailabilityFlags();
      return;
    }

    const texeraGraph = this.workflowActionService.getTexeraGraph();
    const allOperators = texeraGraph.getAllOperators();
    const operatorById = new Map(allOperators.map(op => [op.operatorID, op] as const));
    const enabledOperators = allOperators.filter(operator => !operator.isDisabled);
    const datasetSources: Array<{ operatorId: string; label: string }> = [];

    enabledOperators.forEach(operator => {
      const datasetInfo = this.extractDatasetInfo(operator.operatorProperties?.fileName);
      if (!datasetInfo) {
        return;
      }
      const isDownloadable = this.datasetDownloadableMap.get(datasetInfo.key);
      if (isDownloadable === false) {
        datasetSources.push({ operatorId: operator.operatorID, label: datasetInfo.label });
      }
    });

    const restrictions = new Map<string, Set<string>>();

    if (datasetSources.length === 0) {
      this.restrictedOperatorMap = restrictions;
      this.updateExportAvailabilityFlags();
      return;
    }

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

    this.restrictedOperatorMap = restrictions;
    this.updateExportAvailabilityFlags();
  }

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

  private isOperatorEligibleForExport(operatorId: string): boolean {
    if (this.restrictedOperatorMap.has(operatorId)) {
      return false;
    }
    return (
      this.workflowResultService.hasAnyResult(operatorId) ||
      this.workflowResultService.getResultService(operatorId)?.getCurrentResultSnapshot() !== undefined
    );
  }

  public getExportableOperatorIds(operatorIds: readonly string[]): string[] {
    return operatorIds.filter(operatorId => !this.restrictedOperatorMap.has(operatorId));
  }

  public getBlockedOperatorIds(operatorIds: readonly string[]): string[] {
    return operatorIds.filter(operatorId => this.restrictedOperatorMap.has(operatorId));
  }

  public hasBlockedOperators(operatorIds: readonly string[]): boolean {
    return operatorIds.some(operatorId => this.restrictedOperatorMap.has(operatorId));
  }

  public getBlockingDatasets(operatorIds: readonly string[]): string[] {
    const labels = new Set<string>();
    operatorIds.forEach(operatorId => {
      const datasets = this.restrictedOperatorMap.get(operatorId);
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
    this.refreshDatasetMetadata()
      .pipe(take(1))
      .subscribe(() =>
        this.performExport(
          exportType,
          workflowName,
          datasetIds,
          rowIndex,
          columnIndex,
          filename,
          exportAll,
          destination,
          unit
        )
      );
  }

  private performExport(
    exportType: string,
    workflowName: string,
    datasetIds: number[],
    rowIndex: number,
    columnIndex: number,
    filename: string,
    exportAll: boolean,
    destination: "dataset" | "local",
    unit: DashboardWorkflowComputingUnit | null
  ): void {
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

    const exportableOperatorIds = this.getExportableOperatorIds(operatorIds);

    if (exportableOperatorIds.length === 0) {
      const datasets = this.getBlockingDatasets(operatorIds);
      const suffix = datasets.length > 0 ? `: ${datasets.join(", ")}` : "";
      this.notificationService.error(
        `Cannot export result: selection depends on dataset(s) that are not downloadable${suffix}`
      );
      return;
    }

    if (exportableOperatorIds.length < operatorIds.length) {
      const datasets = this.getBlockingDatasets(operatorIds);
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
