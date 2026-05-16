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

import { Injectable } from "@angular/core";
import {
  isWebDataUpdate,
  isWebPaginationUpdate,
  WebDataUpdate,
  WebPaginationUpdate,
  WebResultUpdate,
  WorkflowResultTableStats,
  WorkflowResultUpdate,
} from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { PaginatedResultEvent, WorkflowAvailableResultEvent } from "../../types/workflow-websocket.interface";
import { map, Observable, of, pairwise, ReplaySubject, Subject } from "rxjs";
import { v4 as uuid } from "uuid";
import { IndexableObject } from "../../types/result-table.interface";
import { isDefined } from "../../../common/util/predicate";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";

/**
 * WorkflowResultService manages the result data of a workflow execution.
 */
@Injectable({
  providedIn: "root",
})
export class WorkflowResultService {
  private paginatedResultServices = new Map<string, OperatorPaginationResultService>();
  private operatorResultServices = new Map<string, OperatorResultService>();

  // Alias map for macro instance IDs: macro op IDs on the canvas don't get
  // direct result entries from the engine (the engine sees the inlined inner
  // ops only). When a macro has at least one output port, route lookups for
  // the macro to the inner op feeding output port 0 so the result panel can
  // show "the macro's result" without the user having to drill down. Set by
  // `MacroService` once body bindings are fetched.
  private macroResultAliases = new Map<string, string>();

  // event stream of operator result update, undefined indicates the operator result is cleared
  private resultUpdateStream = new Subject<Record<string, WebResultUpdate | undefined>>();
  private resultTableStats = new ReplaySubject<Record<string, Record<string, Record<string, number>>>>(1);
  private resultInitiateStream = new Subject<string>();

  constructor(private wsService: WorkflowWebsocketService) {
    this.wsService.subscribeToEvent("WebResultUpdateEvent").subscribe(event => {
      this.handleResultUpdate(event.updates);
      this.handleTableStatsUpdate(event.tableStats);
    });
    this.wsService
      .subscribeToEvent("WorkflowAvailableResultEvent")
      .subscribe(event => this.handleCleanResultCache(event));
    this.resultTableStats.next({});
  }

  public hasAnyResult(operatorID: string): boolean {
    return this.hasResult(operatorID) || this.hasPaginatedResult(operatorID);
  }

  public hasResult(operatorID: string): boolean {
    return isDefined(this.getResultService(operatorID));
  }

  public hasPaginatedResult(operatorID: string): boolean {
    return isDefined(this.getPaginatedResultService(operatorID));
  }

  /**
   * Register/refresh the macro-instance → inner-op alias used to resolve
   * `getResultService` / `getPaginatedResultService` lookups for macro ops.
   * Idempotent — call whenever a macro's body bindings finish loading.
   * `innerOpId` must be a *runtime* (post-MacroExpander-prefix) ID so it
   * matches what the engine sends in `WebResultUpdateEvent`.
   */
  public setMacroResultAlias(macroInstanceId: string, innerOpId: string): void {
    this.macroResultAliases.set(macroInstanceId, innerOpId);
  }

  public clearMacroResultAlias(macroInstanceId: string): void {
    this.macroResultAliases.delete(macroInstanceId);
  }

  // When the canvas is rendering a macro body (drill-down view), the operators
  // on the canvas have body-relative IDs (e.g. `Filter-operator-xyz` from the
  // macro definition) but engine results arrive keyed by the post-expansion
  // runtime UUID assigned by MacroExpander. This map (body-op-id → runtime-
  // UUID) is populated by the workflow-editor when entering a drill-down view;
  // empty means no drill-down rewrite is active.
  //
  // The old prefix-based scheme (`${instanceId}--${bodyOpId}`) no longer works
  // because MacroExpander switched to fresh deterministic UUIDs (see
  // backend/MacroExpander.spliceIntoParent for why long prefixed names had to
  // go). The map is computed via MacroService's runtime-mapping cache.
  private drilldownAliases: Map<string, string> = new Map();

  public setDrilldownAliases(aliases: Map<string, string>): void {
    this.drilldownAliases = aliases;
  }

  private resolveAlias(operatorID: string): string {
    // Drill-down rewrite wins: when viewing a macro body during execution we
    // want the body-relative op ID lifted to its runtime UUID. Macro aliases
    // only fire on the outer canvas, where body-relative IDs aren't present.
    const drill = this.drilldownAliases.get(operatorID);
    if (drill !== undefined) return drill;
    return this.macroResultAliases.get(operatorID) ?? operatorID;
  }

  public getResultUpdateStream(): Observable<Record<string, WebResultUpdate | undefined>> {
    return this.resultUpdateStream;
  }

  public getResultTableStats(): Observable<
    [Record<string, Record<string, Record<string, number>>>, Record<string, Record<string, Record<string, number>>>]
  > {
    return this.resultTableStats.pipe(pairwise());
  }

  public getResultInitiateStream(): Observable<string> {
    return this.resultInitiateStream.asObservable();
  }

  public getPaginatedResultService(operatorID: string): OperatorPaginationResultService | undefined {
    return this.paginatedResultServices.get(this.resolveAlias(operatorID));
  }

  public getResultService(operatorID: string): OperatorResultService | undefined {
    return this.operatorResultServices.get(this.resolveAlias(operatorID));
  }

  private handleCleanResultCache(event: WorkflowAvailableResultEvent): void {
    const removedOrInvalidatedOperators = new Set<string>();
    // remove operators that no longer have results
    this.operatorResultServices.forEach((_, op) => {
      if (!(op in event.availableOperators)) {
        this.operatorResultServices.delete(op);
        removedOrInvalidatedOperators.add(op);
      }
    });
    this.paginatedResultServices.forEach((_, op) => {
      if (!(op in event.availableOperators)) {
        this.paginatedResultServices.delete(op);
        removedOrInvalidatedOperators.add(op);
      }
    });
    // for each operator that has results:
    Object.entries(event.availableOperators).forEach(availableOp => {
      const op = availableOp[0];
      const cacheValid = availableOp[1].cacheValid;
      const outputMode = availableOp[1].outputMode;

      // make sure to init or reuse result service for each operator
      const resultService = (() => {
        if (outputMode.type === "PaginationMode") {
          return this.getOrInitPaginatedResultService(op);
        } else {
          return this.getOrInitResultService(op);
        }
      })();

      // invalidate frontend cache if needed
      if (!cacheValid) {
        resultService.reset();
        removedOrInvalidatedOperators.add(op);
      }
    });

    const invalidatedOperatorsUpdate: Record<string, undefined> = {};
    removedOrInvalidatedOperators.forEach(op => (invalidatedOperatorsUpdate[op] = undefined));
    this.resultUpdateStream.next(invalidatedOperatorsUpdate);
  }

  private handleResultUpdate(event: WorkflowResultUpdate): void {
    Object.keys(event).forEach(operatorID => {
      const update = event[operatorID];
      if (isWebPaginationUpdate(update)) {
        const paginatedResultService = this.getOrInitPaginatedResultService(operatorID);
        paginatedResultService.handleResultUpdate(update);
        // clear previously saved result service
        this.operatorResultServices.delete(operatorID);
      } else if (isWebDataUpdate(update)) {
        const resultService = this.getOrInitResultService(operatorID);
        resultService.handleResultUpdate(update);
        // clear previously saved paginated result service
        this.paginatedResultServices.delete(operatorID);
      }
    });
    this.resultUpdateStream.next(event);
  }

  private handleTableStatsUpdate(event: WorkflowResultTableStats): void {
    Object.keys(event).forEach(operatorID => {
      const paginatedResultService = this.getOrInitPaginatedResultService(operatorID);
      paginatedResultService.handleStatsUpdate(event[operatorID]);
    });
    this.resultTableStats.next(event);
  }

  private getOrInitPaginatedResultService(operatorID: string): OperatorPaginationResultService {
    let service = this.getPaginatedResultService(operatorID);
    if (!service) {
      service = new OperatorPaginationResultService(operatorID, this.wsService);
      this.paginatedResultServices.set(operatorID, service);
      this.resultInitiateStream.next(operatorID);
    }
    return service;
  }

  private getOrInitResultService(operatorID: string): OperatorResultService {
    let service = this.getResultService(operatorID);
    if (!service) {
      service = new OperatorResultService(operatorID);
      this.operatorResultServices.set(operatorID, service);
      this.resultInitiateStream.next(operatorID);
    }
    return service;
  }

  public determineOutputTypes(operatorId: string): {
    hasAnyResult: boolean;
    isTableOutput: boolean;
    isVisualizationOutput: boolean;
    containsBinaryData: boolean;
  } {
    const resultService = this.getResultService(operatorId);
    const paginatedResultService = this.getPaginatedResultService(operatorId);

    return {
      hasAnyResult: this.hasAnyResult(operatorId),
      isTableOutput: this.hasTableOutput(paginatedResultService),
      containsBinaryData: this.hasBinaryData(paginatedResultService),
      isVisualizationOutput: this.hasVisualizationOutput(resultService, paginatedResultService),
    };
  }

  public determineOutputExtension(operatorId: string, defaultExtension: string = "csv"): string {
    if (defaultExtension === "data") return defaultExtension;
    var outputType = this.determineOutputTypes(operatorId);

    if (outputType.isVisualizationOutput) return "html";
    if (outputType.isTableOutput && defaultExtension === "csv") return "csv";
    return defaultExtension;
  }

  private hasTableOutput(paginatedResultService?: OperatorPaginationResultService): boolean {
    return paginatedResultService !== undefined;
  }

  private hasBinaryData(paginatedResultService?: OperatorPaginationResultService): boolean {
    return paginatedResultService?.getSchema().some(attribute => attribute.attributeType === "binary") ?? false;
  }

  private hasVisualizationOutput(
    resultService?: OperatorResultService,
    paginatedResultService?: OperatorPaginationResultService
  ): boolean {
    return resultService !== undefined && paginatedResultService === undefined;
  }
}

export class OperatorResultService {
  private resultSnapshot: ReadonlyArray<object> | undefined;

  constructor(public operatorID: string) {}

  public getCurrentResultSnapshot(): ReadonlyArray<object> | undefined {
    return this.resultSnapshot;
  }

  public reset(): void {
    this.resultSnapshot = undefined;
  }

  public handleResultUpdate(update: WebDataUpdate): void {
    if (update.mode.type === "SetSnapshotMode") {
      // update the result snapshot with latest update
      this.resultSnapshot = update.table;
    } else if (update.mode.type === "SetDeltaMode") {
      // intentionally do nothing, frontend does not accumulate delta results
    }
  }
}

export class OperatorPaginationResultService {
  private pendingRequests: Map<string, Subject<PaginatedResultEvent>> = new Map();
  private resultCache: Map<number, ReadonlyArray<object>> = new Map();
  private prevStatsCache: Record<string, Record<string, number>> = {};
  private statsCache: Record<string, Record<string, number>> = {};
  private currentPageIndex: number = 1;
  private currentTotalNumTuples: number = 0;
  private schema: ReadonlyArray<SchemaAttribute> = [];

  constructor(
    public operatorID: string,
    private workflowWebsocketService: WorkflowWebsocketService
  ) {
    this.workflowWebsocketService.subscribeToEvent("PaginatedResultEvent").subscribe(event => {
      this.schema = event.schema;
      this.handlePaginationResult(event);
    });
  }

  public getStats(): Record<string, Record<string, number>> {
    return this.statsCache;
  }

  public getPrevStats(): Record<string, Record<string, number>> {
    return this.prevStatsCache;
  }

  public getCurrentPageIndex(): number {
    return this.currentPageIndex;
  }

  public getCurrentTotalNumTuples(): number {
    return this.currentTotalNumTuples;
  }

  public getSchema(): ReadonlyArray<SchemaAttribute> {
    return this.schema;
  }

  public selectTuple(
    tupleIndex: number,
    pageSize: number
  ): Observable<{ tuple: IndexableObject; schema: ReadonlyArray<SchemaAttribute> }> {
    // calculate the page index
    // remember that page index starts from 1
    const pageIndex = Math.floor(tupleIndex / pageSize) + 1;
    return this.selectPage(pageIndex, pageSize).pipe(
      map(p => ({
        tuple: p.table[tupleIndex % pageSize],
        schema: this.schema,
      }))
    );
  }

  public selectPage(
    pageIndex: number,
    pageSize: number,
    columnOffset: number = 0,
    columnLimit: number = Number.MAX_SAFE_INTEGER,
    columnSearch: string = ""
  ): Observable<PaginatedResultEvent> {
    // update currently selected page
    this.currentPageIndex = pageIndex;
    // first fetch from frontend result cache
    const useCache = columnOffset === 0 && columnLimit === Number.MAX_SAFE_INTEGER && columnSearch === "";
    const pageCache = useCache ? this.resultCache.get(pageIndex) : undefined;
    if (pageCache) {
      return of(<PaginatedResultEvent>{
        requestID: "",
        operatorID: this.operatorID,
        pageIndex: pageIndex,
        table: pageCache,
        schema: this.schema,
      });
    } else {
      // fetch result data from server
      const requestID = uuid();
      const operatorID = this.operatorID;
      this.workflowWebsocketService.send("ResultPaginationRequest", {
        requestID,
        operatorID,
        pageIndex,
        pageSize,
        columnOffset,
        columnLimit,
        columnSearch,
      });
      const pendingRequestSubject = new Subject<PaginatedResultEvent>();
      this.pendingRequests.set(requestID, pendingRequestSubject);
      return pendingRequestSubject;
    }
  }

  public reset(): void {
    this.pendingRequests.clear();
    this.resultCache.clear();
    this.currentPageIndex = 1;
    this.currentTotalNumTuples = 0;
  }

  public handleResultUpdate(update: WebPaginationUpdate): void {
    this.currentTotalNumTuples = update.totalNumTuples;
    update.dirtyPageIndices.forEach(dirtyPage => {
      this.resultCache.delete(dirtyPage);
    });
  }

  public handleStatsUpdate(statsUpdate: Record<string, Record<string, number>>): void {
    if (!this.statsCache) {
      this.statsCache = statsUpdate;
      this.prevStatsCache = statsUpdate;
    } else {
      this.prevStatsCache = this.statsCache;
      this.statsCache = statsUpdate;
    }
  }

  private handlePaginationResult(res: PaginatedResultEvent): void {
    const pendingRequestSubject = this.pendingRequests.get(res.requestID);
    if (!pendingRequestSubject) {
      return;
    }
    pendingRequestSubject.next(res);
    pendingRequestSubject.complete();
    this.pendingRequests.delete(res.requestID);
  }
}
