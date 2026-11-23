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
  WorkflowResultTableStats,
  WorkflowResultUpdate,
} from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { PaginatedResultEvent } from "../../types/workflow-websocket.interface";
import { map, Observable, of, pairwise, ReplaySubject, Subject } from "rxjs";
import { v4 as uuid } from "uuid";
import { IndexableObject } from "../../types/result-table.interface";
import { isDefined } from "../../../common/util/predicate";
import { SchemaAttribute } from "../../types/workflow-compiling.interface";

/**
 * WorkflowResultService manages the result data of a workflow execution. Every output port of an
 * operator has its own result; a port is named by its index, and the getters default to port 0.
 */
@Injectable({
  providedIn: "root",
})
export class WorkflowResultService {
  // the paged (table) result of each output port, keyed by portKey(operatorID, portIndex)
  private paginatedResultServices = new Map<string, OperatorPaginationResultService>();
  // the visualization result of each output port, keyed by portKey(operatorID, portIndex)
  private operatorResultServices = new Map<string, OperatorResultService>();

  // event stream of the result updates of output ports, keyed by operator id and then by port index
  private resultUpdateStream = new Subject<WorkflowResultUpdate>();
  private resultTableStats = new ReplaySubject<WorkflowResultTableStats>(1);
  private resultInitiateStream = new Subject<string>();
  // emits when clearResults() drops cached results, so the UI can drop stale frames
  private resultClearedStream = new Subject<void>();

  constructor(private wsService: WorkflowWebsocketService) {
    this.wsService.subscribeToEvent("WebResultUpdateEvent").subscribe(event => {
      this.handleResultUpdate(event.updates);
      this.handleTableStatsUpdate(event.tableStats);
    });
    this.resultTableStats.next({});
  }

  public hasAnyResult(operatorID: string, portIndex: number = 0): boolean {
    return this.hasResult(operatorID, portIndex) || this.hasPaginatedResult(operatorID, portIndex);
  }

  /**
   * Whether the operator produced an actual, non-empty result. A step marked view-result still
   * registers a (paginated) result service with zero tuples -- e.g. a UDF that only writes a file
   * or logs -- so hasAnyResult alone is true for those. This checks the tuple count / snapshot
   * length so an empty result reads as "no result".
   */
  public hasNonEmptyResult(operatorID: string, portIndex: number = 0): boolean {
    const paginated = this.getPaginatedResultService(operatorID, portIndex);
    if (paginated) {
      return paginated.getCurrentTotalNumTuples() > 0;
    }
    return (this.getResultService(operatorID, portIndex)?.getCurrentResultSnapshot()?.length ?? 0) > 0;
  }

  public hasResult(operatorID: string, portIndex: number = 0): boolean {
    return isDefined(this.getResultService(operatorID, portIndex));
  }

  public hasPaginatedResult(operatorID: string, portIndex: number = 0): boolean {
    return isDefined(this.getPaginatedResultService(operatorID, portIndex));
  }

  public getResultUpdateStream(): Observable<WorkflowResultUpdate> {
    return this.resultUpdateStream;
  }

  public getResultTableStats(): Observable<[WorkflowResultTableStats, WorkflowResultTableStats]> {
    return this.resultTableStats.pipe(pairwise());
  }

  public getResultInitiateStream(): Observable<string> {
    return this.resultInitiateStream.asObservable();
  }

  /**
   * Emits when clearResults() drops cached results, so consumers can tear down
   * stale frames (clearing the caches alone won't re-render a displayed operator).
   */
  public getResultClearedStream(): Observable<void> {
    return this.resultClearedStream.asObservable();
  }

  public getPaginatedResultService(
    operatorID: string,
    portIndex: number = 0
  ): OperatorPaginationResultService | undefined {
    return this.paginatedResultServices.get(portKey(operatorID, portIndex));
  }

  public getResultService(operatorID: string, portIndex: number = 0): OperatorResultService | undefined {
    return this.operatorResultServices.get(portKey(operatorID, portIndex));
  }

  /**
   * Drop cached results and reset table stats so a re-entered workflow doesn't show
   * stale results (resultTableStats is a ReplaySubject, so push an empty snapshot).
   * Emits resultClearedStream so subscribers tear down already-displayed frames.
   */
  public clearResults(): void {
    this.operatorResultServices.clear();
    this.paginatedResultServices.clear();
    this.resultTableStats.next({});
    this.resultClearedStream.next();
  }

  private handleResultUpdate(event: WorkflowResultUpdate): void {
    Object.entries(event).forEach(([operatorID, ports]) =>
      Object.entries(ports).forEach(([port, update]) => {
        const portIndex = Number(port);
        const key = portKey(operatorID, portIndex);
        if (isWebPaginationUpdate(update)) {
          this.getOrInitPaginatedResultService(operatorID, portIndex).handleResultUpdate(update);
          // clear previously saved result service
          this.operatorResultServices.delete(key);
        } else if (isWebDataUpdate(update)) {
          this.getOrInitResultService(operatorID, portIndex).handleResultUpdate(update);
          // clear previously saved paginated result service
          this.paginatedResultServices.delete(key);
        }
      })
    );
    this.resultUpdateStream.next(event);
  }

  private handleTableStatsUpdate(event: WorkflowResultTableStats): void {
    Object.entries(event).forEach(([operatorID, ports]) =>
      Object.entries(ports).forEach(([port, stats]) =>
        this.getOrInitPaginatedResultService(operatorID, Number(port)).handleStatsUpdate(stats)
      )
    );
    this.resultTableStats.next(event);
  }

  private getOrInitPaginatedResultService(operatorID: string, portIndex: number): OperatorPaginationResultService {
    let service = this.getPaginatedResultService(operatorID, portIndex);
    if (!service) {
      service = new OperatorPaginationResultService(operatorID, this.wsService, portIndex);
      this.paginatedResultServices.set(portKey(operatorID, portIndex), service);
      this.resultInitiateStream.next(operatorID);
    }
    return service;
  }

  private getOrInitResultService(operatorID: string, portIndex: number): OperatorResultService {
    let service = this.getResultService(operatorID, portIndex);
    if (!service) {
      service = new OperatorResultService(operatorID);
      this.operatorResultServices.set(portKey(operatorID, portIndex), service);
      this.resultInitiateStream.next(operatorID);
    }
    return service;
  }

  public determineOutputTypes(
    operatorId: string,
    portIndex: number = 0
  ): {
    hasAnyResult: boolean;
    isTableOutput: boolean;
    isVisualizationOutput: boolean;
    containsBinaryData: boolean;
  } {
    const resultService = this.getResultService(operatorId, portIndex);
    const paginatedResultService = this.getPaginatedResultService(operatorId, portIndex);

    return {
      hasAnyResult: this.hasAnyResult(operatorId, portIndex),
      isTableOutput: this.hasTableOutput(paginatedResultService),
      containsBinaryData: this.hasBinaryData(paginatedResultService),
      isVisualizationOutput: this.hasVisualizationOutput(resultService, paginatedResultService),
    };
  }

  public determineOutputExtension(operatorId: string, defaultExtension: string = "csv", portIndex: number = 0): string {
    if (defaultExtension === "data") return defaultExtension;
    var outputType = this.determineOutputTypes(operatorId, portIndex);

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

function portKey(operatorID: string, portIndex: number): string {
  return `${operatorID}:${portIndex}`;
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

/**
 * A column limit that asks for every column. The server reads the limit as a 32-bit Int, so it is
 * Int.MaxValue there; Number.MAX_SAFE_INTEGER fails to parse and the request is never answered.
 */
export const ALL_COLUMNS = 2147483647;

export class OperatorPaginationResultService {
  private pendingRequests: Map<string, Subject<PaginatedResultEvent>> = new Map();
  private resultCache: Map<number, ReadonlyArray<object>> = new Map();
  private statsCache: Record<string, Record<string, number>> = {};
  private currentPageIndex: number = 1;
  private currentTotalNumTuples: number = 0;
  private schema: ReadonlyArray<SchemaAttribute> = [];

  constructor(
    public operatorID: string,
    private workflowWebsocketService: WorkflowWebsocketService,
    // which output port it pages
    public readonly portIndex: number = 0
  ) {
    this.workflowWebsocketService.subscribeToEvent("PaginatedResultEvent").subscribe(event => {
      this.handlePaginationResult(event);
    });
  }

  public getStats(): Record<string, Record<string, number>> {
    return this.statsCache;
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
    columnLimit: number = ALL_COLUMNS,
    columnSearch: string = ""
  ): Observable<PaginatedResultEvent> {
    // update currently selected page
    this.currentPageIndex = pageIndex;
    // first fetch from frontend result cache
    const useCache = columnOffset === 0 && columnLimit === ALL_COLUMNS && columnSearch === "";
    const pageCache = useCache ? this.resultCache.get(pageIndex) : undefined;
    if (pageCache) {
      return of(<PaginatedResultEvent>{
        requestID: "",
        operatorID: this.operatorID,
        pageIndex: pageIndex,
        table: pageCache,
        schema: this.schema,
        portIndex: this.portIndex,
        totalTuples: this.currentTotalNumTuples,
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
        portIndex: this.portIndex,
      });
      const pendingRequestSubject = new Subject<PaginatedResultEvent>();
      this.pendingRequests.set(requestID, pendingRequestSubject);
      return pendingRequestSubject;
    }
  }

  /**
   * The first rows of the result, for a glance at it. Unlike selectPage this leaves the page the
   * result table is on alone and never touches its cache, so a preview cannot move the table.
   * Every column is asked for, so the schema the reply carries is the whole one, as a table
   * page's would be.
   */
  public peekFirstRows(rowCount: number): Observable<PaginatedResultEvent> {
    const requestID = uuid();
    this.workflowWebsocketService.send("ResultPaginationRequest", {
      requestID,
      operatorID: this.operatorID,
      pageIndex: 1,
      pageSize: rowCount,
      columnOffset: 0,
      columnLimit: ALL_COLUMNS,
      columnSearch: "",
      portIndex: this.portIndex,
    });
    const pendingRequestSubject = new Subject<PaginatedResultEvent>();
    this.pendingRequests.set(requestID, pendingRequestSubject);
    return pendingRequestSubject;
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
    this.statsCache = statsUpdate;
  }

  private handlePaginationResult(res: PaginatedResultEvent): void {
    const pendingRequestSubject = this.pendingRequests.get(res.requestID);
    if (!pendingRequestSubject) {
      return;
    }
    // Every operator's and every port's replies arrive on one stream, so only a reply to this
    // service's own request may set its schema.
    this.schema = res.schema;
    pendingRequestSubject.next(res);
    pendingRequestSubject.complete();
    this.pendingRequests.delete(res.requestID);
  }
}
