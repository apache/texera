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
import type { ResultTarget } from "@texera/workflow-macros";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
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
import { PaginatedResultEvent } from "../../types/workflow-websocket.interface";
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
  private macroAliases: () => Record<string, ResultTarget | null> = () => ({});
  public setMacroAliases(provider: () => Record<string, ResultTarget | null>): void {
    this.macroAliases = provider;
  }
  private withMacroAliases<T>(event: Record<string, T>): Record<string, T> {
    const aliases = this.macroAliases();
    if (!Object.keys(aliases).length) return event;
    const result = { ...event };
    for (const [alias, target] of Object.entries(aliases)) {
      if (!target || target.portIndex !== 0) delete result[alias];
      else if (event[target.operatorID] !== undefined) result[alias] = event[target.operatorID];
    }
    return result;
  }
  private activeExecution?: number;
  private readonly tables = new Map<string, Map<number, OperatorPaginationResultService>>();
  private get tableServices() {
    return [...this.tables.values()].flatMap(ports => [...ports.values()]);
  }
  private operatorResultServices = new Map<string, OperatorResultService>();

  // event stream of operator result update, undefined indicates the operator result is cleared
  private resultUpdateStream = new Subject<Record<string, WebResultUpdate | undefined>>();
  private resultTableStats = new ReplaySubject<WorkflowResultTableStats>(1);
  private resultInitiateStream = new Subject<string>();
  // emits when clearResults() drops cached results, so the UI can drop stale frames
  private resultClearedStream = new Subject<void>();

  constructor(
    private wsService: WorkflowWebsocketService,
    private status: WorkflowStatusService
  ) {
    this.wsService.subscribeToEvent("WorkflowStateEvent").subscribe(({ executionId }) => {
      if (!executionId || executionId === this.activeExecution) return;
      const tables = this.tableServices;
      if (this.activeExecution || tables.some(table => table.executionId && table.executionId !== executionId)) {
        this.clearResults();
      } else {
        // Reconnecting replays the workflow's result store before its execution metadata.
        tables.forEach(table => (table.executionId = executionId));
      }
      this.activeExecution = executionId;
    });
    this.wsService.subscribeToEvent("WebResultUpdateEvent").subscribe(event => {
      this.handleResultUpdate(event.updates);
      this.handleTableStatsUpdate(event.tableStats);
    });
    this.resultTableStats.next({});
  }

  public hasAnyResult(operatorID: string): boolean {
    return this.hasResult(operatorID) || this.hasPaginatedResult(operatorID);
  }

  /**
   * Whether the operator produced an actual, non-empty result. A step marked view-result still
   * registers a (paginated) result service with zero tuples -- e.g. a UDF that only writes a file
   * or logs -- so hasAnyResult alone is true for those. This checks the tuple count / snapshot
   * length so an empty result reads as "no result".
   */
  public hasNonEmptyResult(operatorID: string): boolean {
    const paginated = this.getPaginatedResultService(operatorID);
    if (paginated) {
      return paginated.getCurrentTotalNumTuples() > 0;
    }
    return (this.getResultService(operatorID)?.getCurrentResultSnapshot()?.length ?? 0) > 0;
  }

  public hasResult(operatorID: string): boolean {
    return isDefined(this.getResultService(operatorID));
  }

  public hasPaginatedResult(operatorID: string): boolean {
    return isDefined(this.getPaginatedResultService(operatorID));
  }

  public getResultUpdateStream(): Observable<Record<string, WebResultUpdate | undefined>> {
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

  public getPaginatedResultService(operatorID: string): OperatorPaginationResultService | undefined {
    const id = this.macroAliases()[operatorID];
    return id === null
      ? undefined
      : this.getPortPaginatedResultService(id?.operatorID ?? operatorID, id?.portIndex ?? 0);
  }

  /**
   * The service that pages one of the operator's output ports. The first port's is the one the
   * running execution keeps up to date. Any other port is read only when asked, so its service is
   * made on the first ask, for an operator whose result is a table.
   */
  public getPortPaginatedResultService(
    operatorID: string,
    portIndex: number
  ): OperatorPaginationResultService | undefined {
    const firstPort = this.tables.get(operatorID)?.get(0);
    if (portIndex === 0 || (!firstPort && !this.operatorResultServices.has(operatorID))) return firstPort;
    return this.getOrInitPaginatedResultService(operatorID, portIndex);
  }

  private loadPortCount(service: OperatorPaginationResultService): void {
    if (service.hasPendingPage) return;
    service.selectPage(service.getCurrentPageIndex(), 1).subscribe(({ totalTuples }) => {
      const update: WebPaginationUpdate = {
        mode: { type: "PaginationMode" },
        totalNumTuples: totalTuples,
        dirtyPageIndices: [],
      };
      const aliases = Object.entries(this.macroAliases()).filter(
        ([, target]) => target?.operatorID === service.operatorID && target.portIndex === service.portIndex
      );
      if (aliases.length) this.resultUpdateStream.next(Object.fromEntries(aliases.map(([alias]) => [alias, update])));
    });
  }

  public getResultService(operatorID: string): OperatorResultService | undefined {
    const id = this.macroAliases()[operatorID];
    return id === null || id?.portIndex ? undefined : this.operatorResultServices.get(id?.operatorID ?? operatorID);
  }

  /**
   * Drop cached results and reset table stats so a re-entered workflow doesn't show
   * stale results (resultTableStats is a ReplaySubject, so push an empty snapshot).
   * Emits resultClearedStream so subscribers tear down already-displayed frames.
   */
  public clearResults(): void {
    this.activeExecution = undefined;
    this.operatorResultServices.clear();
    this.tableServices.forEach(service => service.dispose());
    this.tables.clear();
    this.resultTableStats.next({});
    this.resultClearedStream.next();
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
        const ports = this.tables.get(operatorID);
        ports?.get(0)?.dispose();
        ports?.delete(0);
        if (ports?.size === 0) this.tables.delete(operatorID);
      }
      this.tables.get(operatorID)?.forEach(port => {
        if (update && port.portIndex !== 0) this.loadPortCount(port);
      });
    });
    this.resultUpdateStream.next(this.withMacroAliases(event));
  }

  private handleTableStatsUpdate(event: WorkflowResultTableStats): void {
    Object.keys(event).forEach(operatorID => {
      const paginatedResultService = this.getOrInitPaginatedResultService(operatorID);
      paginatedResultService.handleStatsUpdate(event[operatorID]);
    });
    this.resultTableStats.next(this.withMacroAliases(event));
  }

  private getOrInitPaginatedResultService(operatorID: string, portIndex = 0): OperatorPaginationResultService {
    const ports = this.tables.get(operatorID) ?? new Map<number, OperatorPaginationResultService>();
    const cached = ports.get(portIndex);
    if (cached) return cached;
    const service = new OperatorPaginationResultService(
      operatorID,
      this.wsService,
      portIndex,
      this.status.runtimeExecutionId
    );
    ports.set(portIndex, service);
    this.tables.set(operatorID, ports);
    if (portIndex === 0) this.resultInitiateStream.next(operatorID);
    else this.loadPortCount(service);
    return service;
  }

  private getOrInitResultService(operatorID: string): OperatorResultService {
    let service = this.operatorResultServices.get(operatorID);
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
    const snapshot = this.getResultService(operatorId);
    const table = this.getPaginatedResultService(operatorId);
    return {
      hasAnyResult: !!(snapshot || table),
      isTableOutput: !!table,
      containsBinaryData: table?.getSchema().some(attribute => attribute.attributeType === "binary") ?? false,
      isVisualizationOutput: !!snapshot && !table,
    };
  }

  public determineOutputExtension(operatorId: string, defaultExtension: string = "csv"): string {
    if (defaultExtension === "data") return defaultExtension;
    var outputType = this.determineOutputTypes(operatorId);

    if (outputType.isVisualizationOutput) return "html";
    if (outputType.isTableOutput && defaultExtension === "csv") return "csv";
    return defaultExtension;
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
  private readonly subscription;
  public get hasPendingPage(): boolean {
    return this.pendingRequests.size > 0;
  }
  private pendingRequests: Map<string, Subject<PaginatedResultEvent>> = new Map();
  private resultCache: Map<number, ReadonlyArray<object>> = new Map();
  private statsCache: Record<string, Record<string, number>> = {};
  private currentPageIndex: number = 1;
  private currentTotalNumTuples: number = 0;
  private schema: ReadonlyArray<SchemaAttribute> = [];

  constructor(
    public operatorID: string,
    private workflowWebsocketService: WorkflowWebsocketService,
    // which output port it pages. The running execution reports only on the first, so any other
    // port keeps no page cache and takes its row count from each reply.
    public readonly portIndex: number = 0,
    public executionId?: number
  ) {
    this.subscription = this.workflowWebsocketService.subscribeToEvent("PaginatedResultEvent").subscribe(event => {
      this.handlePaginationResult(event);
    });
  }

  public dispose(): void {
    this.subscription.unsubscribe();
    this.pendingRequests.forEach(request => request.complete());
    this.pendingRequests.clear();
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
    columnLimit: number = Number.MAX_SAFE_INTEGER,
    columnSearch: string = ""
  ): Observable<PaginatedResultEvent> {
    // update currently selected page
    this.currentPageIndex = pageIndex;
    // first fetch from frontend result cache
    const useCache =
      this.portIndex === 0 && columnOffset === 0 && columnLimit === Number.MAX_SAFE_INTEGER && columnSearch === "";
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
      const pendingRequestSubject = new ReplaySubject<PaginatedResultEvent>(1);
      this.pendingRequests.set(requestID, pendingRequestSubject);
      this.workflowWebsocketService.send("ResultPaginationRequest", {
        requestID,
        operatorID,
        pageIndex,
        pageSize,
        columnOffset,
        columnLimit: columnLimit === Number.MAX_SAFE_INTEGER ? undefined : columnLimit,
        columnSearch,
        ...(this.portIndex !== 0 && { portIndex: this.portIndex }),
        ...(this.executionId && { executionId: this.executionId }),
      });
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
    if (this.portIndex !== 0) {
      this.currentTotalNumTuples = res.totalTuples;
    }
    pendingRequestSubject.next(res);
    pendingRequestSubject.complete();
    this.pendingRequests.delete(res.requestID);
  }
}
