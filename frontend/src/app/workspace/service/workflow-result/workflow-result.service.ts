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
  WorkflowPortResultUpdate,
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
  private paginatedResultServices = new Map<string, OperatorPaginationResultService>();
  // the services for an operator's output ports past the first, keyed "operatorID:portIndex"
  private otherPortResultServices = new Map<string, OperatorPaginationResultService>();
  // the "operatorID:portIndex" keys of those services a live update of their port made or fed
  private livePaginatedPorts = new Set<string>();
  // the visualization results of an operator's output ports past the first, keyed "operatorID:portIndex"
  private otherPortDataServices = new Map<string, OperatorResultService>();
  private operatorResultServices = new Map<string, OperatorResultService>();

  // event stream of operator result update, undefined indicates the operator result is cleared
  private resultUpdateStream = new Subject<Record<string, WebResultUpdate | undefined>>();
  // event stream of the updates of operators' output ports past the first
  private portResultUpdateStream = new Subject<WorkflowPortResultUpdate>();
  private resultTableStats = new ReplaySubject<Record<string, Record<string, Record<string, number>>>>(1);
  private resultInitiateStream = new Subject<string>();
  // emits when clearResults() drops cached results, so the UI can drop stale frames
  private resultClearedStream = new Subject<void>();

  constructor(private wsService: WorkflowWebsocketService) {
    this.wsService.subscribeToEvent("WebResultUpdateEvent").subscribe(event => {
      this.handleResultUpdate(event.updates);
      this.handleTableStatsUpdate(event.tableStats);
      this.handlePortResultUpdate(event.portUpdates ?? {});
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

  /** Emits the updates of operators' output ports past the first, once their services took them. */
  public getPortResultUpdateStream(): Observable<WorkflowPortResultUpdate> {
    return this.portResultUpdateStream.asObservable();
  }

  public getResultTableStats(): Observable<
    [Record<string, Record<string, Record<string, number>>>, Record<string, Record<string, Record<string, number>>>]
  > {
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
    return this.paginatedResultServices.get(operatorID);
  }

  /**
   * The service that pages one of the operator's output ports. A port past the first gets its service
   * from its live updates, or, for an operator whose first port is a table, on the first ask. A port
   * whose result is a visualization has none.
   */
  public getPortPaginatedResultService(
    operatorID: string,
    portIndex: number
  ): OperatorPaginationResultService | undefined {
    const firstPort = this.getPaginatedResultService(operatorID);
    if (portIndex === 0) return firstPort;
    const key = portKey(operatorID, portIndex);
    // a port whose result is a visualization is not paged
    if (this.otherPortDataServices.has(key)) return undefined;
    let service = this.otherPortResultServices.get(key);
    if (!service && firstPort) {
      service = new OperatorPaginationResultService(operatorID, this.wsService, portIndex);
      this.otherPortResultServices.set(key, service);
    }
    return service;
  }

  public getResultService(operatorID: string): OperatorResultService | undefined {
    return this.operatorResultServices.get(operatorID);
  }

  /** The visualization result of one of the operator's output ports. */
  public getPortResultService(operatorID: string, portIndex: number): OperatorResultService | undefined {
    if (portIndex === 0) return this.getResultService(operatorID);
    return this.otherPortDataServices.get(portKey(operatorID, portIndex));
  }

  /** The indices of the operator's output ports that have a result, ascending. */
  public getResultPortIndices(operatorID: string): number[] {
    const indices = this.hasAnyResult(operatorID) ? [0] : [];
    const prefix = `${operatorID}:`;
    [...this.livePaginatedPorts, ...this.otherPortDataServices.keys()]
      .filter(key => key.startsWith(prefix))
      .forEach(key => indices.push(Number(key.slice(prefix.length))));
    return indices.sort((a, b) => a - b);
  }

  /**
   * Drop cached results and reset table stats so a re-entered workflow doesn't show
   * stale results (resultTableStats is a ReplaySubject, so push an empty snapshot).
   * Emits resultClearedStream so subscribers tear down already-displayed frames.
   */
  public clearResults(): void {
    this.operatorResultServices.clear();
    this.paginatedResultServices.clear();
    this.otherPortResultServices.clear();
    this.livePaginatedPorts.clear();
    this.otherPortDataServices.clear();
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
        this.paginatedResultServices.delete(operatorID);
      }
    });
    this.resultUpdateStream.next(event);
  }

  private handlePortResultUpdate(event: WorkflowPortResultUpdate): void {
    const operatorIDs = Object.keys(event);
    operatorIDs.forEach(operatorID =>
      Object.entries(event[operatorID]).forEach(([port, update]) => {
        const portIndex = Number(port);
        const key = portKey(operatorID, portIndex);
        // a port's result that is new, or that turned from a table into a visualization or back, is
        // announced, so a panel showing the port mounts the frame for its kind
        let announce = false;
        if (isWebPaginationUpdate(update)) {
          announce = !this.livePaginatedPorts.has(key);
          let service = this.otherPortResultServices.get(key);
          if (!service) {
            service = new OperatorPaginationResultService(operatorID, this.wsService, portIndex);
            this.otherPortResultServices.set(key, service);
          }
          service.handleResultUpdate(update);
          this.otherPortDataServices.delete(key);
          this.livePaginatedPorts.add(key);
        } else if (isWebDataUpdate(update)) {
          let service = this.otherPortDataServices.get(key);
          announce = !service;
          if (!service) {
            service = new OperatorResultService(operatorID);
            this.otherPortDataServices.set(key, service);
          }
          service.handleResultUpdate(update);
          this.otherPortResultServices.delete(key);
          this.livePaginatedPorts.delete(key);
        }
        if (announce) {
          this.resultInitiateStream.next(operatorID);
        }
      })
    );
    if (operatorIDs.length > 0) {
      this.portResultUpdateStream.next(event);
    }
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

  public determineOutputTypes(
    operatorId: string,
    portIndex: number = 0
  ): {
    hasAnyResult: boolean;
    isTableOutput: boolean;
    isVisualizationOutput: boolean;
    containsBinaryData: boolean;
  } {
    const resultService = this.getPortResultService(operatorId, portIndex);
    const paginatedResultService =
      portIndex === 0
        ? this.getPaginatedResultService(operatorId)
        : // a port asked about before it has a result has a service, but no result
          this.livePaginatedPorts.has(portKey(operatorId, portIndex))
          ? this.otherPortResultServices.get(portKey(operatorId, portIndex))
          : undefined;

    return {
      hasAnyResult: resultService !== undefined || paginatedResultService !== undefined,
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
    // which output port it pages. The running execution reports only on the first, so any other
    // port keeps no page cache and takes its row count from each reply.
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
      this.workflowWebsocketService.send("ResultPaginationRequest", {
        requestID,
        operatorID,
        pageIndex,
        pageSize,
        columnOffset,
        columnLimit,
        columnSearch,
        ...this.portField(),
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
    this.statsCache = statsUpdate;
  }

  // the first port is the server's default, so a request for it leaves the field out
  private portField(): { portIndex?: number } {
    return this.portIndex === 0 ? {} : { portIndex: this.portIndex };
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
