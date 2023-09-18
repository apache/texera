import { Injectable } from "@angular/core";
import {
  isWebDataUpdate,
  isWebPaginationUpdate,
  WebDataUpdate,
  WebPaginationUpdate,
  WebResultUpdate,
  WorkflowResultUpdate,
} from "../../types/execute-workflow.interface";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { PaginatedResultEvent, WorkflowAvailableResultEvent } from "../../types/workflow-websocket.interface";
import { map, Observable, of, Subject } from "rxjs";
import { v4 as uuid } from "uuid";
import { ChartType } from "../../types/visualization.interface";
import { IndexableObject } from "../../types/result-table.interface";

export const DEFAULT_PAGE_SIZE = 5;

/**
 * WorkflowResultService manages the result data of a workflow execution.
 */
@Injectable({
  providedIn: "root",
})
export class WorkflowResultService {
  private paginatedResultServices = new Map<string, OperatorPaginationResultService>();
  private operatorResultServices = new Map<string, OperatorResultService>();

  // event stream of operator result update, undefined indicates the operator result is cleared
  private resultUpdateStream = new Subject<Record<string, WebResultUpdate | undefined>>();
  private resultInitiateStream = new Subject<string>();

  constructor(private wsService: WorkflowWebsocketService) {
    this.wsService.subscribeToEvent("WebResultUpdateEvent").subscribe(event => this.handleResultUpdate(event.updates));
    this.wsService
      .subscribeToEvent("WorkflowAvailableResultEvent")
      .subscribe(event => this.handleCleanResultCache(event));
  }

  public getResultUpdateStream(): Observable<Record<string, WebResultUpdate | undefined>> {
    return this.resultUpdateStream;
  }

  public getResultInitiateStream(): Observable<string> {
    return this.resultInitiateStream.asObservable();
  }

  public getPaginatedResultService(operatorID: string): OperatorPaginationResultService | undefined {
    return this.paginatedResultServices.get(operatorID);
  }

  public getResultService(operatorID: string): OperatorResultService | undefined {
    return this.operatorResultServices.get(operatorID);
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
      } else {
        const _exhaustiveCheck: never = update;
      }
    });
    this.resultUpdateStream.next(event);
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
}

export class OperatorResultService {
  private chartType: ChartType | undefined;
  private resultSnapshot: ReadonlyArray<object> | undefined;

  constructor(public operatorID: string) {}

  public getCurrentResultSnapshot(): ReadonlyArray<object> | undefined {
    return this.resultSnapshot;
  }

  public getChartType(): ChartType | undefined {
    return this.chartType;
  }

  public reset(): void {
    this.chartType = undefined;
    this.resultSnapshot = undefined;
  }

  public handleResultUpdate(update: WebDataUpdate): void {
    this.chartType = update.chartType;
    if (update.mode.type === "SetSnapshotMode") {
      // update the result snapshot with latest update
      this.resultSnapshot = update.table;
    } else if (update.mode.type === "SetDeltaMode") {
      // intentionally do nothing, frontend does not accumulate delta results
    } else {
      const _exhaustiveCheck: never = update.mode;
    }
  }
}

class OperatorPaginationResultService {
  private pendingRequests: Map<string, Subject<PaginatedResultEvent>> = new Map();
  private resultCache: Map<number, ReadonlyArray<object>> = new Map();
  private currentPageIndex: number = 1;
  private currentTotalNumTuples: number = 0;

  constructor(public operatorID: string, private workflowWebsocketService: WorkflowWebsocketService) {
    this.workflowWebsocketService
      .subscribeToEvent("PaginatedResultEvent")
      .subscribe(event => this.handlePaginationResult(event));
  }

  public getCurrentPageIndex(): number {
    return this.currentPageIndex;
  }

  public getCurrentTotalNumTuples(): number {
    return this.currentTotalNumTuples;
  }

  public selectTuple(tupleIndex: number, pageSize: number): Observable<IndexableObject> {
    if (pageSize !== DEFAULT_PAGE_SIZE) {
      throw new Error("only support fixed page size right now");
    }
    // calculate the page index
    // remember that page index starts from 1
    const pageIndex = Math.floor(tupleIndex / pageSize) + 1;
    return this.selectPage(pageIndex, pageSize).pipe(map(p => p.table[tupleIndex % pageSize]));
  }

  public selectPage(pageIndex: number, pageSize: number): Observable<PaginatedResultEvent> {
    if (pageSize !== DEFAULT_PAGE_SIZE) {
      throw new Error("only support fixed page size right now");
    }
    // update currently selected page
    this.currentPageIndex = pageIndex;
    // first fetch from frontend result cache
    const pageCache = this.resultCache.get(pageIndex);
    if (pageCache) {
      return of(<PaginatedResultEvent>{
        requestID: "",
        operatorID: this.operatorID,
        pageIndex: pageIndex,
        table: pageCache,
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
