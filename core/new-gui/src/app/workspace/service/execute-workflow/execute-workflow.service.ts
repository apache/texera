import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraphReadonly, WorkflowGraph } from './../workflow-graph/model/workflow-graph';
import {
  LogicalLink, LogicalPlan, LogicalOperator,
  ExecutionResult, ErrorExecutionResult, SuccessExecutionResult, BreakpointInfo, ExecutionState, ExecutionStateInfo
} from '../../types/execute-workflow.interface';
import { ResultObject } from '../../types/execute-workflow.interface';
import { v4 as uuid } from 'uuid';
import { environment } from '../../../../environments/environment';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { OperatorPredicate, BreakpointTriggerInfo, BreakpointRequest, Breakpoint } from '../../types/workflow-common.interface';
import { TexeraWebsocketEvent, WorkerTuples, OperatorCurrentTuples } from '../../types/workflow-websocket.interface';
import { isEqual } from 'lodash';
import { PAGINATION_INFO_STORAGE_KEY, ResultPaginationInfo } from '../../types/result-table.interface';
import { sessionGetObject, sessionSetObject } from 'src/app/common/util/storage';

export const FORM_DEBOUNCE_TIME_MS = 150;

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

export const DOWNLOAD_WORKFLOW_ENDPOINT = 'download/result';
export const PAUSE_WORKFLOW_ENDPOINT = 'pause';
export const RESUME_WORKFLOW_ENDPOINT = 'resume';

export const EXECUTION_TIMEOUT = 3000;

/**
 * ExecuteWorkflowService sends the current workflow data to the backend
 *  for execution, then receives backend's response and broadcast it to other components.
 *
 * ExecuteWorkflowService transforms the frontend workflow graph
 *  into backend API compatible workflow graph before sending the request.
 *
 * Components should call executeWorkflow() function to execute the current workflow
 *
 * Components and Services should subscribe to getExecuteStartedStream()
 *  in order to capture the event of workflow graph starts executing.
 *
 * Components and Services subscribe to getExecuteEndedStream()
 *  for the event of the execution result (or errro) returned by the backend.
 *
 * @author Zuozhi Wang
 * @author Henry Chen
 */
@Injectable({
  providedIn: 'root'
})
export class ExecuteWorkflowService {

  private currentState: ExecutionStateInfo = { state: ExecutionState.Uninitialized };
  private executionStateStream = new Subject<{ previous: ExecutionStateInfo, current: ExecutionStateInfo }>();

  private executionTimeoutID: number | undefined;
  private clearTimeoutState: ExecutionState[] | undefined;

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private http: HttpClient
  ) {
    if (environment.amberEngineEnabled) {
      workflowWebsocketService.websocketEvent().subscribe(event => {
        if (event.type !== 'WorkflowStatusUpdateEvent') {
          console.log(event);
        }
        const newState = this.handleExecutionEvent(event);
        this.updateExecutionState(newState);
      });
    }
  }

  public handleExecutionEvent(event: TexeraWebsocketEvent): ExecutionStateInfo {
    switch (event.type) {
      case 'WorkflowStartedEvent':
        return { state: ExecutionState.Running };
      case 'WorkflowCompletedEvent':
        const resultMap = new Map(event.result.map(r => [r.operatorID, r]));
        return { state: ExecutionState.Completed, resultID: undefined, resultMap: resultMap };
      case 'WorkflowPausedEvent':
        if (this.currentState.state === ExecutionState.BreakpointTriggered ||
          this.currentState.state === ExecutionState.Paused) {
          return this.currentState;
        } else {
          return { state: ExecutionState.Paused, currentTuples: {} };
        }
      case 'RecoveryStartedEvent':
        return { state: ExecutionState.Recovering };
      case 'OperatorCurrentTuplesUpdateEvent':
        if (this.currentState.state === ExecutionState.BreakpointTriggered) {
          return this.currentState;
        }
        let pausedCurrentTuples: Readonly<Record<string, OperatorCurrentTuples>>;
        if (this.currentState.state === ExecutionState.Paused) {
          pausedCurrentTuples = this.currentState.currentTuples;
        } else {
          pausedCurrentTuples = {};
        }
        const currentTupleUpdate: Record<string, OperatorCurrentTuples> = {};
        currentTupleUpdate[event.operatorID] = event;
        const newCurrentTuples: Record<string, OperatorCurrentTuples> = {
          ...currentTupleUpdate,
          ...pausedCurrentTuples
        };
        return { state: ExecutionState.Paused, currentTuples: newCurrentTuples };
      case 'WorkflowResumedEvent':
        return { state: ExecutionState.Running };
      case 'BreakpointTriggeredEvent':
        return { state: ExecutionState.BreakpointTriggered, breakpoint: event };
      case 'WorkflowErrorEvent':
        const errorMessages: Record<string, string> = {};
        Object.entries(event.operatorErrors).forEach(entry => {
          errorMessages[entry[0]] = `${entry[1].propertyPath}: ${entry[1].message}`;
        });
        Object.entries(event.generalErrors).forEach(entry => {
          errorMessages[entry[0]] = entry[1];
        });
        return { state: ExecutionState.Failed, errorMessages: errorMessages };
      // TODO: Merge WorkflowErrorEvent and ErrorEvent
      case 'WorkflowExecutionErrorEvent':
        const backendErrorMessages: Record<string, string> = {};
        Object.entries(event.errorMap).forEach(entry => {
          backendErrorMessages[entry[0]] = entry[1];
        });
        return { state: ExecutionState.Failed, errorMessages: backendErrorMessages };
      default:
        return this.currentState;
    }
  }

  public getExecutionState(): ExecutionStateInfo {
    return this.currentState;
  }

  public getErrorMessages(): Readonly<Record<string, string>> | undefined {
    if (this.currentState?.state === ExecutionState.Failed) {
      return this.currentState.errorMessages;
    }
    return undefined;
  }

  /**
   * Return map which contains all sink operators execution result
   */
  public getResultMap(): ReadonlyMap<string, ResultObject> | undefined {
    if (this.currentState?.state === ExecutionState.Completed) {
      return this.currentState.resultMap;
    }
    return undefined;
  }

  public getBreakpointTriggerInfo(): BreakpointTriggerInfo | undefined {
    if (this.currentState?.state === ExecutionState.BreakpointTriggered) {
      return this.currentState.breakpoint;
    }
    return undefined;
  }

  public executeWorkflow(): void {
    if (environment.amberEngineEnabled) {
      this.executeWorkflowAmberTexera();
    } else {
      this.executeWorkflowOldTexera();
    }
  }

  public executeWorkflowAmberTexera(): void {
    // get the current workflow graph
    const logicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(
      this.workflowActionService.getTexeraGraph());
    console.log(logicalPlan);
    // wait for the form debounce to complete, then send
    window.setTimeout(() => {
      this.workflowWebsocketService.send('ExecuteWorkflowRequest', logicalPlan);
    }, FORM_DEBOUNCE_TIME_MS);
    this.updateExecutionState({ state: ExecutionState.WaitingToRun });
    this.setExecutionTimeout('submit workflow timeout', ExecutionState.Running, ExecutionState.Failed);

    // add flag for new execution of workflow
    // so when next time the result panel is displayed, it will use new data
    // instead of those stored in the session storage
    const resultPaginationInfo = sessionGetObject<ResultPaginationInfo>(PAGINATION_INFO_STORAGE_KEY);
    if (resultPaginationInfo) {
      sessionSetObject(PAGINATION_INFO_STORAGE_KEY, { ...resultPaginationInfo, newWorkflowExecuted: true });
    }
  }

  public pauseWorkflow(): void {
    if (!environment.pauseResumeEnabled || !environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState === undefined || this.currentState.state !== ExecutionState.Running) {
      throw new Error('cannot pause workflow, current execution state is ' + this.currentState.state);
    }
    this.workflowWebsocketService.send('PauseWorkflowRequest', {});
    this.updateExecutionState({ state: ExecutionState.Pausing });
    this.setExecutionTimeout('pause operation timeout', ExecutionState.Paused, ExecutionState.Failed);
  }

  public killWorkflow(): void {
    if (!environment.pauseResumeEnabled || !environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState.state === ExecutionState.Uninitialized || this.currentState.state === ExecutionState.Completed) {
      throw new Error('cannot kill workflow, current execution state is ' + this.currentState.state);
    }
    this.workflowWebsocketService.send('KillWorkflowRequest', {});
    this.updateExecutionState({ state: ExecutionState.Completed, resultID: undefined, resultMap: new Map() });
  }

  public resumeWorkflow(): void {
    if (!environment.pauseResumeEnabled || !environment.amberEngineEnabled) {
      return;
    }
    if (!(this.currentState.state === ExecutionState.Paused || this.currentState.state === ExecutionState.BreakpointTriggered)) {
      throw new Error('cannot resume workflow, current execution state is ' + this.currentState.state);
    }
    this.workflowWebsocketService.send('ResumeWorkflowRequest', {});
    this.updateExecutionState({ state: ExecutionState.Resuming });
    this.setExecutionTimeout('resume operation timeout', ExecutionState.Running, ExecutionState.Failed);
  }

  public addBreakpointRuntime(linkID: string, breakpointData: Breakpoint): void {
    if (!environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState.state !== ExecutionState.BreakpointTriggered &&
      this.currentState.state !== ExecutionState.Paused) {
      throw new Error('cannot add breakpoint at runtime, current execution state is ' + this.currentState.state);
    }
    console.log('sending add breakpoint request');
    this.workflowWebsocketService.send('AddBreakpointRequest',
      ExecuteWorkflowService.transformBreakpoint(this.workflowActionService.getTexeraGraph(), linkID, breakpointData));
  }

  public skipTuples(): void {
    if (!environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState.state !== ExecutionState.BreakpointTriggered) {
      throw new Error('cannot skip tuples, current execution state is ' + this.currentState.state);
    }
    this.currentState.breakpoint.report.forEach(fault => {
      this.workflowWebsocketService.send('SkipTupleRequest', { faultedTuple: fault.faultedTuple, actorPath: fault.actorPath });
    });
  }

  public changeOperatorLogic(operatorID: string): void {
    if (!environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState.state !== ExecutionState.BreakpointTriggered && this.currentState.state !== ExecutionState.Paused) {
      throw new Error('cannot modify logic, current execution state is ' + this.currentState.state);
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    const operator: LogicalOperator = {
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType
    };
    this.workflowWebsocketService.send('ModifyLogicRequest', { operator });
  }

  /**
   * Sends the current workflow data to the backend
   *  to execute the workflow and gets the results.
   *  return workflow id to be used by workflowStatusService
   */
  public executeWorkflowOldTexera(): void {
    throw new Error('no longer support executing workflow on old texera engine');
  }

  /**
   * Sends the finished workflow ID to the server to download the excel file using file saver library.
   * @param executionID
   */
  public downloadWorkflowExecutionResult(executionID: string, downloadType: string): void {
    const requestURL = `${AppSettings.getApiEndpoint()}/${DOWNLOAD_WORKFLOW_ENDPOINT}`
      + `?resultID=${executionID}&downloadType=${downloadType}`;

    this.http.get(
      requestURL,
      { responseType: 'blob' }
    ).subscribe(
      // response => saveAs(response, downloadName),
      () => window.location.href = requestURL,
      error => console.log(error)
    );
  }

  public getExecutionStateStream(): Observable<{ previous: ExecutionStateInfo, current: ExecutionStateInfo }> {
    return this.executionStateStream.asObservable();
  }

  private setExecutionTimeout(message: string, ...clearTimeoutState: ExecutionState[]) {
    if (this.executionTimeoutID !== undefined) {
      this.clearExecutionTimeout();
    }
    this.executionTimeoutID = window.setTimeout(() => {
      this.updateExecutionState({ state: ExecutionState.Failed, errorMessages: { 'timeout': message } });
    }, EXECUTION_TIMEOUT);
    this.clearTimeoutState = clearTimeoutState;
  }

  private clearExecutionTimeout() {
    if (this.executionTimeoutID !== undefined) {
      window.clearTimeout(this.executionTimeoutID);
      this.executionTimeoutID = undefined;
      this.clearTimeoutState = undefined;
    }
  }

  private updateExecutionState(stateInfo: ExecutionStateInfo): void {
    this.updateWorkflowActionLock(stateInfo);
    if (isEqual(this.currentState, stateInfo)) {
      return;
    }
    console.log(stateInfo);
    console.log(this.clearTimeoutState);
    console.log(this.clearTimeoutState?.includes(stateInfo.state));
    if (this.clearTimeoutState?.includes(stateInfo.state)) {
      this.clearExecutionTimeout();
    }
    const previousState = this.currentState;
    // update current state
    this.currentState = stateInfo;
    // emit event
    this.executionStateStream.next({ previous: previousState, current: this.currentState });
  }

  /**
   * enables or disables workflow action service based on execution state
   */
  private updateWorkflowActionLock(stateInfo: ExecutionStateInfo): void {
    switch (stateInfo.state) {
      case ExecutionState.Completed:
      case ExecutionState.Failed:
      case ExecutionState.Uninitialized:
        this.workflowActionService.enableWorkflowModification();
        return;
      case ExecutionState.Paused:
      case ExecutionState.Pausing:
      case ExecutionState.Recovering:
      case ExecutionState.Resuming:
      case ExecutionState.Running:
      case ExecutionState.WaitingToRun:
        this.workflowActionService.disableWorkflowModification();
        return;
      default:
        return;
    }
  }

  /**
   * Transform a workflowGraph object to the HTTP request body according to the backend API.
   *
   * All the operators in the workflowGraph will be transformed to LogicalOperator objects,
   *  where each operator has an operatorID and operatorType along with
   *  the properties of the operator.
   *
   * All the links in the workflowGraph will be tranformed to LogicalLink objects,
   *  where each link will store its source id as its origin and target id as its destination.
   *
   * @param workflowGraph
   */
  public static getLogicalPlanRequest(workflowGraph: WorkflowGraphReadonly): LogicalPlan {

    const getInputPortOrdinal = (operatorID: string, inputPortID: string): number => {
      return workflowGraph.getOperator(operatorID).inputPorts.findIndex(port => port.portID === inputPortID);
    };
    const getOutputPortOrdinal = (operatorID: string, outputPortID: string): number => {
      return workflowGraph.getOperator(operatorID).outputPorts.findIndex(port => port.portID === outputPortID);
    };

    const operators: LogicalOperator[] = workflowGraph
      .getAllOperators().map(op => ({
        ...op.operatorProperties,
        operatorID: op.operatorID,
        operatorType: op.operatorType
      }));

    const links: LogicalLink[] = workflowGraph
      .getAllLinks().map(link => ({
        origin: { operatorID: link.source.operatorID, portOrdinal: getOutputPortOrdinal(link.source.operatorID, link.source.portID) },
        destination: { operatorID: link.target.operatorID, portOrdinal: getInputPortOrdinal(link.target.operatorID, link.target.portID) },
      }));

    const breakpoints: BreakpointInfo[] = Array.from(workflowGraph.getAllLinkBreakpoints().entries())
      .map(e => ExecuteWorkflowService.transformBreakpoint(workflowGraph, e[0], e[1]));

    return { operators, links, breakpoints };
  }

  public static transformBreakpoint(
    workflowGraph: WorkflowGraphReadonly, linkID: string, breakpointData: Breakpoint): BreakpointInfo {
    const operatorID = workflowGraph.getLinkWithID(linkID).source.operatorID;
    let breakpoint: BreakpointRequest;
    if ('condition' in breakpointData) {
      breakpoint = { ...breakpointData, type: 'ConditionBreakpoint' };
    } else if ('count' in breakpointData) {
      breakpoint = { ...breakpointData, type: 'CountBreakpoint' };
    } else {
      throw new Error('unhandled breakpoint data ' + breakpointData);
    }
    return { operatorID, breakpoint };
  }

  public static isExecutionSuccessful(result: ExecutionResult | undefined): result is SuccessExecutionResult {
    return !!result && result.code === 0;
  }

  /**
   * Handles the HTTP Error response in different failure scenarios
   *  and converts to an ErrorExecutionResult object.
   * @param errorResponse
   */
  private static processErrorResponse(errorResponse: HttpErrorResponse): Record<string, string> {
    // client side error, such as no internet connection
    if (errorResponse.error instanceof ProgressEvent) {
      return { 'network error': 'Could not reach Texera server' };
    }
    // the workflow graph is invalid
    // error message from backend will be included in the error property
    if (errorResponse.status === 400) {
      const result = <ErrorExecutionResult>(errorResponse.error);
      return { 'workflow error': result.message };
    }
    // other kinds of server error
    return { 'server error': `Texera server error: ${errorResponse.error.message}` };
  }


}
