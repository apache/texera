import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import {
  LogicalLink, LogicalPlan, LogicalOperator,
  ExecutionResult, ErrorExecutionResult, SuccessExecutionResult, BreakpointInfo, ExecutionState, ExecutionStateInfo
} from '../../types/execute-workflow.interface';
import { ResultObject } from '../../types/execute-workflow.interface';
import { v4 as uuid } from 'uuid';
import { environment } from '../../../../environments/environment';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { OperatorPredicate, BreakpointTriggerInfo, BreakpointRequest } from '../../types/workflow-common.interface';
import { TexeraWebsocketEvent, WorkerTuples, OperatorCurrentTuples } from '../../types/workflow-websocket.interface';
import { isEqual } from 'lodash';

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
@Injectable()
export class ExecuteWorkflowService {

  private currentState: ExecutionStateInfo = { state: ExecutionState.Uninitialized };
  // private resultMap: Map<string, ResultObject> = new Map<string, ResultObject>();
  // private breakpoint: BreakpointTriggerInfo | undefined;
  // private errorMessages: Record<string, string> | undefined;
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
      case 'OperatorCurrentTuplesUpdateEvent':
        console.log(event);
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
        console.log(event);
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
    const workflowPlan = this.workflowActionService.getTexeraGraph();
    const logicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);
    console.log(logicalPlan);
    // wait for the form debounce to complete, then send
    window.setTimeout(() => {
      this.workflowWebsocketService.send('ExecuteWorkflowRequest', logicalPlan);
    }, FORM_DEBOUNCE_TIME_MS);
    this.updateExecutionState({ state: ExecutionState.WaitingToRun });
    this.setExecutionTimeout('submit workflow timeout', ExecutionState.Running, ExecutionState.Failed);
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
    if (this.currentState.state !== ExecutionState.BreakpointTriggered) {
      throw new Error('cannot update tuples, current execution state is ' + this.currentState.state);
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    const operator: LogicalOperator = {
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType
    };
    this.workflowWebsocketService.send('ModifyLogicRequest', { operator });
  }

  public modifyOperatorLogic(op: OperatorPredicate): void {
    if (!environment.pauseResumeEnabled || !environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState !== undefined
      || !(this.currentState === ExecutionState.Paused)) {
      throw new Error('cannot resume workflow, current execution state is ' + this.currentState);
    }
    const logicalOperator: LogicalOperator = {
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType
    };
    this.workflowWebsocketService.send('ModifyLogicRequest', { operator: logicalOperator });
  }


  /**
   * Sends the current workflow data to the backend
   *  to execute the workflow and gets the results.
   *  return workflow id to be used by workflowStatusService
   */
  public executeWorkflowOldTexera(): void {
    // get the current workflow graph
    const workflowPlan = this.workflowActionService.getTexeraGraph();

    // create a Logical Plan based on the workflow graph
    const logicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);
    const body = { operators: logicalPlan.operators, links: logicalPlan.links };
    const requestURL = `${AppSettings.getApiEndpoint()}/${EXECUTE_WORKFLOW_ENDPOINT}`;

    this.updateExecutionState({ state: ExecutionState.Running });

    // make a http post request to the API endpoint with the logical plan object
    this.http.post<SuccessExecutionResult>(
      requestURL,
      JSON.stringify(body),
      { headers: { 'Content-Type': 'application/json' } })
      .subscribe(
        // backend will either respond an execution result or an error will occur
        // handle both cases
        response => {
          const resultMap = new Map<string, ResultObject>(response.result.map(r => [r.operatorID, r]));
          this.updateExecutionState({ state: ExecutionState.Completed, resultID: undefined, resultMap: resultMap });
        },
        errorResponse => {
          const errorMessages = ExecuteWorkflowService.processErrorResponse(errorResponse);
          this.updateExecutionState({ state: ExecutionState.Failed, errorMessages: errorMessages });
        }
      );

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

    const operators: LogicalOperator[] = workflowGraph
      .getAllOperators().map(op => ({
        ...op.operatorProperties,
        operatorID: op.operatorID,
        operatorType: op.operatorType
      }));

    const links: LogicalLink[] = workflowGraph
      .getAllLinks().map(link => ({
        origin: link.source.operatorID,
        destination: link.target.operatorID,
      }));

    const breakpoints: BreakpointInfo[] = Array.from(workflowGraph.getAllLinkBreakpoints().entries())
      .map(e => {
        const operatorID = workflowGraph.getLinkWithID(e[0]).source.operatorID;
        const breakpointData = e[1];
        let breakpoint: BreakpointRequest;
        if ('condition' in breakpointData) {
          breakpoint = { ...breakpointData, type: 'ConditionBreakpoint' };
        } else if ('count' in breakpointData) {
          breakpoint = { ...breakpointData, type: 'CountBreakpoint' };
        } else {
          throw new Error('unhandled breakpoint data ' + breakpointData);
        }
        return { operatorID, breakpoint };
      });

    return { operators, links, breakpoints };
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
