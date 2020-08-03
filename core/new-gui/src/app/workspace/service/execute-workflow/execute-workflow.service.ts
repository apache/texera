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

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';

export const DOWNLOAD_WORKFLOW_ENDPOINT = 'download/result';
export const PAUSE_WORKFLOW_ENDPOINT = 'pause';
export const RESUME_WORKFLOW_ENDPOINT = 'resume';

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

  private currentState: ExecutionState | undefined;
  private resultMap: Map<string, ResultObject> = new Map<string, ResultObject>();
  private breakpoint: BreakpointTriggerInfo | undefined;
  private errorMessages: Record<string, string> | undefined;
  private executionStateStream = new Subject<ExecutionStateInfo>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private http: HttpClient
  ) {
    if (environment.amberEngineEnabled) {
      workflowWebsocketService.websocketEvent().subscribe(event => {
        if (event.type === 'WorkflowCompletedEvent') {
          const successResult: ExecutionResult = {
            code: 0,
            result: event.result,
            resultID: '0'
          };
          this.updateResultMap(successResult);
          this.updateExecutionState({ state: ExecutionState.Completed, resultID: undefined, resultMap: this.resultMap });
        } else if (event.type === 'WorkflowPausedEvent') {
          this.updateExecutionState({ state: ExecutionState.Paused });
        } else if (event.type === 'BreakpointTriggeredEvent') {
          this.breakpoint = event;
          this.updateExecutionState({ state: ExecutionState.BreakpointTriggered });
        } else if (event.type === 'WorkflowCompilationErrorEvent') {
          const errorMessages: Record<string, string> = {};
          Object.entries(event.violations).forEach(entry => {
            errorMessages[entry[0]] = `${entry[1].propertyPath}: ${entry[1].message}`;
          });
          this.errorMessages = errorMessages;
          this.updateExecutionState( { state: ExecutionState.Failed, errorMessages: errorMessages});
        }
      });
    }
  }

  public getExecutionState(): ExecutionState | undefined {
    return this.currentState;
  }

  public getErrorMessages(): Readonly<Record<string, string>> | undefined {
    return this.errorMessages;
  }

  /**
   * Return map which contains all sink operators execution result
   */
  public getResultMap(): ReadonlyMap<string, ResultObject> {
    return this.resultMap;
  }

  public getBreakpointTriggerInfo(): BreakpointTriggerInfo | undefined {
    return this.breakpoint;
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
    this.workflowWebsocketService.send('ExecuteWorkflowRequest', logicalPlan);
    this.updateExecutionState({ state: ExecutionState.Running });
  }

  public pauseWorkflow(): void {
    if (!environment.pauseResumeEnabled || !environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState === undefined || this.currentState !== ExecutionState.Running) {
      throw new Error('cannot pause workflow, current execution state is ' + this.currentState);
    }
    this.workflowWebsocketService.send('PauseWorkflowRequest', {});
    this.updateExecutionState({ state: ExecutionState.Pausing });
  }

  public resumeWorkflow(): void {
    if (!environment.pauseResumeEnabled || !environment.amberEngineEnabled) {
      return;
    }
    if (this.currentState === undefined || this.currentState !== ExecutionState.Paused) {
      throw new Error('cannot resume workflow, current execution state is ' + this.currentState);
    }
    this.workflowWebsocketService.send('ResumeWorkflowRequest', {});
    this.updateExecutionState({ state: ExecutionState.Running });
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
    this.workflowWebsocketService.send('ModifyLogicRequest', {operator: logicalOperator});
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
    const body = {operators: logicalPlan.operators, links: logicalPlan.links};
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
          this.updateResultMap(response);
          this.updateExecutionState({ state: ExecutionState.Completed, resultID: undefined, resultMap: this.resultMap });
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

  public getExecutionStateStream(): Observable<ExecutionStateInfo> {
    return this.executionStateStream.asObservable();
  }

  private updateExecutionState(stateInfo: ExecutionStateInfo): void {
    // update current state
    this.currentState = stateInfo.state;
    // clear breakpoint info if execution state changes
    if (this.currentState !== ExecutionState.BreakpointTriggered) {
      this.breakpoint = undefined;
    }
    console.log(this.currentState);
    // emit event
    this.executionStateStream.next(stateInfo);
  }

  /**
   * Update result map when new execution is returned.
   * @param response
   */
  private updateResultMap(response: SuccessExecutionResult): void {
    this.resultMap.clear();
    for (const item of response.result) {
      this.resultMap.set(item.operatorID, item);
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
          breakpoint = {...breakpointData, type: 'ConditionBreakpoint'};
        } else if ('count' in breakpointData) {
          breakpoint = {...breakpointData, type: 'CountBreakpoint'};
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
      return {'network error': 'Could not reach Texera server'};
    }
    // the workflow graph is invalid
    // error message from backend will be included in the error property
    if (errorResponse.status === 400) {
      const result = <ErrorExecutionResult>(errorResponse.error);
      return {'workflow error': result.message};
    }
    // other kinds of server error
    return {'server error': `Texera server error: ${errorResponse.error.message}`};
  }


}
