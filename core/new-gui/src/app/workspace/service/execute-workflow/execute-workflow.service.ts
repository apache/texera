import { Injectable } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';

import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import './../../../common/rxjs-operators';
import { AppSettings } from './../../../common/app-setting';

import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import {
  LogicalLink, LogicalPlan, LogicalOperator,
  ExecutionResult, ErrorExecutionResult, SuccessExecutionResult
} from '../../types/execute-workflow.interface';

export const EXECUTE_WORKFLOW_ENDPOINT = 'queryplan/execute';


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

  private executeStartedStream = new Subject<string>();
  private executeEndedStream = new Subject<ExecutionResult>();

  constructor(private workflowActionService: WorkflowActionService, private http: HttpClient) { }

  /**
   * Sends the current workflow data to the backend
   *  to execute the workflow and gets the results.
   *
   */
  public executeWorkflow(): void {
    // get the current workflow graph
    const workflowPlan = this.workflowActionService.getTexeraGraph();

    // create a Logical Plan based on the workflow graph
    const body = ExecuteWorkflowService.getLogicalPlanRequest(workflowPlan);
    const requestURL = `${AppSettings.getApiEndpoint()}/${EXECUTE_WORKFLOW_ENDPOINT}`;

    this.executeStartedStream.next('execution started');

    // make a http post request to the API endpoint with the logical plan object
    this.http.post<SuccessExecutionResult>(
      requestURL,
      JSON.stringify(body),
      { headers: { 'Content-Type': 'application/json' } })
      .subscribe(
        // backend will either respond an execution result or an error will occur
        // handle both cases
        response => this.handleExecuteResult(response),
        errorResponse => this.handleExecuteError(errorResponse)
      );
  }

  /**
   * Gets the observable for execution started event
   * Contains a string that says:
   *  - execution process has begun
   */
  public getExecuteStartedStream(): Observable<string> {
    return this.executeStartedStream.asObservable();
  }

  /**
   * Gets the observable for execution ended event
   * If execution succeeded, it contains an object with type
   *  `SuccessExecutionResult`:
   *    -  resultID: the result ID of this execution
   *    -  Code: the result code of 0
   *    -  result: the actual result data to be displayed
   *
   * If execution succeeded, it contains an object with type
   *  `ErrorExecutionResult`:
   *    -  Code: the result code 1
   *    -  message: error message
   */
  public getExecuteEndedStream(): Observable<ExecutionResult> {
    return this.executeEndedStream.asObservable();
  }

  /**
   * Handles valid execution result from the backend.
   * Sends the execution result to the execution end event stream.
   *
   * @param response
   */
  private handleExecuteResult(response: SuccessExecutionResult): void {
    this.executeEndedStream.next(response);
  }

  /**
   * Handler function for invalid execution.
   *
   * Send the error messages generated from
   *  backend (if workflow is invalid or server error)
   *  or frontend (if there's no network connection)
   *  to the execution end event stream.
   *
   * @param errorResponse
   */
  private handleExecuteError(errorResponse: HttpErrorResponse): void {
    // error shown to the user in different error scenarios
    const displayedErrorMessage = ExecuteWorkflowService.processErrorResponse(errorResponse);
    this.executeEndedStream.next(displayedErrorMessage);
  }

  /**
   * Transform a workflowGraph object to the HTTP request body according to the backend API.
   *
   * All the operators in the workflowGraph will be transformed to LogicalOperator objects,
   *  where each operator has an operatorID and operatorType along with
   *  the properties of the operator.
   *
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
        destination: link.target.operatorID
      }));

    return { operators, links };
  }

  public static isExecutionSuccessful(result: ExecutionResult | undefined): result is SuccessExecutionResult {
    return !!result && result.code === 0;
  }

  /**
   * Handles the HTTP Error response in different failure scenarios
   *  and converts to an ErrorExecutionResult object.
   * @param errorResponse
   */
  private static processErrorResponse(errorResponse: HttpErrorResponse): ErrorExecutionResult {
    // client side error, such as no internet connection
    if (errorResponse.error instanceof ProgressEvent) {
      return {
        code: 1,
        message: 'Could not reach Texera server'
      };
    }
    // the workflow graph is invalid
    // error message from backend will be included in the error property
    if (errorResponse.status === 400) {
      return <ErrorExecutionResult>(errorResponse.error);
    }
    // other kinds of server error
    return {
      code: 1,
      message: `Texera server error: ${errorResponse.error.message}`
    };
  }


}
