import { Injectable } from "@angular/core";
import { from, Observable, Subject } from "rxjs";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowGraphReadonly } from "../workflow-graph/model/workflow-graph";
import {
  ExecutionState,
  ExecutionStateInfo,
  LogicalLink,
  LogicalOperator,
  LogicalPlan,
} from "../../types/execute-workflow.interface";
import { environment } from "../../../../environments/environment";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import {
  WorkflowFatalError,
  OperatorCurrentTuples,
  TexeraWebsocketEvent,
  ReplayExecutionInfo,
} from "../../types/workflow-websocket.interface";
import { isEqual } from "lodash-es";
import { PAGINATION_INFO_STORAGE_KEY, ResultPaginationInfo } from "../../types/result-table.interface";
import { sessionGetObject, sessionSetObject } from "../../../common/util/storage";
import { Version as version } from "src/environments/version";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { exhaustiveGuard } from "../../../common/util/switch";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";

// TODO: change this declaration
export const FORM_DEBOUNCE_TIME_MS = 150;

export const EXECUTE_WORKFLOW_ENDPOINT = "queryplan/execute";

export const PAUSE_WORKFLOW_ENDPOINT = "pause";
export const RESUME_WORKFLOW_ENDPOINT = "resume";

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
  providedIn: "root",
})
export class ExecuteWorkflowService {
  private currentState: ExecutionStateInfo = {
    state: ExecutionState.Uninitialized,
  };
  private executionStateStream = new Subject<{
    previous: ExecutionStateInfo;
    current: ExecutionStateInfo;
  }>();

  // TODO: move this to another service, or redesign how this
  //   information is stored on the frontend.
  private assignedWorkerIds: Map<string, readonly string[]> = new Map();

  constructor(
    private workflowActionService: WorkflowActionService,
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private notificationService: NotificationService
  ) {
    workflowWebsocketService.websocketEvent().subscribe(event => {
      switch (event.type) {
        case "WorkerAssignmentUpdateEvent":
          this.assignedWorkerIds.set(event.operatorId, event.workerIds);
          break;
        default:
          // workflow status related event
          this.handleReconfigurationEvent(event);
          const newState = this.handleExecutionEvent(event);
          if (newState !== undefined) {
            this.updateExecutionState(newState);
          }
      }
    });
  }

  public handleReconfigurationEvent(event: TexeraWebsocketEvent) {
    switch (event.type) {
      case "ModifyLogicResponse":
        if (!event.isValid) {
          this.notificationService.error(event.errorMessage);
        } else {
          this.notificationService.info("reconfiguration registered");
        }
        return;
      case "ModifyLogicCompletedEvent":
        this.notificationService.info("reconfiguration on operator(s) " + event.opIds + " complete");
    }
  }

  public handleExecutionEvent(event: TexeraWebsocketEvent): ExecutionStateInfo | undefined {
    switch (event.type) {
      case "WorkflowStateEvent":
        let newState = ExecutionState[event.state];
        switch (newState) {
          case ExecutionState.Paused:
            if (this.currentState.state === ExecutionState.Paused) {
              return this.currentState;
            } else {
              return { state: ExecutionState.Paused, currentTuples: {} };
            }
          case ExecutionState.Failed:
            // for failed state, backend will send an additional message after this status event.
            return undefined;
          default:
            return { state: newState };
        }
      case "RecoveryStartedEvent":
        return { state: ExecutionState.Recovering };
      case "OperatorCurrentTuplesUpdateEvent":
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
          ...pausedCurrentTuples,
        };
        return {
          state: ExecutionState.Paused,
          currentTuples: newCurrentTuples,
        };
      case "WorkflowErrorEvent":
        return {
          state: ExecutionState.Failed,
          errorMessages: event.fatalErrors.map(err => {
            return { ...err, message: err.message.replace("\\n", "<br>") };
          }),
        };
      default:
        return undefined;
    }
  }

  public getExecutionState(): ExecutionStateInfo {
    return this.currentState;
  }

  public getErrorMessages(): ReadonlyArray<WorkflowFatalError> {
    if (this.currentState?.state === ExecutionState.Failed) {
      return this.currentState.errorMessages;
    }
    return [];
  }

  public executeWorkflow(executionName: string): void {
    const logicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph());
    this.resetExecutionState();
    this.workflowStatusService.resetStatus();
    this.sendExecutionRequest(executionName, logicalPlan);
  }

  public executeWorkflowWithReplay(replayExecutionInfo: ReplayExecutionInfo): void {
    const logicalPlan = ExecuteWorkflowService.getLogicalPlanRequest(this.workflowActionService.getTexeraGraph());
    this.resetExecutionState();
    this.workflowStatusService.resetStatus();
    this.sendExecutionRequest(
      `Replay run of ${replayExecutionInfo.eid} to ${replayExecutionInfo.interaction}`,
      logicalPlan,
      replayExecutionInfo
    );
  }

  public sendExecutionRequest(
    executionName: string,
    logicalPlan: LogicalPlan,
    replayExecutionInfo: ReplayExecutionInfo | undefined = undefined
  ): void {
    const workflowExecuteRequest = {
      executionName: executionName,
      engineVersion: version.hash,
      logicalPlan: logicalPlan,
      replayFromExecution: replayExecutionInfo,
    };
    // wait for the form debounce to complete, then send
    window.setTimeout(() => {
      this.workflowWebsocketService.send("WorkflowExecuteRequest", workflowExecuteRequest);
    }, FORM_DEBOUNCE_TIME_MS);

    // add flag for new execution of workflow
    // so when next time the result panel is displayed, it will use new data
    // instead of those stored in the session storage
    const resultPaginationInfo = sessionGetObject<ResultPaginationInfo>(PAGINATION_INFO_STORAGE_KEY);
    if (resultPaginationInfo) {
      sessionSetObject(PAGINATION_INFO_STORAGE_KEY, {
        ...resultPaginationInfo,
        newWorkflowExecuted: true,
      });
    }
  }

  public pauseWorkflow(): void {
    if (this.currentState === undefined || this.currentState.state !== ExecutionState.Running) {
      throw new Error("cannot pause workflow, the current execution state is " + this.currentState?.state);
    }
    this.workflowWebsocketService.send("WorkflowPauseRequest", {});
  }

  public killWorkflow(): void {
    if (
      this.currentState.state === ExecutionState.Uninitialized ||
      this.currentState.state === ExecutionState.Completed
    ) {
      throw new Error("cannot kill workflow, the current execution state is " + this.currentState.state);
    }
    this.workflowWebsocketService.send("WorkflowKillRequest", {});
  }

  public takeGlobalCheckpoint(): void {
    if (
      this.currentState.state === ExecutionState.Uninitialized ||
      this.currentState.state === ExecutionState.Completed
    ) {
      throw new Error("cannot take checkpoint, the current execution state is " + this.currentState.state);
    }
    this.workflowWebsocketService.send("WorkflowCheckpointRequest", {});
  }

  public resumeWorkflow(): void {
    if (this.currentState.state !== ExecutionState.Paused) {
      throw new Error("cannot resume workflow, the current execution state is " + this.currentState.state);
    }
    this.workflowWebsocketService.send("WorkflowResumeRequest", {});
  }

  public skipTuples(workers: ReadonlyArray<string>): void {
    if (this.currentState.state !== ExecutionState.Paused) {
      throw new Error("cannot skip tuples, the current execution state is " + this.currentState.state);
    }
    this.workflowWebsocketService.send("SkipTupleRequest", { workers });
  }

  public retryExecution(workers: ReadonlyArray<string>): void {
    if (this.currentState.state !== ExecutionState.Paused) {
      throw new Error("cannot retry the current tuple, the current execution state is " + this.currentState.state);
    }
    this.workflowWebsocketService.send("RetryRequest", { workers });
  }

  public modifyOperatorLogic(operatorID: string): void {
    if (this.currentState.state !== ExecutionState.Paused) {
      throw new Error("cannot modify logic, the current execution state is " + this.currentState.state);
    }
    const op = this.workflowActionService.getTexeraGraph().getOperator(operatorID);
    const operator: LogicalOperator = {
      ...op.operatorProperties,
      operatorID: op.operatorID,
      operatorType: op.operatorType,
    };
    this.workflowWebsocketService.send("ModifyLogicRequest", { operator });
  }

  public getExecutionStateStream(): Observable<{
    previous: ExecutionStateInfo;
    current: ExecutionStateInfo;
  }> {
    return this.executionStateStream.asObservable();
  }

  public resetExecutionState(): void {
    this.currentState = {
      state: ExecutionState.Uninitialized,
    };
  }

  private updateExecutionState(stateInfo: ExecutionStateInfo): void {
    if (isEqual(this.currentState, stateInfo)) {
      return;
    }
    this.updateWorkflowActionLock(stateInfo);
    const previousState = this.currentState;
    // update current state
    this.currentState = stateInfo;
    // emit event
    this.executionStateStream.next({
      previous: previousState,
      current: this.currentState,
    });
  }

  /**
   * enables or disables workflow action service based on execution state
   */
  private updateWorkflowActionLock(stateInfo: ExecutionStateInfo): void {
    switch (stateInfo.state) {
      case ExecutionState.Completed:
      case ExecutionState.Failed:
      case ExecutionState.Uninitialized:
      case ExecutionState.Killed:
        this.workflowActionService.enableWorkflowModification();
        return;
      case ExecutionState.Paused:
      case ExecutionState.Pausing:
      case ExecutionState.Recovering:
      case ExecutionState.Resuming:
      case ExecutionState.Running:
      case ExecutionState.Initializing:
        this.workflowActionService.disableWorkflowModification();
        return;
      default:
        return exhaustiveGuard(stateInfo);
    }
  }

  /**
   * Transform a workflowGraph object to the HTTP request body according to the backend API.
   *
   * All the operators in the workflowGraph will be transformed to LogicalOperator objects,
   *  where each operator has an operatorID and operatorType along with
   *  the properties of the operator.
   *
   * All the links in the workflowGraph will be transformed to LogicalLink objects,
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

    const operators: LogicalOperator[] = workflowGraph.getAllEnabledOperators().map(op => {
      let logicalOp: LogicalOperator = {
        ...op.operatorProperties,
        operatorID: op.operatorID,
        operatorType: op.operatorType,
      };
      logicalOp = {
        ...logicalOp,
        inputPorts: op.inputPorts,
      };
      logicalOp = {
        ...logicalOp,
        outputPorts: op.outputPorts,
      };
      return logicalOp;
    });

    const links: LogicalLink[] = workflowGraph.getAllEnabledLinks().map(link => {
      const outputPortIdx = getOutputPortOrdinal(link.source.operatorID, link.source.portID);
      const inputPortIdx = getInputPortOrdinal(link.target.operatorID, link.target.portID);
      return {
        fromOpId: link.source.operatorID,
        fromPortId: { id: outputPortIdx, internal: false },
        toOpId: link.target.operatorID,
        toPortId: { id: inputPortIdx, internal: false },
      };
    });

    const opsToViewResult: string[] = Array.from(workflowGraph.getOperatorsToViewResult()).filter(
      op => !workflowGraph.isOperatorDisabled(op)
    );

    const opsToReuseResult: string[] = Array.from(workflowGraph.getOperatorsMarkedForReuseResult()).filter(
      op => !workflowGraph.isOperatorDisabled(op)
    );
    return { operators, links, opsToViewResult, opsToReuseResult };
  }

  public getWorkerIds(operatorId: string): ReadonlyArray<string> {
    return this.assignedWorkerIds.get(operatorId) || [];
  }
}
