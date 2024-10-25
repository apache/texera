import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";
import { filter, map, switchMap } from "rxjs/operators";

/**
 * This service provides functionalities for debugging UDF operators.
 */
@Injectable({
  providedIn: "root",
})
export class UdfDebugService {
  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    // Initializes debug handlers for all operators in the workflow graph.
    const graph = this.workflowActionService.getTexeraGraph();
    graph.getAllOperators().forEach(op => {
      graph.createOperatorDebugState(op.operatorID);
      this.registerOperatorStateChangeHandler(op.operatorID);
      this.registerConsoleUpdateHandler(op.operatorID);
    });
  }

  /**
   * Retrieves the debug state for a specific operator.
   *
   * @param operatorId - The unique ID of the operator.
   * @returns A Y.Map containing the operator's debug state.
   */
  getDebugState(operatorId: string) {
    return this.workflowActionService.getTexeraGraph().getOperatorDebugState(operatorId);
  }

  /**
   * Gets the condition of a breakpoint for a specific line.
   *
   * @param operatorId - The unique ID of the operator.
   * @param lineNumber - The line number where the breakpoint is set.
   * @returns The condition string for the breakpoint.
   */
  getCondition(operatorId: string, lineNumber: number): string {
    const line = String(lineNumber);
    const debugState = this.getDebugState(operatorId);
    return debugState.has(line) ? debugState.get(line)!.condition : "";
  }

  /**
   * Updates the condition of a breakpoint if it differs from the existing one.
   *
   * @param operatorId - The unique ID of the operator.
   * @param lineNumber - The line number where the breakpoint is set.
   * @param condition - The new condition to be applied to the breakpoint.
   */
  doUpdateBreakpointCondition(operatorId: string, lineNumber: number, condition: string) {
    if (condition === this.getCondition(operatorId, lineNumber)) return;

    const workerIds = this.executeWorkflowService.getWorkerIds(operatorId);
    const debugState = this.getDebugState(operatorId);
    const breakpointInfo = debugState.get(String(lineNumber));

    if (isDefined(breakpointInfo)) {
      workerIds.forEach(workerId => {
        this.workflowWebsocketService.send("DebugCommandRequest", {
          operatorId,
          workerId,
          cmd: `condition ${breakpointInfo.breakpointId} ${condition}`,
        });
      });
      debugState.set(String(lineNumber), { ...breakpointInfo, condition });
    }
  }

  /**
   * Adds or removes a breakpoint based on its existence.
   *
   * @param operatorId - The unique ID of the operator.
   * @param lineNumber - The line number to add or remove the breakpoint from.
   */
  doModifyBreakpoint(operatorId: string, lineNumber: number) {
    const workerIds = this.executeWorkflowService.getWorkerIds(operatorId);
    const debugState = this.getDebugState(operatorId);
    const cmd = debugState.has(String(lineNumber)) ? "clear" : "break";
    const breakpointId = debugState.get(String(lineNumber))?.breakpointId || "";

    workerIds.forEach(workerId => {
      this.workflowWebsocketService.send("DebugCommandRequest", {
        operatorId,
        workerId,
        cmd: `${cmd} ${cmd === "clear" ? breakpointId : lineNumber}`,
      });
    });
  }

  /**
   * Continues the execution by resetting the temporary breakpoints.
   *
   * @param operatorId - The unique ID of the operator.
   * @param workerId - The ID of the worker to continue execution on.
   */
  doContinue(operatorId: string, workerId: string) {
    this.markContinue(operatorId);
    this.workflowWebsocketService.send("DebugCommandRequest", {
      operatorId,
      workerId,
      cmd: "continue",
    });
  }

  /**
   * Steps through the execution.
   *
   * @param operatorId - The unique ID of the operator.
   * @param workerId - The ID of the worker to step execution on.
   */
  doStep(operatorId: string, workerId: string) {
    this.markContinue(operatorId);
    this.workflowWebsocketService.send("DebugCommandRequest", {
      operatorId,
      workerId,
      cmd: "next",
    });
  }

  /**
   * Registers a handler for state changes of an operator.
   *
   * @param operatorId - The unique ID of the operator.
   */
  private registerOperatorStateChangeHandler(operatorId: string) {
    this.workflowStatusService
      .getStatusUpdateStream()
      .pipe(filter(event => event[operatorId]?.operatorState === OperatorState.Uninitialized))
      .subscribe(() => this.getDebugState(operatorId).clear());
  }

  /**
   * Registers console update handlers for an operator.
   *
   * @param operatorId - The unique ID of the operator.
   */
  private registerConsoleUpdateHandler(operatorId: string) {
    const debugMessageStream = this.workflowWebsocketService.subscribeToEvent("ConsoleUpdateEvent").pipe(
      filter(evt => evt.operatorId === operatorId && evt.messages.length > 0),
      switchMap(evt => evt.messages),
      filter(msg => msg.source === "(Pdb)" && msg.msgType.name === "DEBUGGER")
    );

    // Handle stepping message.
    // Example:
    //   > /path/to/file.py(10)<module>()
    debugMessageStream
      .pipe(
        filter(msg => msg.title.startsWith(">")),
        map(msg => this.extractInfo(msg.title))
      )
      .subscribe(({ lineNum }) => {
        if (!isDefined(lineNum)) return;
        this.markBreakpointAsHit(operatorId, lineNum);
      });

    // Handle breakpoint creation message.
    // Example:
    //   Breakpoint 1 at /path/to/file.py:10
    debugMessageStream
      .pipe(
        filter(msg => msg.title.startsWith("Breakpoint")),
        map(msg => this.extractInfo(msg.title))
      )
      .subscribe(({ breakpointId, lineNum }) => {
        if (isDefined(breakpointId) && isDefined(lineNum)) {
          this.getDebugState(operatorId).set(String(lineNum), {
            breakpointId,
            condition: "",
            hit: false,
          });
        }

        this.continueIfNotHittingBreakpoint(operatorId);
      });

    // Handle breakpoint deletion message.
    // Example:
    //   Deleted breakpoint 1 at /path/to/file.py:10
    debugMessageStream
      .pipe(
        filter(msg => msg.title.startsWith("Deleted")),
        map(msg => this.extractInfo(msg.title))
      )
      .subscribe(({ lineNum }) => {
        if (!isDefined(lineNum)) {
          return;
        }
        const debugState = this.getDebugState(operatorId);
        if (!debugState.has(String(lineNum))) {
          return;
        }
        const breakpointInfo = debugState.get(String(lineNum))!;
        debugState.delete(String(lineNum));

        // if the breakpoint was hit, we need to keep it in the debug state
        if (breakpointInfo.hit) {
          debugState.set(String(lineNum), { ...breakpointInfo, breakpointId: undefined });
        }

        this.continueIfNotHittingBreakpoint(operatorId);
      });

    // Handle breakpoint blank message.
    // Example:
    //   *** Blank or comment
    debugMessageStream.pipe(filter(msg => msg.title.startsWith("*** Blank or comment"))).subscribe(() => {
      this.continueIfNotHittingBreakpoint(operatorId);
    });
  }

  /**
   * Marks a breakpoint as hit, creating a temporary one if needed.
   *
   * @param operatorId - The unique ID of the operator.
   * @param lineNum - The line number of the breakpoint to mark as hit.
   */
  private markBreakpointAsHit(operatorId: string, lineNum: number) {
    const line = String(lineNum);
    const debugState = this.getDebugState(operatorId);
    if (!debugState.has(line)) {
      debugState.set(line, { breakpointId: undefined, condition: "", hit: false });
    }
    const breakpoint = debugState.get(line)!;
    debugState.set(line, { ...breakpoint, hit: true });
  }

  /**
   * Resets hit status and removes temporary breakpoints.
   *
   * @param operatorId - The unique ID of the operator.
   */
  private markContinue(operatorId: string) {
    const debugState = this.getDebugState(operatorId);
    debugState.forEach((value, key) => {
      if (value.hit) debugState.set(key, { ...value, hit: false });
      if (!value.breakpointId) debugState.delete(key);
    });
  }

  /**
   * Extracts breakpoint information from a message.
   *
   * @param message - The message string containing breakpoint information.
   * @returns An object containing breakpointId and lineNum.
   */
  private extractInfo(message: string): { breakpointId?: number; lineNum?: number } {
    const match = message.match(/(?:Breakpoint|Deleted breakpoint) (\d+) at .+:(\d+)/);
    if (match) return { breakpointId: parseInt(match[1], 10), lineNum: parseInt(match[2], 10) };

    const lineMatch = message.match(/\.py\((\d+)\)|:(\d+)/);
    if (lineMatch) return { lineNum: parseInt(lineMatch[1] || lineMatch[2], 10) };

    return {};
  }

  /**
   * Checks if any breakpoint is currently hit (paused) in the debug state
   * for the given operator.
   *
   * @param {string} operatorId - The unique ID of the operator.
   * @returns {boolean} - Returns `true` if any breakpoint is hit, otherwise `false`.
   */
  private isHittingBreakpoint(operatorId: string): boolean {
    const debugState = this.getDebugState(operatorId);
    return Array.from(debugState.values()).some(breakpoint => breakpoint.hit);
  }

  /**
   * Sends a "continue" command to all workers of the specified operator
   * if no breakpoints are currently hit. If a breakpoint is hit, the
   * function exits without sending the "continue" command.
   *
   * @param {string} operatorId - The unique ID of the operator.
   */
  private continueIfNotHittingBreakpoint(operatorId: string): void {
    // If any breakpoint is hit, do not send the "continue" command.
    if (this.isHittingBreakpoint(operatorId)) {
      return;
    }

    // Retrieve all worker IDs and send the "continue" command to each worker.
    this.executeWorkflowService.getWorkerIds(operatorId).forEach(workerId => this.doContinue(operatorId, workerId));
  }
}
