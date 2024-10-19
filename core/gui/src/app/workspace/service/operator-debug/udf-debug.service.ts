import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { DebugCommandRequest } from "../../types/workflow-websocket.interface";
import { OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";

export class BreakpointManager {
  public getDebugState() {
    return this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId);
  }

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private workflowActionService: WorkflowActionService,
    private currentOperatorId: string,
  ) {
    // initialize debug state if not created already
    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(currentOperatorId);

    workflowStatusService.getStatusUpdateStream().subscribe(event => {
      if (
        event[this.currentOperatorId]?.operatorState !== OperatorState.Running ||
        event[this.currentOperatorId]?.operatorState !== OperatorState.Paused
      ) {
        this.executionActive = true;
      }
      if (event[this.currentOperatorId]?.operatorState === OperatorState.Uninitialized) {
        this.resetState();
      }
    });

    workflowWebsocketService.subscribeToEvent("ConsoleUpdateEvent").subscribe(evt => {
      if (evt.messages.length === 0) {
        return;
      }

      evt.messages.forEach(msg => {
        if (msg.source == "(Pdb)" && msg.msgType.name == "DEBUGGER") {
          console.log("received ", msg.title);
          if (msg.title.startsWith(">")) {
            const { lineNum } = this.extractInfo(msg.title);
            if (isDefined(lineNum)) {
              this.setHit(lineNum);
            }
          }
          if (msg.title.startsWith("Breakpoint")) {
            // Handle breakpoint added case
            const { breakpointId, lineNum } = this.extractInfo(msg.title);
            if (isDefined(breakpointId) && isDefined(lineNum)) {
              this.addBreakpoint(lineNum, breakpointId, "");
              // You can add more logic here, such as storing the breakpoint ID
            }
          }
          if (msg.title.startsWith("Deleted")) {
            // Handle breakpoint removed case
            const { breakpointId, lineNum } = this.extractInfo(msg.title);
            if (isDefined(breakpointId) && isDefined(lineNum)) {
              console.log(`Breakpoint removed with ID: ${breakpointId}`);
              this.removeBreakpoint(lineNum);
              // You can add more logic here, such as removing the breakpoint
            }
          }
          if (msg.title.startsWith("New condition set for breakpoint")) {
            // pass
          }
        } else if (msg.msgType.name == "ERROR") {
          const { lineNum } = this.extractInfo(msg.source);
          if (isDefined(lineNum)) {
            this.setHit(lineNum);
          }
        }
      });
    });
  }

  private debugCommandQueue: DebugCommandRequest[] = [];
  private executionActive = false;

  private queueCommand(cmd: DebugCommandRequest) {
    this.debugCommandQueue.push(cmd);
    if (this.executionActive) {
      this.sendCommand();
    } else {
      console.log("execution is not active");
    }
  }

  private sendCommand() {
    if (this.debugCommandQueue.length > 0) {
      let payload = this.debugCommandQueue.shift();
      this.workflowWebsocketService.sendDebugCommand(payload!);
    }
  }

  public resetState() {
    this.executionActive = false;
    this.debugCommandQueue = [];
    this.getDebugState().clear();
  }

  private hasBreakpoint(lineNum: number): boolean {
    return this.getDebugState().has(String(lineNum));
  }

  public getCondition(lineNum: number): string {
    let line = String(lineNum);
    if (!this.getDebugState().has(line)) {
      return "";
    }
    let info = this.getDebugState().get(line)!;
    return info.condition;
  }

  public setCondition(lineNum: number, condition: string, workerIds: readonly string[]) {
    const breakpointInfo = this.getDebugState().get(String(lineNum));
    if (!isDefined(breakpointInfo)) {
      return;
    }
    workerIds.forEach(workerId => {
      this.queueCommand({
        operatorId: this.currentOperatorId,
        workerId,
        cmd: "condition " + breakpointInfo!.breakpointId + " " + condition,
      });
    });

    this.getDebugState().set(String(lineNum), { ...breakpointInfo, condition: condition });
  }

  public setContinue() {
    this.getDebugState().forEach((value, key) => {
      // first unset hit to trigger breakpoint update event
      if (value.hit) {
        this.getDebugState().set(key, { ...value, hit: false });
      }
      // then remove any temporary breakpoints
      if (value.breakpointId === undefined) {
        this.getDebugState().delete(key);
      }
    });
  }

  public setHit(lineNum: number) {
    let line = String(lineNum);
    // if the line has no breakpoint, create a temporary one.
    // this temporary breakpoint will be removed after hit.
    if (!this.getDebugState().has(line)) {
      this.getDebugState().set(line, { breakpointId: undefined, condition: "", hit: false });
    }

    // set the hit flag to true, trigger breakpoint update event.
    let breakpoint = this.getDebugState().get(line)!;
    this.getDebugState().set(line, { ...breakpoint, hit: true });
  }

  addOrRemoveBreakpoint(lineNum: number, workerIds: readonly string[]) {
    const cmd = this.hasBreakpoint(lineNum) ? "clear" : "break";
    const breakpointId = this.getDebugState().get(String(lineNum))?.breakpointId || "";

    workerIds.forEach(workerId => {
      this.queueCommand({
        operatorId: this.currentOperatorId,
        workerId,
        cmd: `${cmd} ${cmd === "clear" ? breakpointId : lineNum}`,
      });
    });
  }

  addBreakpoint(lineNum: number, breakpointId: number, condition: string) {
    this.getDebugState().set(String(lineNum), { breakpointId, condition, hit: false });
  }

  removeBreakpoint(lineNum: number) {
    this.getDebugState().delete(String(lineNum));
  }

  getCurrentBreakpoints() {
    return Array.from(this.getDebugState().keys());
  }

  private extractInfo(message: string): { breakpointId?: number; lineNum?: number } {
    const breakpointMatch = message.match(/(?:Breakpoint|Deleted breakpoint) (\d+) at .+:(\d+)/);
    if (breakpointMatch) {
      return {
        breakpointId: parseInt(breakpointMatch[1], 10),
        lineNum: parseInt(breakpointMatch[2], 10),
      };
    }

    const lineNumberMatch = message.match(/\.py\((\d+)\)|:(\d+)/);
    if (lineNumberMatch) {
      const lineNum = parseInt(lineNumberMatch[1] || lineNumberMatch[2], 10);
      return { lineNum };
    }

    return {};
  }
}

@Injectable({
  providedIn: "root",
})
export class UdfDebugService {
  private breakpointManagers: Map<string, BreakpointManager> = new Map();

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private workflowStatusService: WorkflowStatusService,
    private executeWorkflowService: ExecuteWorkflowService,
  ) {
    // for each operator, create a breakpoint manager
    this.workflowActionService.texeraGraph.getAllOperators().forEach(op => {
      this.getOrCreateManager(op.operatorID);
    });
  }

  public getOrCreateManager(operatorId: string): BreakpointManager {
    if (!this.breakpointManagers.has(operatorId)) {
      this.breakpointManagers.set(
        operatorId,
        new BreakpointManager(
          this.workflowWebsocketService,
          this.workflowStatusService,
          this.workflowActionService,
          operatorId,
        ),
      );
    }
    return this.breakpointManagers.get(operatorId)!;
  }

  doUpdateBreakpointCondition(operatorId: string, lineNumber: number, condition: string) {
    // if new condition is not the same as the saved one, update it
    if (condition !== this.getOrCreateManager(operatorId).getCondition(lineNumber)) {
      this.getOrCreateManager(operatorId).setCondition(
        lineNumber,
        condition,
        this.executeWorkflowService.getWorkerIds(operatorId),
      );
    }
  }

  doModifyBreakpoint(operatorId: string, lineNumber: number) {
    this.getOrCreateManager(operatorId).addOrRemoveBreakpoint(
      lineNumber,
      this.executeWorkflowService.getWorkerIds(operatorId),
    );
  }

  doContinue(operatorId: string, workerId: string) {
    this.getOrCreateManager(operatorId).setContinue();
    // TODO: make this queue command
    this.workflowWebsocketService.send("DebugCommandRequest", {
      operatorId,
      workerId,
      cmd: "continue",
    });
  }

  doStep(operatorId: string, workerId: string) {
    this.getOrCreateManager(operatorId).setContinue();
    // TODO: make this queue command
    this.workflowWebsocketService.send("DebugCommandRequest", {
      operatorId,
      workerId,
      cmd: "next",
    });
  }
}
