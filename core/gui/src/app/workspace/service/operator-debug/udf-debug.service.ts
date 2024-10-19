import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { DebugCommandRequest } from "../../types/workflow-websocket.interface";
import { OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";

export class BreakpointManager {

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private workflowActionService: WorkflowActionService,
    private currentOperatorId: string,
  ) {
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
              this.setHitLineNum(lineNum);
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
            this.setHitLineNum(lineNum);
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
    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).clear();
  }

  private hasBreakpoint(lineNum: number): boolean {
    return this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).has(String(lineNum));
  }

  public getCondition(lineNum: number): string {
    let line = String(lineNum);
    if (!this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).has(line)) {
      return "";
    }
    let info = this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).get(line)!;
    return info.condition;
  }

  public setCondition(lineNum: number, condition: string, workerIds: readonly string[]) {
    const breakpointInfo = this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).get(String(lineNum));
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

    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).set(String(lineNum), { ...breakpointInfo, condition: condition });
  }

  public setContinue() {
    // for each breakpoint in this.lineNumToBreakpointMapping, if breakpointId is undefined, remove it. and if hit is true, set it to false.
    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).forEach((value, key) => {
      if (value.hit) {
        this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).set(key, { ...value, hit: false });
      }
      if (value.breakpointId === undefined) {
        this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).delete(key);
      }
    });
  }

  public setHitLineNum(lineNum: number) {
    let line = String(lineNum);
    if (!this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).has(line)) {
      this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).set(line, { breakpointId: undefined, condition: "", hit: true });
    }
    let breakpointInfo = this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).get(line)!;
    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).set(line, { ...breakpointInfo, hit: true });
  }

  addOrRemoveBreakpoint(lineNum: number, workerIds: readonly string[]) {
    if (this.hasBreakpoint(lineNum)) {
      // for each workerId
      workerIds.forEach(workerId => {
        this.queueCommand({
          operatorId: this.currentOperatorId,
          workerId: workerId,
          cmd: "clear " + this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).get(String(lineNum))?.breakpointId,
        });
      });
    } else {
      // for each workerId
      workerIds.forEach(workerId => {
        this.queueCommand({
          operatorId: this.currentOperatorId,
          workerId: workerId,
          cmd: "break " + lineNum,
        });
      });
    }
  }

  addBreakpoint(lineNum: number, breakpointId: number, condition: string) {
    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).set(String(lineNum), { breakpointId, condition, hit: false });
  }

  removeBreakpoint(lineNum: number) {
    this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).delete(String(lineNum));
  }

  getCurrentBreakpoints() {
    return Array.from(this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId).keys());
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

  public getLineNumToBreakpointMapping() {
    return this.workflowActionService.texeraGraph.getOrCreateOperatorDebugState(this.currentOperatorId);
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
