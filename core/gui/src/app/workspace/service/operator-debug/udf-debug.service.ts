import * as Y from "yjs";
import { Injectable } from "@angular/core";
import { WorkflowWebsocketService } from "../workflow-websocket/workflow-websocket.service";
import { DebugCommandRequest, UDFBreakpointInfo } from "../../types/workflow-websocket.interface";
import { OperatorState } from "../../types/execute-workflow.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { isDefined } from "../../../common/util/predicate";
import { WorkflowStatusService } from "../workflow-status/workflow-status.service";
import { ExecuteWorkflowService } from "../execute-workflow/execute-workflow.service";

export class BreakpointManager {
  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowStatusService: WorkflowStatusService,
    private currentOperatorId: string,
    private lineNumToBreakpointMapping: Y.Map<UDFBreakpointInfo>
  ) {
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
            const lineNum = this.extractLineNumber(msg.title);
            if (isDefined(lineNum)) {
              this.setHitLineNum(lineNum);
            }
          }
          if (msg.title.startsWith("Breakpoint")) {
            // Handle breakpoint added case
            const { breakpointId, lineNum } = this.extractBreakpointInfo(msg.title);
            if (isDefined(breakpointId) && isDefined(lineNum)) {
              this.addBreakpoint(lineNum, breakpointId, "");
              // You can add more logic here, such as storing the breakpoint ID
            }
          }
          if (msg.title.startsWith("Deleted")) {
            // Handle breakpoint removed case
            const { breakpointId, lineNum } = this.extractBreakpointInfo(msg.title);
            if (isDefined(breakpointId) && isDefined(lineNum)) {
              console.log(`Breakpoint removed with ID: ${breakpointId}`);
              this.removeBreakpoint(lineNum);
              // You can add more logic here, such as removing the breakpoint
            }
          }
          if (msg.title.startsWith("New condition set for breakpoint")){
            // pass
          }
        } else if (msg.msgType.name == "ERROR") {
          const lineNum = this.extractLineNumberException(msg.source);
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
    this.lineNumToBreakpointMapping.clear();
  }

  private hasBreakpoint(lineNum: number): boolean {
    return this.lineNumToBreakpointMapping.has(String(lineNum));
  }

  public getCondition(lineNum: number): string {
    let line = String(lineNum);
    if (!this.lineNumToBreakpointMapping.has(line)) {
      return "";
    }
    let info = this.lineNumToBreakpointMapping.get(line)!;
    return info.condition;
  }

  public setCondition(lineNum: number, condition: string, workerIds:readonly string[]) {
    let line = String(lineNum);
    let info = this.lineNumToBreakpointMapping.get(line)!;
    console.log("set condition!!", lineNum, condition);
    workerIds.forEach(workerId => {
      this.queueCommand({
        operatorId: this.currentOperatorId,
        workerId,
        cmd: "condition " + info.breakpointId + " " + condition,
      })
    });
    this.lineNumToBreakpointMapping.set(line, { ...info, condition: condition });

  }

  public setContinue() {
    // for each breakpoint in this.lineNumToBreakpointMapping, if breakpointId is undefined, remove it. and if hit is true, set it to false.
    this.lineNumToBreakpointMapping.forEach((value, key) => {
      if (value.hit) {
        this.lineNumToBreakpointMapping.set(key, { ...value, hit: false });
      }
      if (value.breakpointId === undefined) {
        this.lineNumToBreakpointMapping.delete(key);
      }
    });
  }

  public setHitLineNum(lineNum: number) {
    console.log("set hit on ", lineNum);
    let line = String(lineNum);
    if (!this.lineNumToBreakpointMapping.has(line)) {
      this.lineNumToBreakpointMapping.set(line, { breakpointId: undefined, condition: "", hit: true });
    }
    let breakpointInfo = this.lineNumToBreakpointMapping.get(line)!;
    this.lineNumToBreakpointMapping.set(line, { ...breakpointInfo, hit: true });
  }

  addOrRemoveBreakpoint(lineNum: number, workerIds: readonly string[]) {
    if (this.hasBreakpoint(lineNum)) {
      // for each workerId
      workerIds.forEach(workerId => {
        this.queueCommand({
          operatorId: this.currentOperatorId,
          workerId: workerId,
          cmd: "clear " + this.lineNumToBreakpointMapping.get(String(lineNum))?.breakpointId,
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
    this.lineNumToBreakpointMapping.set(String(lineNum), { breakpointId, condition, hit: false });
  }

  removeBreakpoint(lineNum: number) {
    this.lineNumToBreakpointMapping.delete(String(lineNum));
  }

  getCurrentBreakpoints() {
    return Array.from(this.lineNumToBreakpointMapping.keys());
  }

  private extractLineNumber(message: string): number | undefined {
    const regex = /\.py\((\d+)\)/;
    const match = message.match(regex);

    if (match && match[1]) {
      return parseInt(match[1]);
    }
    return undefined;
  }

  private extractBreakpointInfo(title: string): { breakpointId?: number; lineNum?: number } {
    const match = title.match(/(?:Breakpoint|Deleted breakpoint) (\d+) at .+:(\d+)/);
    return {
      breakpointId: match ? parseInt(match[1], 10) : undefined,
      lineNum: match ? parseInt(match[2], 10) : undefined,
    };
  }

  private extractLineNumberException(message: string): number | undefined {
    const regex = /:(\d+)/;
    const match = message.match(regex);

    if (match && match[1]) {
      return parseInt(match[1]);
    }

    return undefined;
  }

  public getLineNumToBreakpointMapping() {
    return this.lineNumToBreakpointMapping;
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
    private executeWorkflowService: ExecuteWorkflowService
  ) {}

  public getOrCreateManager(operatorId: string): BreakpointManager {
    let debugState = this.workflowActionService.texeraGraph.sharedModel.debugState;
    if (!debugState.has(operatorId)) {
      debugState.set(operatorId, new Y.Map<UDFBreakpointInfo>());
    }
    if (!this.breakpointManagers.has(operatorId)) {
      this.breakpointManagers.set(
        operatorId,
        new BreakpointManager(
          this.workflowWebsocketService,
          this.workflowStatusService,
          operatorId,
          debugState.get(operatorId)
        )
      );
    }
    return this.breakpointManagers.get(operatorId)!;
  }

  doUpdateBreakpointCondition(operatorId:string, lineNumber: number, condition: string) {
    // if new condition is not the same as the saved one, update it
    if (condition !== this.getOrCreateManager(operatorId).getCondition(lineNumber)) {
      this.getOrCreateManager(operatorId).setCondition(lineNumber, condition, this.executeWorkflowService.getWorkerIds(operatorId));
    }
  }

  doModifyBreakpoint(operatorId: string, lineNumber: number) {
    this.getOrCreateManager(operatorId).addOrRemoveBreakpoint(
      lineNumber,
      this.executeWorkflowService.getWorkerIds(operatorId)
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
