import * as Y from "yjs";
import {Injectable} from "@angular/core";
import {BehaviorSubject, Subject} from "rxjs";
import {WorkflowWebsocketService} from "../workflow-websocket/workflow-websocket.service";
import {FrontendDebugCommand, UDFBreakpointInfo} from "../../types/workflow-websocket.interface";
import {ExecutionState} from "../../types/execute-workflow.interface";
import {WorkflowActionService} from "../workflow-graph/model/workflow-action.service";
import { MonacoBreakpoint } from "monaco-breakpoints";

export class BreakpointManager {

  constructor(private workflowWebsocketService:WorkflowWebsocketService,
              private currentOperatorId:string,
              private lineNumToBreakpointMapping:Y.Map<UDFBreakpointInfo>) {
    this.triggerUpdate()
    this.lineNumToBreakpointMapping.observe(evt => {
      this.triggerUpdate()
    })

    workflowWebsocketService.subscribeToEvent("WorkflowStateEvent").subscribe(evt =>{
      if(evt.state === ExecutionState.Initializing){
        if(!this.workflowWebsocketService.executionInitiator){
          this.debugCommandQueue = [];
        }else{
          this.sendCommand();
        }
        this.executionActive = true;
      }
      if(evt.state === ExecutionState.Running || evt.state === ExecutionState.Paused){
        this.executionActive = true;
      }
    })

    workflowWebsocketService.subscribeToEvent("ConsoleUpdateEvent").subscribe(evt =>{
      if(evt.messages.length === 0){
        return;
      }
      if(evt.messages[evt.messages.length-1].msgType.name === "DEBUGGER"){
        this.executionActive = true;
        this.sendCommand();
      }
    });
  }

  public triggerUpdate(){
    console.log("get update ", Array.from(this.lineNumToBreakpointMapping.keys()))
    this.lineNumToBreakpointSubject.next(this.lineNumToBreakpointMapping);
  }

  private hitLineNum = 0
  private hitLineNumSubject: Subject<number> = new BehaviorSubject(this.hitLineNum)
  private lineNumToBreakpointSubject: Subject<Y.Map<UDFBreakpointInfo>> = new BehaviorSubject(new Y.Map());
  private debugCommandQueue: FrontendDebugCommand[] = [];
  private executionActive = false;



  private queueCommand(cmd: FrontendDebugCommand){
    let exist = this.debugCommandQueue.find(value => {
      return value == cmd;
    })
    if(exist){
      return;
    }
    if(cmd.command === "clear"){
      const breakCommandIdx = this.debugCommandQueue.findIndex(request => request.command === "break" && request.line == cmd.line);
      if (breakCommandIdx !== -1) {
        this.debugCommandQueue.splice(breakCommandIdx, 1);
        return;
      }
    } else if(cmd.command === "condition"){
      const breakCommandIdx = this.debugCommandQueue.findIndex(request => request.command === "break" && request.line == cmd.line);
      if (breakCommandIdx !== -1) {
        this.debugCommandQueue[breakCommandIdx] = {...this.debugCommandQueue[breakCommandIdx], condition: cmd.condition}
        return;
      }
    }
    this.debugCommandQueue.push(cmd);
    if(this.executionActive){
      this.sendCommand();
    }
  }

  private sendCommand(){
    console.log("try to send a command");
    if(this.debugCommandQueue.length > 0){
      let payload = this.debugCommandQueue.shift();
      this.workflowWebsocketService.prepareDebugCommand(payload!);
      let needContinue = this.debugCommandQueue.length === 0 || this.debugCommandQueue[this.debugCommandQueue.length-1].command !== "continue";
      if(payload?.command === "break" && this.hitLineNum == 0 && needContinue){
        this.debugCommandQueue.push({...payload, command: "continue"});
      }
    }
  }

  public getBreakpointHitStream() {
    return this.hitLineNumSubject.asObservable();
  }

  public getLineNumToBreakpointMappingStream(){
    return this.lineNumToBreakpointSubject.asObservable();
  }

  public resetState(){
    this.executionActive = false;
    this.debugCommandQueue = [];
    this.lineNumToBreakpointMapping.forEach((v,k) => {
      this.queueCommand({operatorId: this.currentOperatorId, command:"break", line: Number(k), breakpointId:0, condition: v.condition})
    });
    this.setHitLineNum(0);
  }


  public removeBreakpoint(lineNum: number){
    let line = String(lineNum)
    this.setBreakpoints(Array.from(this.lineNumToBreakpointMapping.keys()).filter(e => e!= line), false)
  }

  public setBreakpoints(lineNum: string[], triggerClear:boolean = true){
    console.log("set breakpoint"+lineNum)
    let changed = false;
    let lineSet = new Set(lineNum);
    lineSet.forEach(line =>{
      if(!this.lineNumToBreakpointMapping.has(line)){
        changed = true;
        this.lineNumToBreakpointMapping.set(line, {breakpointId:undefined, condition:""})
        console.log("add break command")
        this.queueCommand({operatorId: this.currentOperatorId,
          command:"break",
          line:Number(line),
          breakpointId:0,
          condition: ""})
      }
    })
    this.lineNumToBreakpointMapping.forEach((v,k,m) =>{
      if(!lineSet.has(k)){
        changed = true;
        if(triggerClear){
          this.queueCommand({operatorId: this.currentOperatorId,
            command:"clear",
            line:Number(k),
            breakpointId:v.breakpointId ?? 0, condition:""});
        }
        m.delete(k)
      }
    })
    if(changed){
      this.triggerUpdate();
    }
  }

  public getCondition(lineNum:number):string{
    let line = String(lineNum)
    if(!this.lineNumToBreakpointMapping.has(line)){
      return "";
    }
    let info = this.lineNumToBreakpointMapping.get(line)!
    return info.condition;
  }

  public setCondition(lineNum:number, condition:string){
    let line = String(lineNum)
    let info = this.lineNumToBreakpointMapping.get(line)!
    this.lineNumToBreakpointMapping.set(line,  {...info, condition:condition});
    this.queueCommand({operatorId: this.currentOperatorId,
      command:"condition",
      line:lineNum,
      breakpointId:info.breakpointId ?? 0,
      condition: condition})
    this.triggerUpdate();
  }

  public assignBreakpointId(lineNum: number, breakpointId: number): void {
    let line = String(lineNum)
    console.log("assign id to breakpoint"+lineNum+" "+breakpointId)
    let info = this.lineNumToBreakpointMapping.get(line)!
    this.lineNumToBreakpointMapping.set(line,  {...info, breakpointId:breakpointId})
  }

  public setHitLineNum(lineNum: number) {
    this.hitLineNum = lineNum
    this.hitLineNumSubject.next(this.hitLineNum)
  }

  setBreakpoint(lineNumber: number) {

  }
}

@Injectable({
  providedIn: "root",
})
export class UdfDebugService {
  private breakpointManagers: Map<string, BreakpointManager> = new Map();

  constructor(private workflowWebsocketService: WorkflowWebsocketService, private workflowActionService:WorkflowActionService) {
    this.subscribePythonLineHighlight();
  }

  public getOrCreateManager(operatorId: string): BreakpointManager {
    let debugState = this.workflowActionService.texeraGraph.sharedModel.debugState
    if(!debugState.has(operatorId)){
      debugState.set(operatorId, new Y.Map<UDFBreakpointInfo>())
    }
    if (!this.breakpointManagers.has(operatorId)) {
      this.breakpointManagers.set(operatorId, new BreakpointManager(this.workflowWebsocketService, operatorId, debugState.get(operatorId)));
    }
    let mgr = this.breakpointManagers.get(operatorId)!;
    mgr.triggerUpdate()
    return mgr;
  }

  public getAllManagers():BreakpointManager[]{
    return Array.from(this.breakpointManagers.values());
  }

  private subscribePythonLineHighlight() {
    this.workflowWebsocketService.subscribeToEvent("ConsoleUpdateEvent").subscribe(event => {
      if (event.messages.length == 0) {
        return
      }
      event.messages.forEach(msg => {
        console.log("processing message", msg)
        const breakpointManager = this.getOrCreateManager(event.operatorId);
        if (msg.source == "(Pdb)" && msg.msgType.name == "DEBUGGER") {
          if (msg.title.startsWith(">")) {
            const lineNum = this.extractLineNumber(msg.title)
            if (lineNum) {
              breakpointManager.setHitLineNum(lineNum)
            }
          }
        }
        if (msg.msgType.name == "ERROR") {
          const lineNum = this.extractLineNumberException(msg.source)
          console.log(lineNum)
          if (lineNum) {
            breakpointManager.setHitLineNum(lineNum)
          }
        }
      })
    })
  }

  private extractLineNumber(message: string): number | undefined {
    const regex = /\.py\((\d+)\)/;
    const match = message.match(regex);

    if (match && match[1]) {
      return parseInt(match[1]);
    }

    return undefined;
  }

  private extractLineNumberException(message: string): number | undefined {
    const regex = /:(\d+)/;
    const match = message.match(regex);

    if (match && match[1]) {
      return parseInt(match[1]);
    }

    return undefined;
  }

  clearDebugStates() {
    this.getAllManagers().forEach(manager => manager.resetState())
  }

  addOrRemoveBreakpoint(operatorId: string, lineNumber: number) {
    this.getOrCreateManager(operatorId).setBreakpoint(lineNumber)
  }
}
