import { Injectable } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { nonNull } from "../../../common/util/assert";
import { Command, CommandMessage } from "../../types/command.interface";
import { WorkflowCollabService } from "./../workflow-collab/workflow-collab.service";

/* TODO LIST FOR BUGS
1. Problem with repeatedly adding and deleting a link without letting go, unintended behavior
2. See if there's a way to only store a previous version of an operator's properties
after a certain period of time so we don't undo one character at a time */

@Injectable({
  providedIn: "root",
})
export class UndoRedoService {
  // lets us know whether to listen to the JointJS observables, most of the time we don't
  public listenJointCommand: boolean = true;
  // private testGraph: WorkflowGraphReadonly;

  private undoStack: Command[] = [];
  private redoStack: Command[] = [];

  private workFlowModificationEnabled = true;

  private canUndoStream = new Subject<boolean>();
  private canRedoStream = new Subject<boolean>();

  constructor(private workflowCollabService: WorkflowCollabService) {
    this.listenToRemoteChange();
  }

  public enableWorkFlowModification() {
    this.workFlowModificationEnabled = true;
  }
  public disableWorkFlowModification() {
    this.workFlowModificationEnabled = false;
  }

  public checkWorkFlowModificationEnabled(): boolean {
    return this.workFlowModificationEnabled;
  }

  public undoAction(): void {
    // We have a toggle to let our service know to add to the redo stack
    if (this.undoStack.length > 0) {
      if (!this.workFlowModificationEnabled && this.undoStack[this.undoStack.length - 1].modifiesWorkflow) {
        console.error("attempted to undo a workflow-modifying command while workflow modification is disabled");
        return;
      }

      const command = nonNull(this.undoStack.pop());
      this.setListenJointCommand(false);
      if (command.undo) command.undo();
      this.redoStack.push(command);
      this.setListenJointCommand(true);
      this.canUndoStream.next(this.canUndo());
      const commandMessage: CommandMessage = { action: "undoredo", parameters: [], type: "undo" };
      this.workflowCollabService.propagateChange(commandMessage);
      console.log("service can undo", this.canUndo());
    }
  }

  public redoAction(): void {
    // need to figure out what to keep on the stack and off
    if (this.redoStack.length > 0) {
      if (!this.workFlowModificationEnabled && this.redoStack[this.redoStack.length - 1].modifiesWorkflow) {
        console.error("attempted to redo a workflow-modifying command while workflow modification is disabled");
        return;
      }
      const command = nonNull(this.redoStack.pop());
      this.setListenJointCommand(false);
      if (command.redo) {
        command.redo();
      } else {
        command.execute();
      }
      this.undoStack.push(command);
      this.setListenJointCommand(true);
      this.canRedoStream.next(this.canRedo());
      const commandMessage: CommandMessage = { action: "undoredo", parameters: [], type: "redo" };
      this.workflowCollabService.propagateChange(commandMessage);
      console.log("service can redo", this.canRedo());
    }
  }

  public addCommand(command: Command): void {
    // if undo and redo modifications are disabled, then don't add to the stack
    if (!this.workFlowModificationEnabled) {
      return;
    }
    this.undoStack.push(command);
    this.redoStack = [];
  }

  public setListenJointCommand(toggle: boolean): void {
    this.listenJointCommand = toggle;
  }

  public getUndoLength(): number {
    return this.undoStack.length;
  }

  public getRedoLength(): number {
    return this.redoStack.length;
  }

  public canUndo(): boolean {
    return (
      this.undoStack.length > 0 &&
      (this.workFlowModificationEnabled || !this.undoStack[this.undoStack.length - 1].modifiesWorkflow)
    );
  }

  public getCanUndoStream(): Observable<boolean> {
    return this.canUndoStream.asObservable();
  }

  public canRedo(): boolean {
    return (
      this.redoStack.length > 0 &&
      (this.workFlowModificationEnabled || !this.redoStack[this.redoStack.length - 1].modifiesWorkflow)
    );
  }

  public getCanRedoStream(): Observable<boolean> {
    return this.canRedoStream.asObservable();
  }

  public clearUndoStack(): void {
    this.undoStack = [];
  }

  public clearRedoStack(): void {
    this.redoStack = [];
  }

  private listenToRemoteChange(): void {
    this.workflowCollabService.getChangeStream().subscribe(message => {
      const previousModificationEnabledStatus = this.workFlowModificationEnabled;
      this.enableWorkFlowModification();
      if (message.type === "undo") {
        this.workflowCollabService.handleRemoteChange(() => {
          this.undoAction();
        });
      } else if (message.type === "redo") {
        this.workflowCollabService.handleRemoteChange(() => {
          this.redoAction();
        });
      }
      if (!previousModificationEnabledStatus) this.disableWorkFlowModification();
    });
  }
}
