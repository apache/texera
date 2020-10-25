import { Command } from './../workflow-graph/model/workflow-action.service';
import { Injectable } from '@angular/core';
import { assertType } from '../../../common/util/assert' ;

/* TODO LIST FOR BUGS
1. Problem with repeatedly adding and deleting a link without letting go, unintended behavior
2. See if there's a way to only store a previous version of an operator's properties
after a certain period of time so we don't undo one character at a time */

@Injectable()
export class UndoRedoService {

  // lets us know whether to listen to the JointJS observables, most of the time we don't
  public listenJointCommand: boolean = true;
  // private testGraph: WorkflowGraphReadonly;

  private undoStack: Command[] = [];
  private redoStack: Command[] = [];

  private workFlowModificationEnabled = true;


  constructor() { }

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
      if (this.undoStack[this.undoStack.length - 1].modifiesWorkflow && !this.workFlowModificationEnabled) {
        console.error('attempted to undo a workflow-modifying command while workflow modification is disabled');
        return;
      }

      const command = this.undoStack.pop();
      assertType<Command>(command);
      this.setListenJointCommand(false);
      command.undo();
      this.redoStack.push(command);
      this.setListenJointCommand(true);
    }
  }

  public redoAction(): void {
    // need to figure out what to keep on the stack and off
    if (this.redoStack.length > 0) {
      if (this.redoStack[this.redoStack.length - 1].modifiesWorkflow && !this.workFlowModificationEnabled) {
        console.error('attempted to redo a workflow-modifying command while workflow modification is disabled');
        return;
      }
      // set clearRedo to false so when we redo an action, we keep the rest of the stack
      const command = this.redoStack.pop();
      assertType<Command>(command);
      this.setListenJointCommand(false);
      if (command.redo) {
        command.redo();
      } else {
        command.execute();
      }
      this.undoStack.push(command);
      this.setListenJointCommand(true);
    }

  }

  public addCommand(command: Command): void {
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
}
