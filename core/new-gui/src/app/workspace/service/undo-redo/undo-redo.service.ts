import { WorkflowActionService, Command } from './../workflow-graph/model/workflow-action.service';
import { JointGraphWrapper } from './../workflow-graph/model/joint-graph-wrapper';
import { WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { Injectable } from '@angular/core';
import { debounceTime } from 'rxjs/operators';
import { OperatorPredicate, OperatorLink } from '../../types/workflow-common.interface';


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


  constructor() { }

  public undoAction(): void {
    // We have a toggle to let our service know to add to the redo stack
    if (this.undoStack.length > 0) {
      const command = this.undoStack.pop();
      if (command) {
        this.setListenJointCommand(false);
        command.undo();
        this.redoStack.push(command);
        this.setListenJointCommand(true);
      }
    }
  }

  public redoAction(): void {
    // need to figure out what to keep on the stack and off
    if (this.redoStack.length > 0) {
      // set clearRedo to false so when we redo an action, we keep the rest of the stack
      const command = this.redoStack.pop();
      if (command) {
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

  }

  public addCommand(command: Command): void {
    this.undoStack.push(command);
    this.redoStack = [];
  }

  public setListenJointCommand(toggle: boolean): void {
    this.listenJointCommand = toggle;
  }
}
