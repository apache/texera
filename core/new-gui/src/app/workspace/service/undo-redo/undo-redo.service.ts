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

  public changeProperty: boolean = true; // when we add an operator, don't want to change properties
  public undoToggle: boolean = true; // lets us know whether to push to undo or redo stack
  public clearRedo: boolean = true; // lets us know whether to clear the redo stack. If we do a normal action, we should reset
  // the stack
  public dragToggle: boolean = true; // lets us know when we should append to the undo or redo stack when undoing dragging
  // private testGraph: WorkflowGraphReadonly;

  private undoStack: Command[] = [];
  private redoStack: Command[] = [];


  constructor() { }

  public undoAction(): void {
    // We have a toggle to let our service know to add to the redo stack
    if (this.undoStack.length > 0) {
      this.undoToggle = false;
      this.dragToggle = false;
      const command = this.undoStack.pop();
      if (command) {
        this.setListenJointCommand(false);
        command.undo();
        this.redoStack.push(command);
        this.setListenJointCommand(true);
      }
    }
    this.undoToggle = true;
  }

  public redoAction(): void {
    // need to figure out what to keep on the stack and off
    if (this.redoStack.length > 0) {
      // set clearRedo to false so when we redo an action, we keep the rest of the stack
      this.clearRedo = false;
      this.dragToggle = false;
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
    this.clearRedo = true;

  }

  public addCommand(command: Command): void {
    this.undoStack.push(command);
    this.redoStack = [];
  }

  public setListenJointCommand(toggle: boolean): void {
    this.listenJointCommand = toggle;
  }


  // private handleLinkAdd(): void {
  //   this.testGraph.getLinkAddStream().subscribe(
  //     value => {
  //       if (this.workflowActionService.separateLink) {
  //         if (this.undoToggle) {
  //           this.undos.push(() => {this.workflowActionService.deleteLinkWithID(value.linkID); });
  //           if (this.clearRedo) {
  //             this.redos = [];
  //           }
  //         } else {
  //           this.redos.push(() => {this.workflowActionService.deleteLinkWithID(value.linkID); });
  //         }
  //       }
  //     }
  //   );
  // }

  // private handleOperatorDrag(): void {
  //   this.workflowActionService.getJointGraphWrapper().getJointOperatorCellDragStream().pipe(debounceTime(350)).subscribe(
  //     value => {
  //       // only want to trigger stuff in here when new action is performed, not from a stack
  //       if (this.undoToggle && this.dragToggle) {
  //         if (this.clearRedo) {
  //           this.redos = [];
  //         }
  //         const pointer = this.workflowActionService.pointsPointer.get(String(value.id));
  //         let points = this.workflowActionService.pointsUndo.get(String(value.id));
  //         if ((pointer || pointer === 0) && points) {
  //           // check to see if we're at the top of the stack
  //           if (pointer !== points.length - 1) {
  //             // we're cutting out all of the coordinates we no longer need after we do a new drag
  //             points = points.slice(0, pointer + 1);
  //           }

  //           // increment pointer
  //           this.workflowActionService.pointsPointer.set(String(value.id), pointer + 1);
  //           // add value to map
  //           points.push(value.attributes.position);
  //           this.workflowActionService.pointsUndo.set(String(value.id), points);
  //         }
  //         this.undos.push(() => {this.undoDrag(String(value.id)); });

  //       }
  //       this.dragToggle = true;
  //     }
  //   );
  // }



  // // lets us undo/redo dragging quickly
  // private undoDrag(ID: string): void {
  //   this.workflowActionService.undoDragOperator(ID);
  //   this.redos.push(() => {this.redoDrag(ID); });
  // }

  // private redoDrag(ID: string): void {
  //   this.workflowActionService.redoDragOperator(ID);
  //   this.undos.push(() => {this.undoDrag(ID); });
  // }
}
