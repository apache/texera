import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { JointGraphWrapper } from './../workflow-graph/model/joint-graph-wrapper';
import { WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { Point, OperatorPredicate, OperatorLink, OperatorPort } from '../../types/workflow-common.interface';
import { DragDropService } from '../drag-drop/drag-drop.service';
import { Injectable } from '@angular/core';
import { debounceTime } from 'rxjs/operators';
import { BehaviorSubject } from 'rxjs';


/* TODO LIST FOR BUGS
1. Problem with repeatedly adding and deleting a link without letting go, unintended behavior
2. See if there's a way to only store a previous version of an operator's properties
after a certain period of time so we don't undo one character at a time */

@Injectable()
export class UndoRedoService {
  // TODO: Next step, work on the function that wil add values to the undo/redo stacks
  // Will probably need to check what kind of value we receive and whatnot
  public undos: Array<Function> = []; // array to store values to undo
  public redos: Array<Function> = [];
  public changeProperty: boolean = true; // when we add an operator, don't want to change properties
  public undoToggle: boolean = true; // lets us know whether to push to undo or redo stack
  public clearRedo: boolean = true; // lets us know whether to clear the redo stack. If we do a normal action, we should reset
  // the stack
  public dragToggle: boolean = true; // lets us know when we should append to the undo or redo stack when undoing dragging
  private testGraph: WorkflowGraphReadonly;

  constructor(
    private workflowActionService: WorkflowActionService,
    private dragDrop: DragDropService
  ) {
    this.testGraph = workflowActionService.getTexeraGraph();

    this.testGraph.getOperatorAddStream().subscribe(
      value => {
        console.log(value.operatorID);
        if (this.undoToggle) {
          this.undos.push(function() {workflowActionService.deleteOperator(value.operatorID); });
          if (this.clearRedo) {
            this.redos = [];
          }
          this.changeProperty = false; /* sometimes when we create an operator, we trigger a property change event
          that we want to ignore */
          setTimeout(() => { // sets changeProperty back to true after half a second
            this.changeProperty = true;
          }, 500);
        } else {
          this.redos.push(function() {workflowActionService.deleteOperator(value.operatorID); });
        }
      }
    );

    // potential problem: when dragging the link around everytime it gets magnetted to a port and pulled away
    // it gets counted as link creation/deletion. You end up with a ton of them for one link creation
    this.testGraph.getOperatorDeleteStream().subscribe(
      value => {
        // try to see if I can add point to OperatorPredicate
        if (this.undoToggle) {
          // we need to somehow grab the point
          this.undos.push(function() {workflowActionService.addOperator(value.deletedOperator,
            workflowActionService.getPoint(value.deletedOperator.operatorID)); });

          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(function() {workflowActionService.addOperator(value.deletedOperator,
            workflowActionService.getPoint(value.deletedOperator.operatorID)); });
        }
      }
    );

    this.testGraph.getLinkAddStream().subscribe(
      value => {
        if (this.undoToggle) {
          this.undos.push(function() {workflowActionService.deleteLinkWithID(value.linkID); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(function() {workflowActionService.deleteLinkWithID(value.linkID); });
        }
      }
    );
    this.testGraph.getLinkDeleteStream().subscribe(
      value => {
        if (this.undoToggle) {
          this.undos.push(function() {workflowActionService.addLink(value.deletedLink); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(function() {workflowActionService.addLink(value.deletedLink); });
        }
      }
    );

    this.testGraph.getOperatorPropertyChangeStream().subscribe(
      value => {
        // the problem is that when we readd an operator, we want to add the starting state but it won't work DONE
        // another problem: not all operators will trigger this when operator is created SOLVED
        if (this.undoToggle && this.changeProperty) {
          this.undos.push(function() {workflowActionService.changeOperatorProperty(value.operator.operatorID, value.oldProperty); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else if (this.changeProperty) {
          this.redos.push(function() {workflowActionService.changeOperatorProperty(value.operator.operatorID, value.oldProperty); });
        }
      }
    );

    // TODO: work on undoing dragging around
    // CURRENT BUGS: 1. When undoing too fast, the function for redoing the drag will get added after everything else
    // 2. After readding an operator, the elements change slightly so it won't work. Possible solution: directly modify
    // undoAction()
    workflowActionService.getJointGraphWrapper().getJointOperatorCellDragStream().pipe(debounceTime(400)).subscribe(
      value => {
        console.log(value.id);
        if (this.undoToggle && this.dragToggle) {
          // check to see if we're at the top of the stack
          if (this.clearRedo) {
            // might have to also modify the maps
           /*  workflowActionService.pointsUndo.set(String(value.id),
              workflowActionService.pointsUndo.get(String(value.id)).slice(0,
              workflowActionService.pointsPointer.get(String(value.id)) + 1));
            workflowActionService.pointsPointer.set(String(value.id), workflowActionService.pointsUndo.get(String(value.id)).length - 1); */
            this.redos = [];
          }
          if (workflowActionService.pointsPointer.get(String(value.id)) ===
          workflowActionService.pointsUndo.get(String(value.id)).length - 1 && this.clearRedo) { // only want to expand when not redoing
            // increment the pointer
            console.log('Expanding');
            workflowActionService.pointsPointer.set(String(value.id), workflowActionService.pointsPointer.get(String(value.id)) + 1);
            // add value to map
            workflowActionService.pointsUndo.get(String(value.id)).push(value.attributes.position);
          }
          this.undos.push(function() {workflowActionService.undoDragOperator(String(value.id)); });
        } else {
          // problem with redo
          this.redos.push(function() {workflowActionService.redoDragOperator(String(value.id)); });
        }
      }
    );


  }


  public undoAction(): void {
    // We have a toggle to let our service know to add to the redo stack
    if (this.undos.length > 0) {
      this.undoToggle = false;
      this.dragToggle = false;
      this.undos[this.undos.length - 1]();
      this.undos.pop();
    }
    setTimeout(() => { // sets dragToggle back to true after 0.6 seconds
      this.dragToggle = true;
    }, 600);
    this.undoToggle = true;
  }

  public redoAction(): void {
    // need to figure out what to keep on the stack and off
    if (this.redos.length > 0) {
      // set clearRedo to false so when we redo an action, we keep the rest of the stack
      this.clearRedo = false;
      this.redos[this.redos.length - 1]();
      this.redos.pop();
    }
    setTimeout(() => { // sets dragToggle back to true after 0.6 seconds
      this.clearRedo = true;
    }, 600);
  }
}
