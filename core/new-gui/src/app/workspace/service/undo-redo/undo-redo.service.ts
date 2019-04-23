import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { JointGraphWrapper } from './../workflow-graph/model/joint-graph-wrapper';
import { WorkflowGraphReadonly } from './../workflow-graph/model/workflow-graph';
import { Injectable } from '@angular/core';
import { debounceTime } from 'rxjs/operators';


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
  ) {
    this.testGraph = workflowActionService.getTexeraGraph();

    this.testGraph.getOperatorAddStream().subscribe(
      value => {
        console.log(value.operatorID);
        if (this.undoToggle) {
          this.undos.push(() => {workflowActionService.deleteOperator(value.operatorID); });
          if (this.clearRedo) {
            this.redos = [];
          }
          this.changeProperty = false; /* sometimes when we create an operator, we trigger a property change event
          that we want to ignore */
          setTimeout(() => { // sets changeProperty back to true after half a second since not all operators trigger it
            this.changeProperty = true;
          }, 500);
        } else {
          this.redos.push(() => {workflowActionService.deleteOperator(value.operatorID); });
        }
      }
    );

    // potential problem: when dragging the link around everytime it gets magnetted to a port and pulled away
    // it gets counted as link creation/deletion. You end up with a ton of them for one link creation
    this.testGraph.getOperatorDeleteStream().subscribe(
      value => {
        if (this.undoToggle) {
          // we need to somehow grab the point
          this.undos.push(() => {workflowActionService.addOperator(value.deletedOperator,
            workflowActionService.getPoint(value.deletedOperator.operatorID)); });

          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(() => {workflowActionService.addOperator(value.deletedOperator,
            workflowActionService.getPoint(value.deletedOperator.operatorID)); });
        }
      }
    );

    this.testGraph.getLinkAddStream().subscribe(
      value => {
        if (this.undoToggle) {
          this.undos.push(() => {workflowActionService.deleteLinkWithID(value.linkID); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(() => {workflowActionService.deleteLinkWithID(value.linkID); });
        }
      }
    );
    this.testGraph.getLinkDeleteStream().subscribe(
      value => {
        if (this.undoToggle) {
          this.undos.push(() => {workflowActionService.addLink(value.deletedLink); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(() => {workflowActionService.addLink(value.deletedLink); });
        }
      }
    );

    this.testGraph.getOperatorPropertyChangeStream().subscribe(
      value => {
        // another problem: not all operators will trigger this when operator is created SOLVED
        if (this.undoToggle && this.changeProperty) {
          this.undos.push(() => {workflowActionService.changeOperatorProperty(value.operator.operatorID, value.oldProperty); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else if (this.changeProperty) {
          this.redos.push(() => {workflowActionService.changeOperatorProperty(value.operator.operatorID, value.oldProperty); });
        }
      }
    );

    // CURRENT BUGS: 1. When undoing too fast, the function for redoing the drag will get added after everything else
    // 2. After re-adding an operator, the elements change slightly so it won't work. Possible solution: directly modify
    // undoAction()
    // problem is you want to wait a bit before adding to undo, but when you redo you want to add it back instantly
    // solution: write a function in this file, an arrow function(?)
    // consider moving the
    workflowActionService.getJointGraphWrapper().getJointOperatorCellDragStream().pipe(debounceTime(400)).subscribe(
      value => {
        // only want to trigger stuff in here when new action is performed, not from a stack
        if (this.undoToggle && this.dragToggle) {
          if (this.clearRedo) {
            this.redos = [];
          }
          const pointer = workflowActionService.pointsPointer.get(String(value.id));
          let points = workflowActionService.pointsUndo.get(String(value.id));
          if ((pointer || pointer === 0) && points && this.clearRedo) {
            // check to see if we're at the top of the stack
            if (pointer !== points.length - 1) {
              points = points.slice(0, pointer + 1);
            }

            // increment pointer
            workflowActionService.pointsPointer.set(String(value.id), pointer + 1);
            // add value to map
            console.log('boo');
            points.push(value.attributes.position);
            workflowActionService.pointsUndo.set(String(value.id), points);
          }
          this.undos.push(() => {this.undoDrag(String(value.id)); });

        }
        this.dragToggle = true;
      }
    );


  }

  // lets us undo/redo dragging quickly
  public undoDrag(ID: string): void {
    console.log('hi');
    console.log(this.workflowActionService.pointsUndo.get(ID));
    this.workflowActionService.undoDragOperator(ID);
    this.redos.push(() => {this.redoDrag(ID); });
  }

  public redoDrag(ID: string): void {
    console.log('bye');
    console.log(this.workflowActionService.pointsUndo.get(ID));
    this.workflowActionService.redoDragOperator(ID);
    this.undos.push(() => {this.undoDrag(ID); });
  }

  public undoAction(): void {
    // We have a toggle to let our service know to add to the redo stack
    if (this.undos.length > 0) {
      this.undoToggle = false;
      this.dragToggle = false;
      this.undos[this.undos.length - 1]();
      this.undos.pop();
    }
    this.undoToggle = true;
  }

  public redoAction(): void {
    // need to figure out what to keep on the stack and off
    if (this.redos.length > 0) {
      // set clearRedo to false so when we redo an action, we keep the rest of the stack
      this.clearRedo = false;
      this.dragToggle = false;
      this.redos[this.redos.length - 1]();
      this.redos.pop();
    }
    this.clearRedo = true;
  }
}
