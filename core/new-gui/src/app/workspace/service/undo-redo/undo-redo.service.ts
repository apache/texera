import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
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

    this.handleOperatorAdd();
    this.handleOperatorDelete();
    this.handleLinkAdd();
    this.handleLinkDelete();
    this.handlePropertyChange();
    this.handleOperatorDrag();

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
  private handleOperatorAdd(): void {

    // when we undo to readd link + operators, also toggle the boolean(?). Probably do this in the
    // new function itself
    this.testGraph.getOperatorAddStream().subscribe(
      value => {
        if (this.undoToggle) {
          this.undos.push(() => {this.workflowActionService.deleteOperator(value.operatorID); });
          if (this.clearRedo) {
            this.redos = [];
          }
          this.changeProperty = false; /* sometimes when we create an operator, we trigger a property change event
          that we want to ignore */
          setTimeout(() => { // sets changeProperty back to true after half a second since not all operators trigger it
            this.changeProperty = true;
          }, 500);
        } else {
          this.redos.push(() => {this.workflowActionService.deleteOperator(value.operatorID); });
        }
      }
    );
  }

  private handleOperatorDelete(): void {
    // potential problem: when dragging the link around everytime it gets magnetted to a port and pulled away
    // it gets counted as link creation/deletion. You end up with a ton of them for one link creation
    this.testGraph.getOperatorDeleteStream().subscribe(
      value => {
         // When we click redo to recreate an operator, we don't ever have to worry about creating
        // the links with it. Only matters for undoing.
        if (this.undoToggle) {
          // we need to somehow grab the point
          const links = this.workflowActionService.deletedLinks;
          this.undos.push(() => {this.workflowActionService.addOperatorAndLinks(value.deletedOperator,
            this.workflowActionService.getPoint(value.deletedOperator.operatorID), links); });

          if (this.clearRedo) {
            this.redos = [];
          }
        } else {
          this.redos.push(() => {this.workflowActionService.addOperator(value.deletedOperator,
            this.workflowActionService.getPoint(value.deletedOperator.operatorID)); });
        }
        this.workflowActionService.deletedLinks = [];
        this.workflowActionService.separateLink = true;
      }
    );
  }

  private handleLinkAdd(): void {
    this.testGraph.getLinkAddStream().subscribe(
      value => {
        if (this.workflowActionService.separateLink) {
          if (this.undoToggle) {
            this.undos.push(() => {this.workflowActionService.deleteLinkWithID(value.linkID); });
            if (this.clearRedo) {
              this.redos = [];
            }
          } else {
            this.redos.push(() => {this.workflowActionService.deleteLinkWithID(value.linkID); });
          }
        }
      }
    );
  }

  private handleLinkDelete(): void {
    this.testGraph.getLinkDeleteStream().subscribe(
      value => {
        // find a way to capture link deletion and operator deletion at the same time
        // Idea: have a new boolean toggle whenever we remove an operator. Set it to false
        // for like half a second, record any links that get deleted within that time.
        // Those links will get associated with that operator deletion and get stored as a
        // single command pattern rather than their separate deltions.
        // Do the same thing when adding back? So when we add the operator with the links nothing
        // weird happens
        // we don't even care specifically about LinkID, just the entire link
          // links are getting deleted first
        // figure out how to reset the array of things, probably move it to WorkflowActionService
        if (this.workflowActionService.separateLink) {
          if (this.undoToggle) {
            this.undos.push(() => {this.workflowActionService.addLink(value.deletedLink); });
            if (this.clearRedo) {
              this.redos = [];
            }
          } else {
            this.redos.push(() => {this.workflowActionService.addLink(value.deletedLink); });
          }
        } else {
          this.workflowActionService.deletedLinks.push(value.deletedLink);
        }
      }
    );
  }

  private handlePropertyChange(): void {
    this.testGraph.getOperatorPropertyChangeStream().subscribe(
      value => {
        // another problem: not all operators will trigger this when operator is created SOLVED
        if (this.undoToggle && this.changeProperty) {
          this.undos.push(() => {this.workflowActionService.changeOperatorProperty(value.operator.operatorID, value.oldProperty); });
          if (this.clearRedo) {
            this.redos = [];
          }
        } else if (this.changeProperty) {
          this.redos.push(() => {this.workflowActionService.changeOperatorProperty(value.operator.operatorID, value.oldProperty); });
        }
      }
    );
  }

  private handleOperatorDrag(): void {
    this.workflowActionService.getJointGraphWrapper().getJointOperatorCellDragStream().pipe(debounceTime(350)).subscribe(
      value => {
        // only want to trigger stuff in here when new action is performed, not from a stack
        if (this.undoToggle && this.dragToggle) {
          if (this.clearRedo) {
            this.redos = [];
          }
          const pointer = this.workflowActionService.pointsPointer.get(String(value.id));
          let points = this.workflowActionService.pointsUndo.get(String(value.id));
          if ((pointer || pointer === 0) && points) {
            // check to see if we're at the top of the stack
            if (pointer !== points.length - 1) {
              // we're cutting out all of the coordinates we no longer need after we do a new drag
              points = points.slice(0, pointer + 1);
            }

            // increment pointer
            this.workflowActionService.pointsPointer.set(String(value.id), pointer + 1);
            // add value to map
            points.push(value.attributes.position);
            this.workflowActionService.pointsUndo.set(String(value.id), points);
          }
          this.undos.push(() => {this.undoDrag(String(value.id)); });

        }
        this.dragToggle = true;
      }
    );
  }
  // lets us undo/redo dragging quickly
  private undoDrag(ID: string): void {
    this.workflowActionService.undoDragOperator(ID);
    this.redos.push(() => {this.redoDrag(ID); });
  }

  private redoDrag(ID: string): void {
    this.workflowActionService.redoDragOperator(ID);
    this.undos.push(() => {this.undoDrag(ID); });
  }
}
