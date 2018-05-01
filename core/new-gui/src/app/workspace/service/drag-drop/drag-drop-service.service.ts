import { WorkflowUtilService } from './../workflow-graph/util/workflow-util.service';
import { JointUIService } from './../joint-ui/joint-ui.service';
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';
import { OperatorPredicate } from '../../types/workflow-graph';

import * as joint from 'jointjs';

/**
 * The OperatorDragDropService class implements the behavior of dragging an operator label from the side bar
 *  and drop it as an operator box on to the main workflow editor.
 *
 * This behavoir is implemented using jQueryUI draggable and droppable.
 *  1. jQueryUI draggable allows providing a custom DOM element that is displayed when dragging around.
 *  2. the custom DOM element (called "flyPaper") is a JointJS paper that only contains one operator box and has the exact same size of it.
 *  3. when dragging ends, the temporary DOM element ("flyPaper") is destoryed by jQueryUI
 *  4. when dragging ends (operator is dropped), it will notify the observer of the event,
 *    the Operator UI Serivce is responsible for creating an operator at the place dropped.
 *
 * The method mentioned above is the best working way to implement this functionailtiy as of 02/2018.
 * Here are some other methods that have been tried but didn't work.
 *
 *  1. Using HTML5 native drag and drop API.
 *    This doesn't work because the problem of the "ghost image" (the image that follows the mouse when dragging).
 *    The native HTML5 drag/drop API requires that the "ghost" image must be visually the same as the original element.
 *    However, in our case, the dragging operator is not the same as the original element.
 *    There is **NO** workaround for this problem: see this post for details: https://kryogenix.org/code/browser/custom-drag-image.html
 *      (part of the post isn't exactly true on Chrome anymore because the Chrome itself changed)
 *    The HTML5 drag and Drop API itself is also considered a disaster: https://www.quirksmode.org/blog/archives/2009/09/the_html5_drag.html
 *
 *  2. Using some angular drag and drop libraries for Angular, for example:
 *    ng2-dnd: https://github.com/akserg/ng2-dnd
 *    ng2-dragula: https://github.com/valor-software/ng2-dragula
 *    ng-drag-drop: https://github.com/ObaidUrRehman/ng-drag-drop
 *
 *    These drag and drop libraries have the same ghost image problem mentioned above. Moreover, some of them are designed
 *      for moving a DOM element to another place by dragging and dropping, which is not we want.
 *
 * @author Zuozhi Wang
 *
 */
@Injectable()
export class DragDropServiceService {

  /** mapping of DOM Element ID to operatorType */
  private elementOperatorTypeMap = new Map<string, string>();
  /** the current operatorType of the operator being dragged */
  private currentOperatorType: string | undefined;


  /** Subject for operator dragging is started */
  private operatorDragStartedSubject = new Subject<{ operatorType: string }>();


  /**
   * Observable for operator dragging is started.
   * Contains an object with:
   *  - operatorType - the type of the operator dragged
   */
  // public operatorDragStarted = this.operatorDragStartedSubject.asObservable();


  /** Subject for operator is dropped on the main workflow editor (equivalent to dragging is stopped) */
  private operatorDroppedSubject = new Subject<{
    'operator': OperatorPredicate,
    'offset': { 'x': number, 'y': number }
  }>();

  /**
   * Observable for operator is dropped on the main workflow editor.
   * Contains an object with:
   *  - operatorType - the type of the operator dropped
   *  - offset.x - the x offset relative to document root
   *  - offset.y - the y offset relative to document root
   */
  // public operatorDroppedInEditor = this.operatorDroppedSubject.asObservable();

  constructor(
    private jointUIService: JointUIService,
    private workflowUtilService: WorkflowUtilService
  ) {
  }

  /**
   * This function is intended by be used by the operator labels to make the element draggable.
   * It also binds hanlder functions the following property or events:
   *  - helper: a function the DOM element to display when dragging to make it look like an operator
   *  - start: triggers when dragging starts
   *
   * more detail at jQuery UI draggable documentation: http://api.jqueryui.com/draggable/
   *
   * @param dragElementID the DOM Element ID
   * @param operatorType the operator type that the element corresponds to
   */
  public registerOperatorLabelDrag(dragElementID: string, operatorType: string): void {
    this.elementOperatorTypeMap.set(dragElementID, operatorType);

    // register callback functions for jquery UI
    jQuery('#' + dragElementID).draggable({
      helper: () => this.createFlyingOperatorElement(operatorType),
      start: (event: any, ui) => this.onOperatorDragStarted(event, ui)
    });
  }

  /**
   * This function should be only used by the Workflow Editor Componenet
   *  to register itself as a droppable area.
  */
  public registerWorkflowEditorDrop(dropElementID: string): void {
    jQuery('#' + dropElementID).droppable({
      drop: (event: any, ui) => this.onOperatorDropped(event, ui)
    });
  }

  /**
   * Creates a DOM Element that visually looks identical to the operator when dropped on main workflow editor
   *
   * This function temporarily creates a DOM element which contains a JointJS paper that has the exact size of the operator,
   *    then create the operator Element based on the operatorType and make it fully occupy the JointJS paper.
   *
   * The temporary JointJS paper element has ID "flyPaper". This DOM elememtn will be destroyed by jQueryUI when the dragging ends.
   *
   * @param operatorType - the type of the operator
   */
  private createFlyingOperatorElement(operatorType: string): JQuery<HTMLElement> {
    this.currentOperatorType = operatorType;

    // create a temporary ghost element
    jQuery('body').append('<div id="flyPaper" style="position:fixed;z-index:100;pointer-event:none;"></div>');

    // create an operator and get the UI element from the operator type
    const operator = this.workflowUtilService.getNewOperatorPredicate(operatorType);
    const operatorUIElement = this.jointUIService.getJointjsOperatorElement(operator, { x: 0, y: 0 });

    // create the jointjs model and paper of the ghost element
    const tempGhostModel = new joint.dia.Graph();
    const tempGhostPaper = new joint.dia.Paper({
      el: jQuery('#flyPaper'),
      width: JointUIService.DEFAULT_OPERATOR_WIDTH,
      height: JointUIService.DEFAULT_OPERATOR_HEIGHT,
      model: tempGhostModel,
      gridSize: 1
    });

    // add the operator JointJS element to the paper
    tempGhostModel.addCell(operatorUIElement);

    // return the jQuery Object of the DOM Element
    return jQuery('#flyPaper');
  }

  /**
   * Hanlder function for jQueryUI's drag started event.
   * It converts the event to the drag started Subject.
   *
   * @param event JQuery.Event type, although JQueryUI typing says the type is Event, the object's actual type is JQuery.Event
   * @param ui jQueryUI Draggable Event UI
   */
  private onOperatorDragStarted(event: JQuery.Event, ui: JQueryUI.DraggableEventUIParams): void {
    // get the operatorType based on the DOM element ID
    const operatorType = this.elementOperatorTypeMap.get(event.toElement.id);
    // set the currentOperatorType
    this.currentOperatorType = operatorType;
    // notify the subject of the event
    this.operatorDragStartedSubject.next({ 'operatorType': operatorType });
  }

  /**
   * Hanlder function for jQueryUI's drag started event.
   * It converts the event to the drag stopped Subject.
   * Notice that we view Drag Stopp is equivalent to
   *
   * @param event
   * @param ui
   */
  private onOperatorDropped(event: JQuery.Event, ui: JQueryUI.DraggableEventUIParams): void {
    console.log('on op dropped called');
    // notify the subject of the event
    // use ui.offset instead of ui.position because offset is relative to document root, where position is relative to parent element
    this.operatorDroppedSubject.next({
      operator: this.workflowGraphUtilsService.getNewOperatorPredicate(this.currentOperatorType),
      offset: {
        x: ui.offset.left,
        y: ui.offset.top
      }
    });
  }

}
