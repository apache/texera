import { Point, OperatorPredicate, OperatorLink } from './../../types/workflow-common.interface';
import { WorkflowActionService } from './../workflow-graph/model/workflow-action.service';
import { Observable } from 'rxjs/Observable';
import { WorkflowUtilService } from './../workflow-graph/util/workflow-util.service';
import { JointUIService } from './../joint-ui/joint-ui.service';
import { Injectable } from '@angular/core';
import { Subject } from 'rxjs/Subject';

import * as joint from 'jointjs';
import isEqual from 'lodash-es/isEqual';

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
export class DragDropService {

  private static readonly DRAG_DROP_TEMP_OPERATOR_TYPE = 'drag-drop-temp-operator-type';

  // distance threshold for suggesting operators before user dropped an operator
  private static readonly SUGGESTION_DISTANCE_THRESHOLD = 300;

  private readonly operatorSuggestionHighlightStream =  new Subject <string>();
  private readonly operatorSuggestionUnhighlightStream =  new Subject <string>();

  // currently suggested operator to link with
  private suggestionOperator: OperatorPredicate | undefined;

  // check whether the suggested operator is on the left or on the right.
  private isSuggestionOnLeft: boolean = false;

  /** mapping of DOM Element ID to operatorType */
  private elementOperatorTypeMap = new Map<string, string>();
  /** the current operatorType of the operator being dragged */
  private currentOperatorType = DragDropService.DRAG_DROP_TEMP_OPERATOR_TYPE;
  /** Subject for operator dragging is started */
  private operatorDragStartedSubject = new Subject<{ operatorType: string }>();

  /** Subject for operator is dropped on the main workflow editor (equivalent to dragging is stopped) */
  private operatorDroppedSubject = new Subject<{
    operatorType: string,
    offset: Point
  }>();

  constructor(
    private jointUIService: JointUIService,
    private workflowUtilService: WorkflowUtilService,
    private workflowActionService: WorkflowActionService
  ) {
    this.handleOperatorDropEvent();
  }

  /**
   * Handles the event of operator being dropped.
   * Adds the operator to the workflow graph at the same position of it being dropped.
   */
  public handleOperatorDropEvent(): void {
    this.getOperatorDropStream().subscribe(
      value => {
        // construct the operator from the drop stream value
        const operator = this.workflowUtilService.getNewOperatorPredicate(value.operatorType);
        /**
         * get the new drop coordinate of operator, when users drag or zoom the panel, to make sure the operator will
         drop on the right location.
         */

        const newOperatorOffset: Point = {
          x:  (value.offset.x - this.workflowActionService.getJointGraphWrapper().getDragOffset().x)
              / this.workflowActionService.getJointGraphWrapper().getZoomRatio(),
          y: (value.offset.y - this.workflowActionService.getJointGraphWrapper().getDragOffset().y)
              / this.workflowActionService.getJointGraphWrapper().getZoomRatio()
        };

        // add the operator
        this.workflowActionService.addOperator(operator, newOperatorOffset);

        // has suggestion and must auto-create the operator link between 2 operators.
        if (this.suggestionOperator !== undefined) {
          if (this.isSuggestionOnLeft) {
            this.workflowActionService.addLink(this.getNewOperatorLink(this.suggestionOperator, operator));
          } else {
            this.workflowActionService.addLink(this.getNewOperatorLink(operator, this.suggestionOperator));
          }

          // after the link is created, unhightlight the suggestion and reset suggestionOperator
          this.operatorSuggestionUnhighlightStream.next(this.suggestionOperator.operatorID);
          this.suggestionOperator = undefined;
        }
        // highlight the operator after adding the operator
        this.workflowActionService.getJointGraphWrapper().highlightOperator(operator.operatorID);
        // reset the current operator type to an non-exist type
        this.currentOperatorType = DragDropService.DRAG_DROP_TEMP_OPERATOR_TYPE;
      }
    );
  }

  /**
   * Gets an observable for operator dragging started event
   * Contains an object with:
   *  - operatorType - the type of the dragged operator
   */
  public getOperatorStartDragStream(): Observable<{ operatorType: string }> {
    return this.operatorDragStartedSubject.asObservable();
  }

  /**
   * Gets an observable for operator is dropped on the main workflow editor event
   * Contains an object with:
   *  - operatorType - the type of the operator dropped
   *  - offset - the x and y point where the operator is dropped (relative to document root)
   */
  public getOperatorDropStream(): Observable<{ operatorType: string, offset: Point }> {
    return this.operatorDroppedSubject.asObservable();
  }

  /**
   * Gets an observable for new suggestion event to highlight an operator to link with.
   *
   * Contains the operator ID to highlight for suggestion
   */
  public getOperatorSuggestionHighlightStream(): Observable<string> {
    return this.operatorSuggestionHighlightStream.asObservable();
  }

    /**
   * Gets an observable for removing suggestion event to unhighlight an operator
   *
   * Contains the operator ID to unhighlight to remove previous suggestion
   */
  public getOperatorSuggestionUnhighlightStream(): Observable<string> {
    return this.operatorSuggestionUnhighlightStream.asObservable();
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
      // declare event as type any because the jQueryUI type declaration is wrong
      // it should be of type JQuery.Event, which is incompatible with the the declared type Event
      start: (event: any, ui) => this.handleOperatorStartDrag(event, ui),
      // The draggable element will be created with the mouse starting point at the center
      cursorAt : {
        left: JointUIService.DEFAULT_OPERATOR_WIDTH / 2,
        top: JointUIService.DEFAULT_OPERATOR_HEIGHT / 2
      },
      stop: (event: any, ui) => {
        // this is to unhighlight the suggested operator when the user release mouse at other
        //  components than the workflow editor
        if (this.suggestionOperator !== undefined) {
          this.operatorSuggestionUnhighlightStream.next(this.suggestionOperator.operatorID);
          this.suggestionOperator = undefined;
        }
      }
    });
  }

  /**
   * This function should be only used by the Workflow Editor Componenet
   *  to register itself as a droppable area.
  */
  public registerWorkflowEditorDrop(dropElementID: string): void {
    jQuery('#' + dropElementID).droppable({
      drop: (event: any, ui) => this.handleOperatorDrop(event, ui)
    });
  }

  /**
   * Creates a DOM Element that visually looks identical to the operator when dropped on main workflow editor
   *
   * This function temporarily creates a DOM element which contains a JointJS paper that has the exact size of the operator,
   *    then create the operator Element based on the operatorType and make it fully occupy the JointJS paper.
   *
   * The temporary JointJS paper element has ID "flyingJointPaper". This DOM elememtn will be destroyed by jQueryUI when the dragging ends.
   *
   * @param operatorType - the type of the operator
   */
  private createFlyingOperatorElement(operatorType: string): JQuery<HTMLElement> {
    // set the current operator type from an nonexist placeholder operator type
    //  to the operator type being dragged
    this.currentOperatorType = operatorType;

    // create a temporary ghost element
    jQuery('body').append('<div id="flyingJointPaper" style="position:fixed;z-index:100;pointer-event:none;"></div>');

    // create an operator and get the UI element from the operator type
    const operator = this.workflowUtilService.getNewOperatorPredicate(operatorType);
    const operatorUIElement = this.jointUIService.getJointOperatorElement(operator, { x: 0, y: 0 });

    // create the jointjs model and paper of the ghost element
    const tempGhostModel = new joint.dia.Graph();
    const tempGhostPaper = new joint.dia.Paper({
      el: jQuery('#flyingJointPaper'),
      width: JointUIService.DEFAULT_OPERATOR_WIDTH,
      height: JointUIService.DEFAULT_OPERATOR_HEIGHT,
      model: tempGhostModel,
    });

    // add the operator JointJS element to the paper
    tempGhostModel.addCell(operatorUIElement);

    // return the jQuery object of the DOM Element
    return jQuery('#flyingJointPaper');
  }

  /**
   * Hanlder function for jQueryUI's drag started event.
   * It converts the event to the drag started Subject.
   *
   * @param event JQuery.Event type, although JQueryUI typing says the type is Event, the object's actual type is JQuery.Event
   * @param ui jQueryUI Draggable Event UI
   */
  private handleOperatorStartDrag(event: JQuery.Event, ui: JQueryUI.DraggableEventUIParams): void {
    const eventElement = event.target;
    if (!(eventElement instanceof Element)) {
      throw new Error('Incorrect type: in most cases, this element is type Element');
    }
    if (eventElement === undefined) {
      throw new Error('drag and drop: cannot find element when drag is started');
    }
    // get the operatorType based on the DOM element ID
    const operatorType = this.elementOperatorTypeMap.get(eventElement.id);
    if (operatorType === undefined) {
      throw new Error(`drag and drop: cannot find operator type ${operatorType} from DOM element ${eventElement}`);
    }
    // set the currentOperatorType
    this.currentOperatorType = operatorType;
    // notify the subject of the event
    this.operatorDragStartedSubject.next({ operatorType });

    // begin the operator link recommendation process
    this.handleOperatorRecommendationOnDrag();
  }

  /**
   * Hanlder function for jQueryUI's drag stopped event.
   * It converts the event to the drag stopped Subject.
   * Notice that we view Drag Stopped is equivalent to the operator being Dropped
   *
   * @param event
   * @param ui
   */
  private handleOperatorDrop(event: JQuery.Event, ui: JQueryUI.DraggableEventUIParams): void {
    // notify the subject of the event
    // use ui.offset instead of ui.position because offset is relative to document root, where position is relative to parent element
    this.operatorDroppedSubject.next({
      operatorType: this.currentOperatorType,
      offset: {
        x: ui.offset.left,
        y: ui.offset.top
      }
    });
  }

  /**
   * This is the handler for recommending operator to link to when
   *  the user is dragging the ghost operator before dropping.
   *
   */
  private handleOperatorRecommendationOnDrag(): void {
    let isOperatorDropped = false;

    Observable.fromEvent<MouseEvent>(window, 'mouseup').first()
      .subscribe(
        () => isOperatorDropped = true,
        error => console.error(error)
      );

    Observable.fromEvent<MouseEvent>(window, 'mousemove')
      .map(value => [value.clientX, value.clientY])
      .filter(() => !isOperatorDropped)
      .subscribe(mouseCoordinates => {
          const currentMouseCoordinates = {x: mouseCoordinates[0], y: mouseCoordinates[1]};
          this.findClosestOperator(currentMouseCoordinates);
      },
      error => console.error(error)
    );
  }

  /**
   * This method go through all the existing operators and find the operator that has
   *  the closest distance from the cursor when dragging new operator and has distance that
   *  does not exceed the threshold defined.
   *
   * When the cursor is on right of other operators, it will check
   *  1. whether the distance between the cursor and a operator is smaller than the min-distance found
   *  2. whether the operator user is currently is dragging has input port available for other operator
   *      to add a link from its output port to the input port
   *  3. whether other operators have at least one output port available to add
   *
   * When the cursor is on left of other operators, it will check
   *  1. whether the distance between the cursor and a operator is smaller than the min-distance found
   *  2. whether the operator user is currently is dragging has output port available for the current operator
   *      to add a link from this output port to the input port of other operators
   *  3. whether other operators have at least one input port available to add
   *
   * When an operator is selected among all the operators, this method will unhighlight the previously
   *  highlighted operator if they are different and highlight the newly selected operator.
   *
   * If there is no suggestion found (means no operator satisfied the suggestion constraints), remove
   *  the highlighted operator if there exist.
   *
   * @param mouseCoordinate current cursor position when dragging ghost operator
   */
  private findClosestOperator(mouseCoordinate: Point): void {
    const curruntOperator = this.workflowUtilService.getNewOperatorPredicate(this.currentOperatorType);
    const operatorList = this.workflowActionService.getTexeraGraph().getAllOperators();
    const operatorLinks = this.workflowActionService.getTexeraGraph().getAllLinks();

    let minDistance = Number.MAX_VALUE; // keep tracking the closest operator
    let newSuggestionOperator: OperatorPredicate | undefined;

    operatorList.forEach(operator => {
      const operatorPosition = this.workflowActionService.getJointGraphWrapper().getOperatorPosition(operator.operatorID);
      const distanceFromCurrentOperator = Math.sqrt((mouseCoordinate.x - operatorPosition.x) ** 2
      + (mouseCoordinate.y - operatorPosition.y) ** 2);

      if (distanceFromCurrentOperator < minDistance && distanceFromCurrentOperator < DragDropService.SUGGESTION_DISTANCE_THRESHOLD) {

        const existPortCountFromSource = operatorLinks
          .filter(link => link.source.operatorID === operator.operatorID).length;
        const existPortCountFromTarget = operatorLinks
          .filter(link => link.target.operatorID === operator.operatorID).length;

        // check left or right, if current operator can add input or output ports, and the operator
        //  we are checking to suggest has output/input ports available to add
        if ((operatorPosition.x < mouseCoordinate.x && curruntOperator.inputPorts.length > 0 &&
          existPortCountFromSource !== operator.outputPorts.length) ||
            (operatorPosition.x >= mouseCoordinate.x && curruntOperator.outputPorts.length > 0 &&
              existPortCountFromTarget !== operator.inputPorts.length)) {

            // check if the dragging operator is on the left or right of the selected operator
            this.isSuggestionOnLeft = operatorPosition.x < mouseCoordinate.x ? true : false;

            newSuggestionOperator = operator;
            minDistance = distanceFromCurrentOperator;
          }
      }
    });

    // if no suggestion operator is found and there is currently a suggestion, remove that suggestion
    if (newSuggestionOperator === undefined && this.suggestionOperator !== undefined) {
      this.operatorSuggestionUnhighlightStream.next(this.suggestionOperator.operatorID);
      this.suggestionOperator = undefined;
    } else if (newSuggestionOperator !== undefined && !isEqual(newSuggestionOperator, this.suggestionOperator)) {
      // if there is a new suggested operator, unhighlight the previously suggested and highlight the new operator
      if (this.suggestionOperator !== undefined) {
        this.operatorSuggestionUnhighlightStream.next(this.suggestionOperator.operatorID);
      }
      this.suggestionOperator = newSuggestionOperator;
      this.operatorSuggestionHighlightStream.next(newSuggestionOperator.operatorID);
    }
  }

    /**
   * This method will use an unique ID and 2 operator predicate to create and return
   *  a new OperatorLink with initialized properties for the ports
   *
   * @param sourceOperator
   * @param targetOperator
   */
  private getNewOperatorLink(sourceOperator: OperatorPredicate, targetOperator: OperatorPredicate): OperatorLink {
    const operatorLinks = this.workflowActionService.getTexeraGraph().getAllLinks();

    // find the port that has not being connected
    const allPortsFromSource = operatorLinks
      .filter(link => link.source.operatorID === sourceOperator.operatorID)
      .map(link => link.source.portID);

    const allPortsFromTarget = operatorLinks
      .filter(link => link.target.operatorID === targetOperator.operatorID)
      .map(link => link.target.portID);

    const validSourcePortsID = sourceOperator.outputPorts.filter(portID => !allPortsFromSource.includes(portID));
    const validTargetPortsID = targetOperator.inputPorts.filter(portID => !allPortsFromTarget.includes(portID));

    const linkID = this.workflowUtilService.getLinkRandomUUID();
    const source = { operatorID: sourceOperator.operatorID, portID: validSourcePortsID[0]};
    const target = { operatorID: targetOperator.operatorID, portID: validTargetPortsID[0]};
    return { linkID, source, target };
  }
}
