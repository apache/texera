import { OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { fromEvent, Observable, Subject } from "rxjs";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { Injectable } from "@angular/core";
import TinyQueue from "tinyqueue";

import * as joint from "jointjs";

// if jQuery needs to be used: 1) use jQuery instead of `$`, and
// 2) always add this import statement even if TypeScript doesn't show an error https://github.com/Microsoft/TypeScript/issues/22016
import * as jQuery from "jquery";
// this is the property way to import jquery-ui to Angular, make sure to import it after import jQuery
// https://stackoverflow.com/questions/43323515/error-when-using-jqueryui-with-typescript-and-definitelytyped-definition-file
// this approach is better than including it in `scripts` in `angular.json` because it avoids loading jQuery overrides jQuery UI
import "jquery-ui-dist/jquery-ui";
import { filter, first, map } from "rxjs/operators";

/**
 * The OperatorDragDropService class implements the behavior of dragging an operator label from the side bar
 *  and drop it as an operator box on to the main workflow editor.
 *
 * This behavior is implemented using jQueryUI draggable and droppable.
 *  1. jQueryUI draggable allows providing a custom DOM element that is displayed when dragging around.
 *  2. the custom DOM element (called "flyPaper") is a JointJS paper that only contains one operator box and has the exact same size of it.
 *  3. when dragging ends, the temporary DOM element ("flyPaper") is destroyed by jQueryUI
 *  4. when dragging ends (operator is dropped), it will notify the observer of the event,
 *    the Operator UI Service is responsible for creating an operator at the place dropped.
 *
 * The method mentioned above is the best working way to implement this functionality as of 02/2018.
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
@Injectable({
  providedIn: "root",
})
export class DragDropService {
  // distance threshold for suggesting operators before user dropped an operator
  public static readonly SUGGESTION_DISTANCE_THRESHOLD = 300;

  private static readonly DRAG_DROP_TEMP_ELEMENT_ID = "drag-drop-temp-element-id";
  private static readonly DRAG_DROP_TEMP_OPERATOR_TYPE = "drag-drop-temp-operator-type";

  private readonly operatorSuggestionHighlightStream = new Subject<string>();
  private readonly operatorSuggestionUnhighlightStream = new Subject<string>();

  // current suggested operators to link with
  private suggestionInputs: OperatorPredicate[] = [];
  private suggestionOutputs: OperatorPredicate[] = [];

  /** mapping of DOM Element ID to operatorType */
  private elementOperatorTypeMap = new Map<string, string>();
  /** the current element ID of the operator being dragged */
  private currentDragElementID = DragDropService.DRAG_DROP_TEMP_ELEMENT_ID;
  /** the current operatorType of the operator being dragged */
  private currentOperatorType = DragDropService.DRAG_DROP_TEMP_OPERATOR_TYPE;
  /** Subject for operator dragging is started */
  private operatorDragStartedSubject = new Subject<{ operatorType: string }>();

  /** Subject for operator is dropped on the main workflow editor (equivalent to dragging is stopped) */
  private operatorDroppedSubject = new Subject<{
    operatorType: string;
    offset: Point;
    dragElementID: string;
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
   * **Proposal**: currently doesn't support multiple links to/from same suggested input/output
   */
  public handleOperatorDropEvent(): void {
    this.getOperatorDropStream().subscribe(value => {
      // construct the operator from the drop stream value
      const operator = this.workflowUtilService.getNewOperatorPredicate(value.operatorType);

      let coordinates: Point | undefined = this.workflowActionService
        .getJointGraphWrapper()
        .getMainJointPaper()
        ?.pageToLocalPoint(value.offset.x, value.offset.y);
      if (!coordinates) {
        coordinates = value.offset;
      }

      const scale = this.workflowActionService.getJointGraphWrapper().getMainJointPaper()?.scale() ?? { sx: 1, sy: 1 };

      const newOperatorOffset = {
        x: coordinates.x / scale.sx,
        y: coordinates.y / scale.sy,
      };

      const operatorsAndPositions: { op: OperatorPredicate; pos: Point }[] = [{ op: operator, pos: newOperatorOffset }];
      // create new links from suggestions
      const newLinks: OperatorLink[] = this.getNewOperatorLinks(
        operator,
        this.suggestionInputs,
        this.suggestionOutputs
      );

      this.workflowActionService.addOperatorsAndLinks(operatorsAndPositions, newLinks);
      this.resetSuggestions();

      // reset the current operator type to an non-exist type
      this.currentDragElementID = DragDropService.DRAG_DROP_TEMP_ELEMENT_ID;
      this.currentOperatorType = DragDropService.DRAG_DROP_TEMP_OPERATOR_TYPE;
    });
  }

  /**
   * Gets an observable for operator is dropped on the main workflow editor event
   * Contains an object with:
   *  - operatorType - the type of the operator dropped
   *  - offset - the x and y point where the operator is dropped (relative to document root)
   */
  public getOperatorDropStream(): Observable<{
    operatorType: string;
    offset: Point;
    dragElementID: string;
  }> {
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
   * It also binds handler functions the following property or events:
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
    jQuery("#" + dragElementID).draggable({
      helper: () => this.createFlyingOperatorElement(dragElementID, operatorType),
      // declare event as type any because the jQueryUI type declaration is wrong
      // it should be of type JQuery.Event, which is incompatible with the the declared type Event
      start: (event: JQueryEventObject, ui: JQueryUI.DraggableEventUIParams) => this.handleOperatorStartDrag(event, ui),
      // The draggable element will be created with the mouse starting point at the center
      cursorAt: {
        left: JointUIService.DEFAULT_OPERATOR_WIDTH / 2,
        top: JointUIService.DEFAULT_OPERATOR_HEIGHT / 2,
      },
      stop: (event: JQueryEventObject, ui: JQueryUI.DraggableEventUIParams) => {
        this.resetSuggestions();
      },
      // prevents dragging from starting on elements of class disable-drag-drop
      cancel: ".disable-drag-drop",
    });
  }

  /**
   * This function should be only used by the Workflow Editor Component
   *  to register itself as a droppable area.
   */
  public registerWorkflowEditorDrop(dropElementID: string): void {
    jQuery("#" + dropElementID).droppable({
      drop: (event: any, ui) => this.handleOperatorDrop(event, ui),
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
  private createFlyingOperatorElement(dragElementID: string, operatorType: string): JQuery<HTMLElement> {
    // set the current operator type from an nonexist placeholder operator type
    //  to the operator type being dragged
    this.currentDragElementID = dragElementID;
    this.currentOperatorType = operatorType;

    // create a temporary ghost element
    jQuery("body").append("<div id=\"flyingJointPaper\" style=\"position:fixed;z-index:1001;pointer-event:none;\"></div>");

    // create an operator and get the UI element from the operator type
    const operator = this.workflowUtilService.getNewOperatorPredicate(operatorType);
    const operatorUIElement = this.jointUIService.getJointOperatorElement(operator, { x: 0, y: 0 });

    // create the jointjs model and paper of the ghost element
    const tempGhostModel = new joint.dia.Graph();
    new joint.dia.Paper({
      el: jQuery("#flyingJointPaper"),
      width: JointUIService.DEFAULT_OPERATOR_WIDTH,
      height: JointUIService.DEFAULT_OPERATOR_HEIGHT,
      model: tempGhostModel,
    });
    // add the operator JointJS element to the paper
    tempGhostModel.addCell(operatorUIElement);

    // return the jQuery object of the DOM Element
    return jQuery("#flyingJointPaper");
  }

  /**
   * Handler function for jQueryUI's drag started event.
   * It converts the event to the drag started Subject.
   *
   * @param event JQuery.Event type, although JQueryUI typing says the type is Event, the object's actual type is JQuery.Event
   * @param ui jQueryUI Draggable Event UI
   */
  private handleOperatorStartDrag(event: Event, ui: JQueryUI.DraggableEventUIParams): void {
    const eventElement = event.target;
    if (!(eventElement instanceof Element)) {
      throw new Error("Incorrect type: in most cases, this element is type Element");
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
   * Handler function for jQueryUI's drag stopped event.
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
        y: ui.offset.top,
      },
      dragElementID: this.currentDragElementID,
    });
  }

  /**
   * This is the handler for recommending operator to link to when
   *  the user is dragging the ghost operator before dropping.
   *
   */
  private handleOperatorRecommendationOnDrag(): void {
    const currentOperator = this.workflowUtilService.getNewOperatorPredicate(this.currentOperatorType);
    let isOperatorDropped = false;

    fromEvent<MouseEvent>(window, "mouseup")
      .pipe(first())
      .subscribe(() => (isOperatorDropped = true));

    fromEvent<MouseEvent>(window, "mousemove")
      .pipe(
        map(value => [value.clientX, value.clientY]),
        filter(() => !isOperatorDropped)
      )
      .subscribe(mouseCoordinates => {
        const currentMouseCoordinates = {
          x: mouseCoordinates[0],
          y: mouseCoordinates[1],
        };

        let coordinates: Point | undefined = this.workflowActionService
          .getJointGraphWrapper()
          .getMainJointPaper()
          ?.pageToLocalPoint(currentMouseCoordinates.x, currentMouseCoordinates.y);
        if (!coordinates) {
          coordinates = currentMouseCoordinates;
        }

        let scale: { sx: number; sy: number } | undefined = this.workflowActionService
          .getJointGraphWrapper()
          .getMainJointPaper()
          ?.scale();
        if (scale === undefined) {
          scale = { sx: 1, sy: 1 };
        }

        const scaledMouseCoordinates = {
          x: coordinates.x / scale.sx,
          y: coordinates.y / scale.sy,
        };

        // search for nearby operators as suggested input/output operators
        let newInputs, newOutputs: OperatorPredicate[];
        [newInputs, newOutputs] = this.findClosestOperators(scaledMouseCoordinates, currentOperator);
        // update highlighting class vars to reflect new input/output operators
        this.updateHighlighting(this.suggestionInputs.concat(this.suggestionOutputs), newInputs.concat(newOutputs));
        // assign new suggestions
        [this.suggestionInputs, this.suggestionOutputs] = [newInputs, newOutputs];
      });
  }

  /**
   * Finds nearby operators that can input to currentOperator and accept it's outputs.
   *
   * Only looks for inputs left of mouseCoordinate/ outputs right of mouseCoordinate.
   * Only looks for operators within distance DragDropService.SUGGESTION_DISTANCE_THRESHOLD.
   * **Warning**: assumes operators only output one port each (IE always grabs 3 operators for 3 input ports
   * even if first operator has 3 free outputs to match 3 inputs)
   * @mouseCoordinate is the location of the currentOperator on the JointGraph when dragging ghost operator
   * @currentOperator is the current operator, used to determine how many inputs and outputs to search for
   * @returns [[inputting-ops ...], [output-accepting-ops ...]]
   */
  private findClosestOperators(
    mouseCoordinate: Point,
    currentOperator: OperatorPredicate
  ): [OperatorPredicate[], OperatorPredicate[]] {
    const operatorLinks = this.workflowActionService.getTexeraGraph().getAllLinks();
    const operatorList = this.workflowActionService
      .getTexeraGraph()
      .getAllOperators()
      .filter(
        operator => !this.workflowActionService.getOperatorGroup().getGroupByOperator(operator.operatorID)?.collapsed
      );

    const numInputOps: number = currentOperator.inputPorts.length;
    const numOutputOps: number = currentOperator.outputPorts.length;

    // These two functions are a performance concern
    const hasFreeOutputPorts = (operator: OperatorPredicate): boolean => {
      return (
        operatorLinks.filter(link => link.source.operatorID === operator.operatorID).length <
        operator.outputPorts.length
      );
    };
    const hasFreeInputPorts = (operator: OperatorPredicate): boolean => {
      return (
        operatorLinks.filter(link => link.target.operatorID === operator.operatorID).length < operator.inputPorts.length
      );
    };

    // closest operators sorted least to greatest by distance using priority queue
    const compare = (
      a: { op: OperatorPredicate; dist: number },
      b: { op: OperatorPredicate; dist: number }
    ): number => {
      return b.dist - a.dist;
    };
    const inputOps: TinyQueue<{ op: OperatorPredicate; dist: number }> = new TinyQueue([], compare);
    const outputOps: TinyQueue<{ op: OperatorPredicate; dist: number }> = new TinyQueue([], compare);

    const greatestDistance = (queue: TinyQueue<{ op: OperatorPredicate; dist: number }>): number => {
      const greatest = queue.peek();
      if (greatest) {
        return greatest.dist;
      } else {
        return 0;
      }
    };

    // for each operator, check if in range/has free ports/is on the right side/is closer than prev closest ops/
    operatorList.forEach(operator => {
      const operatorPosition = this.workflowActionService
        .getJointGraphWrapper()
        .getElementPosition(operator.operatorID);
      const distanceFromCurrentOperator = Math.sqrt(
        (mouseCoordinate.x - operatorPosition.x) ** 2 + (mouseCoordinate.y - operatorPosition.y) ** 2
      );
      if (distanceFromCurrentOperator < DragDropService.SUGGESTION_DISTANCE_THRESHOLD) {
        if (
          numInputOps > 0 &&
          operatorPosition.x < mouseCoordinate.x &&
          (inputOps.length < numInputOps || distanceFromCurrentOperator < greatestDistance(inputOps)) &&
          hasFreeOutputPorts(operator)
        ) {
          inputOps.push({ op: operator, dist: distanceFromCurrentOperator });
          if (inputOps.length > numInputOps) {
            inputOps.pop();
          }
        } else if (
          numOutputOps > 0 &&
          operatorPosition.x > mouseCoordinate.x &&
          (outputOps.length < numOutputOps || distanceFromCurrentOperator < greatestDistance(outputOps)) &&
          hasFreeInputPorts(operator)
        ) {
          outputOps.push({ op: operator, dist: distanceFromCurrentOperator });
          if (outputOps.length > numOutputOps) {
            outputOps.pop();
          }
        }
      }
    });
    return [<OperatorPredicate[]>inputOps.data.map(x => x.op), <OperatorPredicate[]>outputOps.data.map(x => x.op)];
  }

  /**
   * Updates highlighted operators based on the diff between prev
   *
   * @param prevHighlights are highlighted (some may be unhighlighted)
   * @param newHighlights will be highlighted after execution
   */
  private updateHighlighting(prevHighlights: OperatorPredicate[], newHighlights: OperatorPredicate[]) {
    // unhighlight ops in prevHighlights but not in newHighlights
    prevHighlights
      .filter(operator => !newHighlights.includes(operator))
      .forEach(operator => {
        this.operatorSuggestionUnhighlightStream.next(operator.operatorID);
      });

    // highlight ops in newHghlights but not in prevHighlights
    newHighlights
      .filter(operator => !prevHighlights.includes(operator))
      .forEach(operator => {
        this.operatorSuggestionHighlightStream.next(operator.operatorID);
      });
  }

  /**  Unhighlights suggestions and clears suggestion lists */
  private resetSuggestions(): void {
    this.updateHighlighting(this.suggestionInputs.concat(this.suggestionOutputs), []);
    this.suggestionInputs = [];
    this.suggestionOutputs = [];
  }

  /**
   * This method will use an unique ID and 2 operator predicate to create and return
   *  a new OperatorLink with initialized properties for the ports.
   * **Warning** links created w/o spacial awareness. May connect two distant ports when it makes more sense to connect closer ones'
   * @param sourceOperator gives output
   * @param targetOperator accepts input
   * @param OperatorLinks optionally specify extant links (used to find which ports are occupied), defaults to all links.
   */
  private getNewOperatorLink(
    sourceOperator: OperatorPredicate,
    targetOperator: OperatorPredicate,
    operatorLinks?: OperatorLink[]
  ): OperatorLink {
    if (operatorLinks === undefined) {
      operatorLinks = this.workflowActionService.getTexeraGraph().getAllLinks();
    }
    // find the port that has not being connected
    const allPortsFromSource = operatorLinks
      .filter(link => link.source.operatorID === sourceOperator.operatorID)
      .map(link => link.source.portID);

    const allPortsFromTarget = operatorLinks
      .filter(link => link.target.operatorID === targetOperator.operatorID)
      .map(link => link.target.portID);

    const validSourcePortsID = sourceOperator.outputPorts.filter(port => !allPortsFromSource.includes(port.portID));
    const validTargetPortsID = targetOperator.inputPorts.filter(port => !allPortsFromTarget.includes(port.portID));

    const linkID = this.workflowUtilService.getLinkRandomUUID();
    const source = {
      operatorID: sourceOperator.operatorID,
      portID: validSourcePortsID[0].portID,
    };
    const target = {
      operatorID: targetOperator.operatorID,
      portID: validTargetPortsID[0].portID,
    };
    return { linkID, source, target };
  }

  /**
   *Get many links to one central "hub" operator
   * @param hubOperator
   * @param inputOperators
   * @param receiverOperators
   */
  private getNewOperatorLinks(
    hubOperator: OperatorPredicate,
    inputOperators: OperatorPredicate[],
    receiverOperators: OperatorPredicate[]
  ): OperatorLink[] {
    // remember newly created links to prevent multiple link assignment to same port
    const occupiedLinks: OperatorLink[] = this.workflowActionService.getTexeraGraph().getAllLinks();
    const newLinks: OperatorLink[] = [];
    const graph = this.workflowActionService.getJointGraphWrapper();

    // sort ops by height, in order to pair them with ports closest to them
    // assumes that for an op with multiple input/output ports, ports in op.inputPorts/outPutports are rendered
    //              [first ... last] => [North ... South]
    const heightSortedInputs: OperatorPredicate[] = inputOperators
      .slice(0)
      .sort((op1, op2) => graph.getElementPosition(op1.operatorID).y - graph.getElementPosition(op2.operatorID).y);
    const heightSortedOutputs: OperatorPredicate[] = receiverOperators
      .slice(0)
      .sort((op1, op2) => graph.getElementPosition(op1.operatorID).y - graph.getElementPosition(op2.operatorID).y);

    // if new operator has suggested links, create them
    if (heightSortedInputs !== undefined) {
      heightSortedInputs.forEach(inputOperator => {
        const newLink = this.getNewOperatorLink(inputOperator, hubOperator, occupiedLinks);
        newLinks.push(newLink);
        occupiedLinks.push(newLink);
      });
    }
    if (heightSortedOutputs !== undefined) {
      heightSortedOutputs.forEach(outputOperator => {
        const newLink = this.getNewOperatorLink(hubOperator, outputOperator, occupiedLinks);
        newLinks.push(newLink);
        occupiedLinks.push(newLink);
      });
    }

    return newLinks;
  }
}
