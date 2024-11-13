import { OperatorLink, OperatorPredicate, Point } from "../../types/workflow-common.interface";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { fromEvent, Observable, Subject } from "rxjs";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { JointUIService } from "../joint-ui/joint-ui.service";
import { Injectable } from "@angular/core";
import { filter, first, map } from "rxjs/operators";
import TinyQueue from "tinyqueue";
import * as joint from "jointjs";

@Injectable({
  providedIn: "root",
})
export class DragDropService {
  public static readonly SUGGESTION_DISTANCE_THRESHOLD = 300;
  private op!: OperatorPredicate;
  private operatorDroppedSubject = new Subject<void>();
  private readonly operatorSuggestionHighlightStream = new Subject<string>();
  private readonly operatorSuggestionUnhighlightStream = new Subject<string>();
  private suggestionInputs: OperatorPredicate[] = [];
  private suggestionOutputs: OperatorPredicate[] = [];

  constructor(
    private jointUIService: JointUIService,
    private workflowUtilService: WorkflowUtilService,
    private workflowActionService: WorkflowActionService
  ) {}

  public dragStarted(operatorType: string): void {
    this.op = this.workflowUtilService.getNewOperatorPredicate(operatorType);
    const scale = this.workflowActionService.getJointGraphWrapper().getMainJointPaper()?.scale().sx ?? 1;
    new joint.dia.Paper({
      el: document.getElementById("flyingOP")!,
      width: JointUIService.DEFAULT_OPERATOR_WIDTH * scale,
      height: JointUIService.DEFAULT_OPERATOR_HEIGHT * scale,
      model: new joint.dia.Graph().addCell(this.jointUIService.getJointOperatorElement(this.op, { x: 0, y: 0 })),
    }).scale(scale);
    this.handleOperatorRecommendationOnDrag();
  }

  public dragDropped(dropPoint: Point): void {
    const coordinates = this.workflowActionService
      .getJointGraphWrapper()
      .getMainJointPaper()
      ?.pageToLocalPoint(dropPoint.x, dropPoint.y)!;
    const newLinks = this.getNewOperatorLinks(this.op, this.suggestionInputs, this.suggestionOutputs);
    this.workflowActionService.addOperatorsAndLinks([{ op: this.op, pos: coordinates }], newLinks);
    this.resetSuggestions();
    this.operatorDroppedSubject.next();
  }

  get operatorDropStream() {
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
   * This is the handler for recommending operator to link to when
   *  the user is dragging the ghost operator before dropping.
   *
   */
  private handleOperatorRecommendationOnDrag(): void {
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
        [newInputs, newOutputs] = this.findClosestOperators(scaledMouseCoordinates, this.op);
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
    const operatorList = this.workflowActionService.getTexeraGraph().getAllOperators();

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
   * @param operatorLinks optionally specify extant links (used to find which ports are occupied), defaults to all links.
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
