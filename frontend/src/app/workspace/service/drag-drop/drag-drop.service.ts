/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

    // Check if the operator is dropped on top of an existing edge
    const intersectedLink = this.findIntersectedLink(coordinates);

    let newLinks: OperatorLink[];
    if (intersectedLink) {
      newLinks = this.createEdgeReconnectionLinks(this.op, intersectedLink);
    } else {
      newLinks = this.getNewOperatorLinks(this.op, this.suggestionInputs, this.suggestionOutputs);
    }

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
    let currentIntersectedLink: OperatorLink | null = null;

    fromEvent<MouseEvent>(window, "mouseup")
      .pipe(first())
      .subscribe(() => {
        isOperatorDropped = true;
        // Clear any edge intersection highlighting when drag ends
        if (currentIntersectedLink) {
          this.clearEdgeIntersectionHighlight(currentIntersectedLink);
          currentIntersectedLink = null;
        }
      });

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

    // Edge intersection detection
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

        // Only check for edge intersection if the operator can be inserted into edges
        const hasInputPorts = this.op.inputPorts.length > 0;
        const hasOutputPorts = this.op.outputPorts.length > 0;

        let intersectedLink: OperatorLink | null = null;

        if (hasInputPorts && hasOutputPorts) {
          // Check for edge intersection for visual feedback
          intersectedLink = this.findIntersectedLink(coordinates);
        }

        // Update edge intersection highlighting only when it changes
        if (intersectedLink !== currentIntersectedLink) {
          // Clear previous highlighting
          if (currentIntersectedLink) {
            this.clearEdgeIntersectionHighlight(currentIntersectedLink);
          }

          // Add new highlighting
          if (intersectedLink) {
            this.highlightEdgeIntersection(intersectedLink);
          }

          currentIntersectedLink = intersectedLink;
        }
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

  /**
   * Finds if the dropped operator intersects with any existing link on the workflow graph.
   * This checks if the operator's bounding box intersects with the edge, not just the cursor position.
   *
   * @param dropPoint The point where the operator is currently being dragged
   * @returns The intersected OperatorLink if found, null otherwise
   */
  private findIntersectedLink(dropPoint: Point): OperatorLink | null {
    const allLinks = this.workflowActionService.getTexeraGraph().getAllLinks();
    const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();

    if (!paper) {
      return null;
    }

    // Get operator dimensions for bounding box calculation
    const operatorWidth = JointUIService.DEFAULT_OPERATOR_WIDTH;
    const operatorHeight = JointUIService.DEFAULT_OPERATOR_HEIGHT;

    // Create operator bounding box (centered on drop point)
    const operatorBounds = {
      x: dropPoint.x - operatorWidth / 2,
      y: dropPoint.y - operatorHeight / 2,
      width: operatorWidth,
      height: operatorHeight,
    };

    for (const link of allLinks) {
      const jointLink = paper.getModelById(link.linkID) as joint.dia.Link;
      if (!jointLink) {
        continue;
      }

      const linkView = paper.findViewByModel(jointLink) as joint.dia.LinkView;
      if (!linkView) {
        continue;
      }

      // Get the path of the link
      const pathElement = linkView.el.querySelector(".connection") as SVGPathElement;
      if (!pathElement) {
        continue;
      }

      // Check if the operator bounding box intersects with the link path
      if (this.doesOperatorIntersectPath(operatorBounds, pathElement)) {
        return link;
      }
    }

    return null;
  }

  /**
   * Checks if an operator's bounding box intersects with an SVG path element.
   *
   * @param operatorBounds The bounding box of the operator
   * @param pathElement The SVG path element representing the link
   * @returns True if the operator intersects with the path, false otherwise
   */
  private doesOperatorIntersectPath(
    operatorBounds: { x: number; y: number; width: number; height: number },
    pathElement: SVGPathElement
  ): boolean {
    const pathLength = pathElement.getTotalLength();

    const samples = Math.min(20, Math.max(5, Math.floor(pathLength / 20)));

    for (let i = 0; i <= samples; i++) {
      const lengthRatio = (i / samples) * pathLength;
      const pathPoint = pathElement.getPointAtLength(lengthRatio);

      // Check if this point on the path is within the operator's bounding box
      if (
        pathPoint.x >= operatorBounds.x &&
        pathPoint.x <= operatorBounds.x + operatorBounds.width &&
        pathPoint.y >= operatorBounds.y &&
        pathPoint.y <= operatorBounds.y + operatorBounds.height
      ) {
        return true;
      }
    }

    return false;
  }

  /**
   * Creates new links to reconnect operators when an operator is dropped on an edge.
   * This removes the original link and creates two new links: one from the source to the new operator,
   * and one from the new operator to the original target.
   *
   * @param newOperator The operator being inserted into the edge
   * @param intersectedLink The link that was intersected
   * @returns Array of new OperatorLink objects for reconnection
   */
  private createEdgeReconnectionLinks(newOperator: OperatorPredicate, intersectedLink: OperatorLink): OperatorLink[] {
    const newLinks: OperatorLink[] = [];

    // Get source and target operators
    const sourceOperator = this.workflowActionService.getTexeraGraph().getOperator(intersectedLink.source.operatorID);
    const targetOperator = this.workflowActionService.getTexeraGraph().getOperator(intersectedLink.target.operatorID);

    if (!sourceOperator || !targetOperator) {
      return [];
    }

    // Check if the new operator has compatible ports
    const hasInputPorts = newOperator.inputPorts.length > 0;
    const hasOutputPorts = newOperator.outputPorts.length > 0;

    if (!hasInputPorts || !hasOutputPorts) {
      // If the new operator doesn't have both input and output ports, fall back to regular suggestions
      return this.getNewOperatorLinks(newOperator, this.suggestionInputs, this.suggestionOutputs);
    }

    // Delete the original link
    this.workflowActionService.deleteLinkWithID(intersectedLink.linkID);

    // Create link from source to new operator
    const sourceToNewLink: OperatorLink = {
      linkID: this.workflowUtilService.getLinkRandomUUID(),
      source: {
        operatorID: sourceOperator.operatorID,
        portID: intersectedLink.source.portID,
      },
      target: {
        operatorID: newOperator.operatorID,
        portID: newOperator.inputPorts[0].portID, // Use first available input port
      },
    };
    newLinks.push(sourceToNewLink);

    // Create link from new operator to target
    const newToTargetLink: OperatorLink = {
      linkID: this.workflowUtilService.getLinkRandomUUID(),
      source: {
        operatorID: newOperator.operatorID,
        portID: newOperator.outputPorts[0].portID, // Use first available output port
      },
      target: {
        operatorID: targetOperator.operatorID,
        portID: intersectedLink.target.portID,
      },
    };
    newLinks.push(newToTargetLink);

    return newLinks;
  }

  /**
   * Highlights an edge.
   *
   * @param link The link to highlight
   */
  private highlightEdgeIntersection(link: OperatorLink): void {
    const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
    if (!paper) {
      return;
    }

    const jointLink = paper.getModelById(link.linkID);
    if (jointLink) {
      jointLink.attr({
        ".connection": {
          stroke: "#FF6B35",
          "stroke-width": 4,
          "stroke-dasharray": "5,5",
        },
        ".marker-source": { fill: "#FF6B35" },
        ".marker-target": { fill: "#FF6B35" },
      });
    }
  }

  /**
   * Clears the highlighting on an edge.
   *
   * @param link The link to clear highlighting from
   */
  private clearEdgeIntersectionHighlight(link: OperatorLink): void {
    const paper = this.workflowActionService.getJointGraphWrapper().getMainJointPaper();
    if (!paper) {
      return;
    }

    const jointLink = paper.getModelById(link.linkID);
    if (jointLink) {
      jointLink.attr({
        ".connection": {
          stroke: "#848484", // Default link color
          "stroke-width": 2,
          "stroke-dasharray": "none",
        },
        ".marker-source": { fill: "none" },
        ".marker-target": { fill: "none" },
      });
    }
  }
}
