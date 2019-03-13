import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { SyncTexeraModel } from './sync-texera-model';
import { JointGraphWrapper } from './joint-graph-wrapper';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { WorkflowGraph, WorkflowGraphReadonly } from './workflow-graph';
import { Injectable } from '@angular/core';
import { Point, OperatorPredicate, OperatorLink, OperatorPort } from '../../../types/workflow-common.interface';

import * as joint from 'jointjs';


/**
 *
 * WorkflowActionService exposes functions (actions) to modify the workflow graph model of both JointJS and Texera,
 *  such as addOperator, deleteOperator, addLink, deleteLink, etc.
 * WorkflowActionService performs checks the validity of these actions,
 *  for example, throws an error if deleting an nonexist operator
 *
 * All changes(actions) to the workflow graph should be called through WorkflowActionService,
 *  then WorkflowActionService will propagate these actions to JointModel and Texera Model automatically.
 *
 * For an overview of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */
@Injectable()
export class WorkflowActionService {

  public pointsUndo = new Map<String, Array<Point>>(); // use this to get the points to readd
  public pointsPointer = new Map<String, number>(); // contains pointers to locations in pointsUndo for dragging
  private readonly texeraGraph: WorkflowGraph;
  private readonly jointGraph: joint.dia.Graph;
  private readonly jointGraphWrapper: JointGraphWrapper;
  private readonly syncTexeraModel: SyncTexeraModel;
  constructor(
    private operatorMetadataService: OperatorMetadataService,
    private jointUIService: JointUIService
  ) {
    this.texeraGraph = new WorkflowGraph();
    this.jointGraph = new joint.dia.Graph();
    this.jointGraphWrapper = new JointGraphWrapper(this.jointGraph);
    this.syncTexeraModel = new SyncTexeraModel(this.texeraGraph, this.jointGraphWrapper);
  }

  /**
   * Gets the read-only version of the TexeraGraph
   *  to access the properties and event streams.
   *
   * Texera Graph contains information about the logical workflow plan of Texera,
   *  such as the types and properties of the operators.
   */
  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  /**
   * Gets the JointGraph Wrapper, which contains
   *  getter for properties and event streams as RxJS Observables.
   *
   * JointJS Graph contains information about the UI,
   *  such as the position of operator elements, and the event of user dragging a cell around.
   */
  public getJointGraphWrapper(): JointGraphWrapper {
    return this.jointGraphWrapper;
  }

  /**
   * Let the JointGraph model be attached to the joint paper (paperOptions will be passed to Joint Paper constructor).
   *
   * We don't want to expose JointModel as a public variable, so instead we let JointPaper to pass the constructor options,
   *  and JointModel can be still attached to it without being publicly accessible by other modules.
   *
   * @param paperOptions JointJS paper options
   */
  public attachJointPaper(paperOptions: joint.dia.Paper.Options): joint.dia.Paper.Options {
    paperOptions.model = this.jointGraph;
    return paperOptions;
  }

  /**
   * Adds an opreator to the workflow graph at a point.
   * Throws an Error if the operator ID already existed in the Workflow Graph.
   *
   * @param operator
   * @param point
   */
  public addOperator(operator: OperatorPredicate, point: Point): void {
    // check that the operator doesn't exist
    this.texeraGraph.assertOperatorNotExists(operator.operatorID);
    // check that the operator type exists
    if (! this.operatorMetadataService.operatorTypeExists(operator.operatorType)) {
      throw new Error(`operator type ${operator.operatorType} is invalid`);
    }
    // get the JointJS UI element
    const operatorJointElement = this.jointUIService.getJointOperatorElement(operator, point);

    // add operator to joint graph first
    // if jointJS throws an error, it won't cause the inconsistency in texera graph
    this.jointGraph.addCell(operatorJointElement);
    // add operator to texera graph
    this.texeraGraph.addOperator(operator);

    // add operator to the maps
    if (this.pointsUndo.has(operator.operatorID) === false) {
      this.pointsUndo.set(operator.operatorID, []);
      this.pointsPointer.set(operator.operatorID, 0);
      this.pointsUndo.get(operator.operatorID).push(point);
    }
    // highlight operator
    this.jointGraphWrapper.unhighlightCurrent();
    this.jointGraphWrapper.highlightOperator(operator.operatorID);
  }

  /**
   * Deletes an operator from the workflow graph
   * Throws an Error if the operator ID doesn't exist in the Workflow Graph.
   * @param operatorID
   */
  public deleteOperator(operatorID: string): void {
    this.texeraGraph.assertOperatorExists(operatorID);
    // remove the operator from JointJS
    this.jointGraph.getCell(operatorID).remove();
    // JointJS operator delete event will propagate and trigger Texera operator delete
  }

  /**
   * Adds a link to the workflow graph
   * Throws an Error if the link ID or the link with same source and target already exists.
   * @param link
   */
  public addLink(link: OperatorLink): void {
    this.texeraGraph.assertLinkNotExists(link);
    this.texeraGraph.assertLinkIsValid(link);
    // add the link to JointJS
    const jointLinkCell = JointUIService.getJointLinkCell(link);
    this.jointGraph.addCell(jointLinkCell);
    // JointJS link add event will propagate and trigger Texera link add
  }

  /**
   * Deletes a link with the linkID from the workflow graph
   * Throws an Error if the linkID doesn't exist in the workflow graph.
   * @param linkID
   */
  public deleteLinkWithID(linkID: string): void {
    this.texeraGraph.assertLinkWithIDExists(linkID);
    this.jointGraph.getCell(linkID).remove();
    // JointJS link delete event will propagate and trigger Texera link delete
  }

  /**
   * Deletes a link with the source and target from the workflow graph
   * Throws an Error if the linkID doesn't exist in the workflow graph.
   * @param linkID
   */
  public deleteLink(source: OperatorPort, target: OperatorPort): void {
    this.texeraGraph.assertLinkExists(source, target);
    const link = this.texeraGraph.getLink(source, target);
    if (!link) {
      throw new Error(`link with source ${source} and target ${target} doesn't exist`);
    }
    this.jointGraph.getCell(link.linkID).remove();
    // JointJS link delete event will propagate and trigger Texera link delete
  }

  // use this to modify properties
  public setOperatorProperty(operatorID: string, newProperty: object) {
    this.texeraGraph.setOperatorProperty(operatorID, newProperty);
  }

  // Function to change the property then rehighlight to immediately see changes
  public changeOperatorProperty(operatorID: string, newProperty: object) {
    this.setOperatorProperty(operatorID, newProperty);
    this.jointGraphWrapper.unhighlightCurrent();
    this.jointGraphWrapper.highlightOperator(operatorID);
  }

  // Function to undo the dragging operation
  public undoDragOperator(operatorID: string) {
    // cast cell to Element
    const operatorCell = this.jointGraph.getCell(operatorID);
    if (operatorCell instanceof joint.dia.Element) { // just do a type check
      if (this.pointsPointer.get(operatorID) > 0) {
        const currentPoint = this.pointsUndo.get(operatorID)[this.pointsPointer.get(operatorID)];
        this.pointsPointer.set(operatorID, this.pointsPointer.get(operatorID) - 1);
        const previousPoint = this.pointsUndo.get(operatorID)[this.pointsPointer.get(operatorID)];
        operatorCell.translate(previousPoint.x - currentPoint.x, previousPoint.y - currentPoint.y);
      }
      // next, figure out a way to store the the previous x, y coordinates
      // create two maps, one with x/y coordinates and another with a pointer
    } else {
      throw new Error(`Dragged cell is not an operator`);
    }
    console.log('UNDO');
    console.log(this.pointsPointer.get(operatorID));
    console.log(this.pointsUndo.get(operatorID).length - 1);
  }

  // for redoing drag
  public redoDragOperator(operatorID: string) {
    const operatorCell = this.jointGraph.getCell(operatorID);
    if (operatorCell instanceof joint.dia.Element) { // just do a type check
      if (this.pointsPointer.get(operatorID) >= 0 && this.pointsPointer.get(operatorID) < this.pointsUndo.get(operatorID).length - 1) {
        const currentPoint = this.pointsUndo.get(operatorID)[this.pointsPointer.get(operatorID)];
        this.pointsPointer.set(operatorID, this.pointsPointer.get(operatorID) + 1);
        const newPoint = this.pointsUndo.get(operatorID)[this.pointsPointer.get(operatorID)];
        operatorCell.translate(newPoint.x - currentPoint.x, newPoint.y - currentPoint.y);
      }
      // next, figure out a way to store the the previous x, y coordinates
      // create two maps, one with x/y coordinates and another with a pointer
    } else {
      throw new Error(`Dragged cell is not an operator`);
    }
    console.log('REDO');
    console.log(this.pointsPointer.get(operatorID));
    console.log(this.pointsUndo.get(operatorID).length - 1);
  }

  // TEMP FUNCTION
   public getPoint(operatorID: string): Point  {
    if (this.pointsUndo.has(operatorID) === false) {
      throw new Error(`operator ID does not exist in graph`);
    }
    const points = this.pointsUndo.get(operatorID);
    return points[this.pointsPointer.get(operatorID)];
  }

}
