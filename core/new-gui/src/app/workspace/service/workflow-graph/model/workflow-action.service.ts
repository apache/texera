import { JointGraphReadonly } from './../../../types/joint-graph';
import { JointUIService } from './../../joint-ui/joint-ui.service';
import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly.interface';
import { WorkflowGraph } from './../../../types/workflow-graph';
import { Observable } from 'rxjs/Observable';
import { Injectable } from '@angular/core';
import { Point, OperatorPredicate, OperatorLink } from '../../../types/common.interface';

import * as joint from 'jointjs';

/**
 *
 * WorkflowActionService exposes functions (actions) to modify the workflow graph model,
 *  such as addOperator, deleteOperator, addLink, deleteLink, etc.
 * WorkflowActionService checks the validity of these actions,
 *  for example, check if adding two operators with the same ID.
 *
 * All changes(actions) to the workflow graph should be called through WorkflowActionService,
 *  then WorkflowActionService will propagate these actions to the JointModelService and TexeraModelService,
 *  where the changes will be actually made.
 *
 * For the details of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */
@Injectable()
export class WorkflowActionService {

  private texeraGraph = new WorkflowGraph();
  private jointGraph = new joint.dia.Graph();
  private jointGraphReadonly = new JointGraphReadonly(this.jointGraph);

  constructor(
  ) { }

  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  public getJointGraphWrapper(): JointGraphReadonly {
    return this.jointGraphReadonly;
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
  public addOperator(operator: OperatorPredicate, operatorJointElement: joint.dia.Element): void {
    if (operator.operatorID !== operatorJointElement.id.toString()) {
      throw new Error(`operatorID ${operator.operatorID} and Joint UI Element ID ${operatorJointElement.id.toString()} are inconsistent`);
    }
    this.texeraGraph.addOperator(operator);
    this.jointGraph.addCell(operatorJointElement);
  }

  /**
   * Deletes an operator from the workflow graph
   * Throws an Error if the operator ID doesn't exist in the Workflow Graph.
   * @param operatorID
   */
  public deleteOperator(operatorID: string): void {
    this.texeraGraph.deleteOperator(operatorID);
    this.jointGraph.getCell(operatorID).remove();
  }

  /**
   * Adds a link to the workflow graph
   * Throws an Error if the link ID or the link with same source and target already exists.
   * @param link
   */
  public addLink(link: OperatorLink): void {
    this.texeraGraph.addLink(link);
    const jointLinkCell = JointUIService.getJointLinkCell(link);
    this.jointGraph.addCell(jointLinkCell);
  }

  /**
   * Deletes a link from the workflow graph
   * Throws an Error if the linkID doesn't exist in the workflow graph.
   * @param linkID
   */
  public deleteLinkWithID(linkID: string): void {
    this.texeraGraph.deleteLinkWithID(linkID);
    this.jointGraph.getCell(linkID).remove();
  }

}
