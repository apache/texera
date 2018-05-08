import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly.interface';

import { WorkflowActionService } from './workflow-action.service';
import { Injectable } from '@angular/core';
import { Point, OperatorPredicate, OperatorLink } from '../../../types/common.interface';
import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';

import * as joint from 'jointjs';
import { JointUIService } from '../../joint-ui/joint-ui.service';

/**
 *
 * JointModelService includes the JointJS model (workflow graph).
 *
 * Whenever the user made changes to the JointJS paper on the UI, such as
 *  deleting a link or an operator, JointModelService will propagate
 *  that event to TexeraModelService to make sure the Texera work graph is
 *  sync with the JointJS graph.
 *
 */
@Injectable()
export class JointModelService {

  private jointGraph = new joint.dia.Graph();

  /**
   * This will capture all events in JointJS
   *  involving the 'add' operation
   */
  private jointCellAddStream = Observable
    .fromEvent(this.jointGraph, 'add')
    .map(value => <joint.dia.Cell>value);

  /**
   * This will capture all events in JointJS
   *  involving the 'remove' operation
   */
  private jointCellDeleteStream = Observable
    .fromEvent(this.jointGraph, 'remove')
    .map(value => <joint.dia.Cell>value);


  constructor(
    private workflowActionService: WorkflowActionService,
    private jointUIService: JointUIService) {

    /**
     * These will listen to the event propagated from workflowActionService
     *  and update the JointJS graph accordingly
     */

    this.workflowActionService._onAddOperatorAction().subscribe(
      value => this.addJointOperatorElement(value.operator, value.point)
    );

    this.workflowActionService._onDeleteOperatorAction().subscribe(
      value => this.deleteJointOperatorElement(value.operatorID)
    );

    this.workflowActionService._onAddLinkAction().subscribe(
      value => this.addJointLinkCell(value.link)
    );

    this.workflowActionService._onDeleteLinkAction().subscribe(
      value => this.deleteJointLinkCell(value.linkID)
    );
  }

  /**
   * To make our code as modularize as possible, we want the JointJS graph only exist
   *  in JointModelService. However, to successfully create a JointJS paper, a JointJS graph
   *  object is required. So, before creating a JointJS paper, it will need to add the
   *  graph object in JointModelService into the paper options. Then, whenever the JointJS
   *  paper changes, the graph in JointModelService can detect the events.
   *
   * @param paperOptions JointJS paper options
   */
  public attachJointPaper(paperOptions: joint.dia.Paper.Options): joint.dia.Paper.Options {
    paperOptions.model = this.jointGraph;
    return paperOptions;
  }

  /**
   * This will return an observable catching the operator delete event
   *  in JointJS graph.
   */
  public onJointOperatorCellDelete(): Observable<joint.dia.Element> {
    const jointOperatorDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isElement())
      .map(cell => <joint.dia.Element>cell);
    return jointOperatorDeleteStream;
  }

  /**
   * This will return an observable catching the link add event
   *  in JointJS graph.
   */
  public onJointLinkCellAdd(): Observable<joint.dia.Link> {
    const jointLinkAddStream = this.jointCellAddStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkAddStream;
  }

  /**
   * This will return an observable catching the link delete event
   *  in JointJS graph.
   */
  public onJointLinkCellDelete(): Observable<joint.dia.Link> {
    const jointLinkDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkDeleteStream;
  }

  /**
   * This will return an observable catching the link change event
   *  in JointJS graph. This happens when the user is changing the
   *  already connected link.
   */
  public onJointLinkCellChange(): Observable<joint.dia.Link> {
    const jointLinkChangeStream = Observable
      .fromEvent(this.jointGraph, 'change:source change:target')
      .map(value => <joint.dia.Link>value);

    return jointLinkChangeStream;
  }

  /**
   * This will create a JointJS element based on the OperatorPredicate
   *  passed in. And this element will be created at the position 'point'
   *
   * @param operator OperatorPredicate
   * @param point the position to create the element
   */
  private addJointOperatorElement(operator: OperatorPredicate, point: Point): void {
    const operatorJointElement = this.jointUIService.getJointOperatorElement(
      operator, point);

    this.jointGraph.addCell(operatorJointElement);
  }

  /**
   * Remove a existing JointJS element by ID
   *
   * @param operatorID ID of an operator
   */
  private deleteJointOperatorElement(operatorID: string): void {
    this.jointGraph.getCell(operatorID).remove();
  }

  /**
   * Create a new link based on the OperatorLink passed in. The source
   *  and the target element for this link should be created before
   *  adding this link.
   *
   * @param link OperatorLink
   */
  private addJointLinkCell(link: OperatorLink): void {
    const jointLinkCell = JointUIService.getJointLinkCell(link);
    this.jointGraph.addCell(jointLinkCell);
  }

  /**
   * This will delete a JointJS link by ID
   * @param linkID link ID
   */
  private deleteJointLinkCell(linkID: string): void {
    this.jointGraph.getCell(linkID).remove();
  }

}

