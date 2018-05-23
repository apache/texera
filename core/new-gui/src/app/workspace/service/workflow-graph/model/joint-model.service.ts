import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly.interface';

import { WorkflowActionService } from './workflow-action.service';
import { Injectable } from '@angular/core';
import { Point, OperatorPredicate, OperatorLink } from '../../../types/common.interface';
import { Observable } from 'rxjs/Observable';

import '../../../../common/rxjs-operators';

import * as joint from 'jointjs';
import { JointUIService } from '../../joint-ui/joint-ui.service';

/**
 * JointModelService manages the JointJS model (jointGraph).
 * JointModel provides events and properties of JointJS related to the UI.
 * Note that these events/properties are from the perspective of the UI.
 *
 * The jointGraph is private, because we only want to expose read-only methods to external components
 *  if access to more event stream or properties are needed, `getter` methods could be created
 *
 * The JointJS model jointGraph is two-way binded with the View, whenever the UI is changed,
 *  JointJS will change the Model as well, vice versa.
 * JointModelService also subscribes to the events from Workflow Actions to change the graph accordingly
 *
 * Relevant JointJS documentation: https://resources.jointjs.com/tutorial/graph-and-paper
 * For the details of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */
@Injectable()
export class JointModelService {

  /**
   * Cre JointJS graph model, two-way binded with the view
   * This model is kept private to prevent external modules to
   *  directly modify the jointGraph and bypass the rules of WorkflowGraphServices
   *
   * To access the properties or events of the graph,
   *  getter methods should be created, changing the jointGraph directly is prohibited.
   */
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
     * Listen to the action event propagated from workflowActionService
     *  and update the JointJS graph accordingly:
     *  - add operator
     *  - delete operator
     *  - add link
     *  - delete link
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
   * Let the JointGraph model be attached to the joint paper (paperOptions will be passed to Joint Paper constructor).
   * JointPaper (object representing View) in WorkflowEditorComponent must to have a model binded with it during construction.
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
   * Returns an Observable stream capturing the operator cell delete event in JointJS graph.
   */
  public onJointOperatorCellDelete(): Observable<joint.dia.Element> {
    const jointOperatorDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isElement())
      .map(cell => <joint.dia.Element>cell);
    return jointOperatorDeleteStream;
  }

  /**
   * Returns an Observable stream capturing the link cell add event in JointJS graph.
   *
   * Notice that a link added to JointJS graph doesn't mean it will be added to Texera Workflow Graph as well
   *  because the link might not be valid (not connected to a target operator and port yet).
   * This event only represents that a link cell is visually added to the UI.
   *
   */
  public onJointLinkCellAdd(): Observable<joint.dia.Link> {
    const jointLinkAddStream = this.jointCellAddStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkAddStream;
  }

  /**
   * Returns an Observable stream capturing the link cell delete event in JointJS graph.
   *
   * Notice that a link deleted from JointJS graph doesn't mean the same event happens for Texera Workflow Grap
   *  because the link might not be valid and doesn't exist logically in the Workflow Graph.
   * This event only represents that a link cell visually disappears from the UI.
   *
   */
  public onJointLinkCellDelete(): Observable<joint.dia.Link> {
    const jointLinkDeleteStream = this.jointCellDeleteStream
      .filter(cell => cell.isLink())
      .map(cell => <joint.dia.Link>cell);

    return jointLinkDeleteStream;
  }

  /**
   * Returns an Observable stream capturing the link cell delete event in JointJS graph.
   *
   * Notice that the link change event will be triggered whenever the link's source or target is changed:
   *  - one end of the link is attached to a port
   *  - one end of the link is detached to a port and become a point (coordinate) in the paper
   *  - one end of the link is moved from one point to another point in the paper
   */
  public onJointLinkCellChange(): Observable<joint.dia.Link> {
    const jointLinkChangeStream = Observable
      .fromEvent(this.jointGraph, 'change:source change:target')
      .map(value => <joint.dia.Link>value);

    return jointLinkChangeStream;
  }

  /**
   * This will add a JointJS element based on the OperatorPredicate
   *  passed in at the position `point`
   *
   * The operator should not exist, this precondition is checked in WorkflowActionService.
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
   * Remove a existing JointJS element (operator) by ID
   *
   * The operator should already exist, this precondition is checked in WorkflowActionService.
   *
   * @param operatorID ID of an operator
   */
  private deleteJointOperatorElement(operatorID: string): void {
    this.jointGraph.getCell(operatorID).remove();
  }

  /**
   * Create a new link based on the OperatorLink passed in.
   * The source and the target operator and port for this link should already exist.
   * This precondition is checked in WorkflowActionService.
   *
   * @param link OperatorLink
   */
  private addJointLinkCell(link: OperatorLink): void {
    const jointLinkCell = JointUIService.getJointLinkCell(link);
    this.jointGraph.addCell(jointLinkCell);
  }

  /**
   * This will delete a JointJS link by ID.
   * The link should already exists, this precondition is checked in WorkflowActionService.
   * @param linkID link ID
   */
  private deleteJointLinkCell(linkID: string): void {
    this.jointGraph.getCell(linkID).remove();
  }

}

