import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { Subject } from 'rxjs/Subject';

import { OperatorMetadataService } from './../../operator-metadata/operator-metadata.service';
import { WorkflowActionService } from './workflow-action.service';
import { JointModelService } from './joint-model.service';

import { WorkflowGraphReadonly } from './../../../types/workflow-graph-readonly.interface';
import { OperatorSchema } from './../../../types/operator-schema.interface';
import { OperatorLink, OperatorPredicate } from '../../../types/common.interface';
import { WorkflowGraph } from './../../../types/workflow-graph';

/**
 * TexeraModelService manages the Texera Model.
 * It provides a read-only version of the workflow graph (changes should be made via `WorkflowActionService`)
 * It also provides event streams for the changes, such as operator/link added/deleted
 *  in a logical level of Texera (contrary to JointJS's UI related events)
 *
 * External modules should use this service to access the workflow graph,
 *  and listen to the events of changes to the graph.
 *
 * For the details of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */
@Injectable()
export class TexeraModelService {

  private texeraGraph: WorkflowGraph;

  private addOperatorSubject = new Subject<OperatorPredicate>();
  private deleteOperatorSubject = new Subject<OperatorPredicate>();
  private addLinkSubject = new Subject<OperatorLink>();
  private deleteLinkSubject = new Subject<OperatorLink>();

  constructor(
    private workflowActionService: WorkflowActionService,
    private jointModelService: JointModelService,
  ) {
    // Bypass Typescript type system to access a private variable
    //  because Typescript doesn't support package (same folder) access level :(
    //  and we don't want to expose the write-able workflow graph object to the outside
    // This is very dangerous and should be prohibited in most cases
    this.texeraGraph = (workflowActionService as any).texeraGraph;

    /**
     * Listen events from Workflow Action Service:
     *  - add operator
     */
    this.workflowActionService._onAddOperatorAction()
      .subscribe(value => this.addOperator(value.operator));

    /**
     * Listen events from Joint Model Service:
     *  - delete operator
     *  - add link
     *  - delete link
     *  - link changed
     * (JointJS link events are special, explained below)
     */

    this.jointModelService.onJointOperatorCellDelete()
      .map(element => element.id.toString())
      .subscribe(elementID => this.deleteOperator(elementID));

    /**
     * Notes about handling JointJS link events:
     * JointJS link events reflect the changes to the link View in the UI.
     * Workflow link requires the link to have both source and target port to be considered valid.
     *
     * Cases where JointJS and Texera link have different semantics:
     *  - When the user drags the link from one port, but not yet to connect to another port,
     *      the link is added in the semantic of JointJS, but not in the semantic of Texera Workflow graph.
     *  - When an invalid link that is not connected to a port disappears,
     *      the link delete event is trigger by JointJS, but the link never existed from Texera's perspective.
     *  - When the user drags and detaches the end of a valid link, the link is disconnected from the target port,
     *      the link change event (not delete) is triggered by JointJS, but the link should be deleted from Texera's graph.
     *  - When the user attaches the end of the link to a target port,
     *      the link change event (not add) is triggered by JointJS, but it should be added to the Texera Graph.
     *  - When the user drags the link around, the link change event will be trigger continuously,
     *      when the target being a changing coordinate. But this event should not have any effect on the Texera Graph.
     *
     * To address the disparity of the semantics, the linkAdded / linkDeleted / linkChanged events need to be handled carefully.
     */


    /**
     * on link cell add:
     * we need to check if the link is a valid link in Texera's semantic (has both source and target port)
     *  and only add valid links to the graph
     */
    this.jointModelService.onJointLinkCellAdd()
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => this.addLink(link));

    /**
     * on link cell delete:
     * we need to check if the deleted link cell is a valid link beforehead (check if the link ID existed in the Workflow Graph)
     * and only delete the link by the link ID
     */
    this.jointModelService.onJointLinkCellDelete()
      .filter(link => this.texeraGraph.hasLinkWithID(link.id.toString()))
      .subscribe(link => this.deleteLink(link.id.toString()));


    /**
     * on link cell change:
     * link cell change could cause deletion of a link or addition of a link, or simply no effect
     * TODO: finish this documentation
     */
    this.jointModelService.onJointLinkCellChange()
      // we intentially want the side effect (delete the link) to happen **before** other operations in the chain
      .do((link) => {
        const linkID = link.id.toString();
        if (this.texeraGraph.hasLinkWithID(linkID)) { this.deleteLink(linkID); }
      })
      .filter(link => TexeraModelService.isValidLink(link))
      .map(link => TexeraModelService.getOperatorLink(link))
      .subscribe(link => {
        this.addLink(link);
      });

  }

  /**
   * Gets the Texera Graph object as a read-only variable
   */
  public getTexeraGraph(): WorkflowGraphReadonly {
    return this.texeraGraph;
  }

  /**
   * Gets the Observable stream representing
   *  the event of an operator being added to the Workflow Graph
   *  with the operator data (such as operatorID, properties, etc..) of the newly added operator.
   *
   */
  public onOperatorAdd(): Observable<OperatorPredicate> {
    return this.addOperatorSubject.asObservable();
  }

  /**
   * Gets the Observable stream representing
   *  the event of an operator being deleted from the Workflow Graph
   *  with the operator data of the deleted operator.
   */
  public onOperatorDelete(): Observable<OperatorPredicate> {
    return this.deleteOperatorSubject.asObservable();
  }

  /**
   * Gets the Observable stream representing
   *  the event of a link being aded to the Workflow Graph
   *  with the link data (link ID, source/target operator and port)
   */
  public onLinkAdd(): Observable<OperatorLink> {
    return this.addLinkSubject.asObservable();
  }

  /**
   * Gets the Observable stream representing
   *  the event of a link being deleted to the Workflow Graph
   *  with the deleted link data.
   */
  public onLinkDelete(): Observable<OperatorLink> {
    return this.deleteLinkSubject.asObservable();
  }

  /**
   * a set of helper functions to handle changes to the operator graph
   *  to both change the graph, and push the corresponding event to the Subject
   */

  private addOperator(operator: OperatorPredicate): void {
    this.texeraGraph.addOperator(operator);
    this.addOperatorSubject.next(operator);
  }

  private deleteOperator(operatorID: string): void {
    const deletedOperator = this.texeraGraph.deleteOperator(operatorID);
    this.deleteOperatorSubject.next(deletedOperator);
  }

  private addLink(link: OperatorLink): void {
    this.texeraGraph.addLink(link);
    this.addLinkSubject.next(link);
  }

  private deleteLink(linkID: string): void {
    const deletedLink = this.texeraGraph.deleteLinkWithID(linkID);
    this.deleteLinkSubject.next(deletedLink);
  }

  /**
   * Transforms a JointJS link (joint.dia.Link) to a Texera Link Object
   * The JointJS link must be valid, otherwise an error will be thrown.
   * @param jointLink
   */
  static getOperatorLink(jointLink: joint.dia.Link): OperatorLink {

    // the link should be a valid link (both source and target are connected to an operator)
    // isValidLink function is not reused because of Typescript strict null checking
    const jointSourceElement = jointLink.getSourceElement();
    const jointTargetElement = jointLink.getTargetElement();

    if (jointSourceElement === null || jointTargetElement === null) {
      throw new Error('Invalid JointJS Link:');
    }

    return {
      linkID: jointLink.id.toString(),
      source: {
        operatorID: jointSourceElement.id.toString(),
        portID: jointLink.get('source').port.toString()
      },
      target: {
        operatorID: jointTargetElement.id.toString(),
        portID: jointLink.get('target').port.toString()
      }
    };
  }

  /**
   * Determines if a jointJS link is valid (both ends are connected to a port of  port).
   * If a JointJS link's target is still a point (not connected), it's not considered a valid link.
   * @param jointLink
   */
  static isValidLink(jointLink: joint.dia.Link): boolean {
    return jointLink.getSourceElement() !== null && jointLink.getTargetElement() !== null;
  }


}


