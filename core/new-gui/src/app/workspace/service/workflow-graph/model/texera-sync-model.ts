import { OperatorLink } from './../../../types/common.interface';
import { Injectable } from '@angular/core';

import { WorkflowActionService } from './workflow-action.service';

import { WorkflowGraph } from './../../../types/workflow-graph';
import { JointGraphReadonly } from '../../../types/joint-graph';

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
export class TexeraSyncModel {

  constructor(
    private texeraGraph: WorkflowGraph,
    private jointGraphWrapper: JointGraphReadonly
  ) {

    /**
     * Listen events from Joint Model Service to sync the changes:
     *  - delete operator
     *  - link events: link add/delete/change
     */
    this.handleJointOperatorDelete();
    this.handleJointLinkEvents();
  }


  /**
   * Handles JointJS operator element delete events:
   *  deletes the corresponding operator in Texera workflow graph.
   *
   * Deletion of an operator will also cause its connected links to be deleted as well,
   *  JointJS automatically hanldes this logic,
   *  therefore we don't handle it to avoid inconsistency (deleting already deleted link).
   *
   * When an operator is deleted, JointJS will trigger the corresponding
   *  link delete events and cause texera link to be deleted.
   */
  private handleJointOperatorDelete(): void {
    this.jointGraphWrapper.onJointOperatorCellDelete()
      .map(element => element.id.toString())
      .filter(operatorID => this.texeraGraph.hasOperator(operatorID))
      .subscribe(elementID => this.texeraGraph.deleteOperator(elementID));
  }

  /**
   * Handles JointJS link events:
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
  private handleJointLinkEvents(): void {
    /**
     * on link cell add:
     * we need to check if the link is a valid link in Texera's semantic (has both source and target port)
     *  and only add valid links to the graph
     */
    this.jointGraphWrapper.onJointLinkCellAdd()
      .filter(link => TexeraSyncModel.isValidJointLink(link))
      .map(link => TexeraSyncModel.getOperatorLink(link))
      .filter(link => ! this.texeraGraph.hasLink(link.source, link.target))
      .subscribe(link => this.texeraGraph.addLink(link));

    /**
     * on link cell delete:
     * we need to check if the deleted link cell is a valid link beforehead (check if the link ID existed in the Workflow Graph)
     * and only delete the link by the link ID
     */
    this.jointGraphWrapper.onJointLinkCellDelete()
      .filter(link => this.texeraGraph.hasLinkWithID(link.id.toString()))
      .subscribe(link => this.texeraGraph.deleteLinkWithID(link.id.toString()));


    /**
     * on link cell change:
     * link cell change could cause deletion of a link or addition of a link, or simply no effect
     * TODO: finish this documentation
     */
    this.jointGraphWrapper.onJointLinkCellChange()
      // we intentially want the side effect (delete the link) to happen **before** other operations in the chain
      .do((link) => {
        const linkID = link.id.toString();
        if (this.texeraGraph.hasLinkWithID(linkID)) { this.texeraGraph.deleteLinkWithID(linkID); }
      })
      .filter(link => TexeraSyncModel.isValidJointLink(link))
      .map(link => TexeraSyncModel.getOperatorLink(link))
      .subscribe(link => {
        this.texeraGraph.addLink(link);
      });
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

    if (jointSourceElement === null) {
      throw new Error(`Invalid JointJS Link: no source element`);
    }

    if (jointTargetElement === null) {
      throw new Error(`Invalid JointJS Link: no target element`);
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
  static isValidJointLink(jointLink: joint.dia.Link): boolean {
    return jointLink.getSourceElement() !== null && jointLink.getTargetElement() !== null;
  }


}


