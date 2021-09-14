import { OperatorLink } from "../../../types/workflow-common.interface";

import { WorkflowGraph } from "./workflow-graph";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { OperatorGroup } from "./operator-group";
import { filter, map, tap } from "rxjs/operators";

/**
 * SyncTexeraModel subscribes to the graph change events from JointJS,
 *  then sync the changes to the TexeraGraph:
 *    - delete operator
 *    - link events: link add/delete/change
 *
 * For details of handling each JointJS event type, see the comments below for each function.
 *
 * For an overview of the services in WorkflowGraphModule, see workflow-graph-design.md
 *
 */
export class SyncTexeraModel {
  constructor(
    private texeraGraph: WorkflowGraph,
    private jointGraphWrapper: JointGraphWrapper,
    private operatorGroup: OperatorGroup
  ) {
    this.handleJointOperatorDelete();
    this.handleJointLinkEvents();
  }

  /**
   * Handles JointJS operator element delete events:
   *  deletes the corresponding operator in Texera workflow graph.
   *
   * Deletion of an operator will also cause its connected links to be deleted as well,
   *  JointJS automatically handles this logic,
   *  therefore we don't handle it to avoid inconsistency (deleting already deleted link).
   *
   * When an operator is deleted, JointJS will trigger the corresponding
   *  link delete events and cause texera link to be deleted.
   */
  private handleJointOperatorDelete(): void {
    this.jointGraphWrapper
      .getJointElementCellDeleteStream()
      .pipe(
        map(element => element.id.toString()),
        filter(elementID => this.texeraGraph.hasOperator(elementID) && this.operatorGroup.getSyncTexeraGraph())
      )
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
    this.jointGraphWrapper
      .getJointLinkCellAddStream()
      .pipe(
        filter(link => this.isValidJointLink(link) && this.operatorGroup.getSyncTexeraGraph()),
        map(link => SyncTexeraModel.getOperatorLink(link))
      )
      .subscribe(link => this.texeraGraph.addLink(link));

    /**
     * on link cell delete:
     * we need to first check if the link is a valid link
     *  then delete the link by the link ID
     */
    this.jointGraphWrapper
      .getJointLinkCellDeleteStream()
      .pipe(
        filter(link => this.isValidJointLink(link) && this.operatorGroup.getSyncTexeraGraph()),
        map(link => SyncTexeraModel.getOperatorLink(link))
      )
      .subscribe(link => this.texeraGraph.deleteLinkWithID(link.linkID));

    /**
     * on link cell change:
     * link cell change could cause deletion of a link or addition of a link, or simply no effect
     * TODO: finish this documentation
     */
    this.jointGraphWrapper
      .getJointLinkCellChangeStream()
      .pipe(
        filter(() => this.operatorGroup.getSyncTexeraGraph()),
        // we intentionally want the side effect (delete the link) to happen **before** other operations in the chain
        tap(link => {
          const linkID = link.id.toString();
          if (this.texeraGraph.hasLinkWithID(linkID)) {
            this.texeraGraph.deleteLinkWithID(linkID);
          }
        }),
        filter(link => this.isValidJointLink(link)),
        map(link => SyncTexeraModel.getOperatorLink(link))
      )
      .subscribe(link => {
        this.texeraGraph.addLink(link);
      });
  }

  /**
   * Determines if a jointJS link is valid (both ends are connected to a port
   * of operator or are connected to a collapsed group).
   * If a JointJS link's target is still a point (not connected), it's not considered a valid link.
   * @param jointLink
   */
  private isValidJointLink(jointLink: joint.dia.Link): boolean {
    return (
      jointLink &&
      jointLink.attributes &&
      jointLink.attributes.source &&
      jointLink.attributes.target &&
      jointLink.attributes.source.id &&
      (jointLink.attributes.source.port || this.operatorGroup.hasGroup(jointLink.attributes.source.id.toString())) &&
      jointLink.attributes.target.id &&
      (jointLink.attributes.target.port || this.operatorGroup.hasGroup(jointLink.attributes.target.id.toString())) &&
      (this.texeraGraph.hasOperator(jointLink.attributes.source.id.toString()) ||
        this.texeraGraph.hasOperator(jointLink.attributes.target.id.toString()) ||
        (this.operatorGroup.hasGroup(jointLink.attributes.source.id.toString()) &&
          this.operatorGroup.hasGroup(jointLink.attributes.source.id.toString())))
    );
    // the above two lines are causing unit test fail in sync-texera-model.spec.ts
    // since if operator is deleted first the link will become invalid and thus undeletable.
  }

  /**
   * Transforms a JointJS link (joint.dia.Link) to a Texera Link object
   * The JointJS link must be valid, otherwise an error will be thrown.
   * @param jointLink
   */
  static getOperatorLink(jointLink: joint.dia.Link): OperatorLink {
    type jointLinkEndpointType = { id: string; port: string } | null | undefined;

    // the link should be a valid link (both source and target are connected to an operator)
    // isValidLink function is not reused because of Typescript strict null checking
    const jointSourceElement: jointLinkEndpointType = jointLink.attributes.source;
    const jointTargetElement: jointLinkEndpointType = jointLink.attributes.target;

    if (!jointSourceElement) {
      throw new Error("Invalid JointJS Link: no source element");
    }

    if (!jointTargetElement) {
      throw new Error("Invalid JointJS Link: no target element");
    }

    return {
      linkID: jointLink.id.toString(),
      source: {
        operatorID: jointSourceElement.id,
        portID: jointSourceElement.port,
      },
      target: {
        operatorID: jointTargetElement.id,
        portID: jointTargetElement.port,
      },
    };
  }
}
