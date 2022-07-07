import { WorkflowGraph } from "./workflow-graph";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { OperatorGroup } from "./operator-group";
import { filter, map } from "rxjs/operators";

export class SyncOperatorGroup {
  constructor(
    private texeraGraph: WorkflowGraph,
    private jointGraphWrapper: JointGraphWrapper,
    private operatorGroup: OperatorGroup
  ) {
    this.handleTexeraGraphLinkDelete();
    this.handleTexeraGraphLinkAdd();
    this.handleTexeraGraphOperatorDelete();

    this.handleOperatorPositionChange();
    this.handleGroupPositionChange();
    this.handleOperatorLayerChange();
    this.handleLinkLayerChange();
  }

  /**
   * Handles operator delete events on texera graph.
   * If the deleted operator is embedded in some group,
   *  1) remove the operator from the group
   *  2) reposition the group to fit remaining operators if
   *     the group is not collapsed
   */
  private handleTexeraGraphOperatorDelete(): void {
    this.texeraGraph
      .getOperatorDeleteStream()
      .pipe(map(operator => operator.deletedOperatorID))
      .subscribe(deletedOperatorID => {
        const group = this.operatorGroup.getGroupByOperator(deletedOperatorID);
        if (group && !group.collapsed && group.operators.size > 1) {
          group.operators.delete(deletedOperatorID);
          this.operatorGroup.repositionGroup(group);
        } else if (group) {
          group.operators.delete(deletedOperatorID);
        }
      });
  }

  /**
   * Handles link add events on texera graph.
   * Checks if the added link is related to some group, and adds the
   * link to the group as a link, in-link, or out-link.
   */
  private handleTexeraGraphLinkAdd(): void {
    this.texeraGraph.getLinkAddStream().subscribe(link => {
      this.operatorGroup.getAllGroups().forEach(group => {
        if (group.operators.has(link.source.operatorID) && group.operators.has(link.target.operatorID)) {
          this.operatorGroup.addLinkToGroup(link.linkID, group.groupID);
        } else if (!group.operators.has(link.source.operatorID) && group.operators.has(link.target.operatorID)) {
          this.operatorGroup.addInLinkToGroup(link.linkID, group.groupID);
        } else if (group.operators.has(link.source.operatorID) && !group.operators.has(link.target.operatorID)) {
          this.operatorGroup.addOutLinkToGroup(link.linkID, group.groupID);
        }
      });
    });
  }

  /**
   * Handles link delete events on texera graph.
   * If the deleted link is related to some group, remove it from the group.
   */
  private handleTexeraGraphLinkDelete(): void {
    this.texeraGraph
      .getLinkDeleteStream()
      .pipe(map(link => link.deletedLink))
      .subscribe(deletedLink => {
        this.operatorGroup.getAllGroups().forEach(group => {
          if (group.links.has(deletedLink.linkID)) {
            group.links.delete(deletedLink.linkID);
          } else if (group.inLinks.includes(deletedLink.linkID)) {
            group.inLinks.splice(group.inLinks.indexOf(deletedLink.linkID), 1);
          } else if (group.outLinks.includes(deletedLink.linkID)) {
            group.outLinks.splice(group.outLinks.indexOf(deletedLink.linkID), 1);
          }
        });
      });
  }

  /**
   * Handles operator position change events.
   * If the moved operator is embedded in some group,
   *  1) update the operator's position stored in the group
   *  2) reposition the group to fit the moved operator
   */
  private handleOperatorPositionChange(): void {
    this.jointGraphWrapper
      .getElementPositionChangeEvent()
      .pipe(
        filter(() => this.operatorGroup.getSyncOperatorGroup()),
        filter(movedElement => this.texeraGraph.hasOperator(movedElement.elementID))
      )
      .subscribe(movedOperator => {
        this.operatorGroup.getAllGroups().forEach(group => {
          const operatorInfo = group.operators.get(movedOperator.elementID);
          if (operatorInfo) {
            operatorInfo.position = movedOperator.newPosition;
            this.operatorGroup.repositionGroup(group);
          }
        });
      });
  }

  /**
   * Handles group position change events.
   * When a group is moved, update its embedded operators' position according
   * to the relative offset. If the group is not collapsed, move embedded
   * operators together with the group.
   */
  private handleGroupPositionChange(): void {
    this.jointGraphWrapper
      .getElementPositionChangeEvent()
      .pipe(
        filter(() => this.operatorGroup.getSyncOperatorGroup()),
        filter(movedElement => this.operatorGroup.hasGroup(movedElement.elementID))
      )
      .subscribe(movedGroup => {
        const group = this.operatorGroup.getGroup(movedGroup.elementID);
        const offsetX = movedGroup.newPosition.x - movedGroup.oldPosition.x;
        const offsetY = movedGroup.newPosition.y - movedGroup.oldPosition.y;
        group.operators.forEach((operatorInfo, operatorID) => {
          operatorInfo.position = {
            x: operatorInfo.position.x + offsetX,
            y: operatorInfo.position.y + offsetY,
          };
          if (!group.collapsed) {
            const listenPositionChange = this.jointGraphWrapper.getListenPositionChange();
            this.operatorGroup.setSyncOperatorGroup(false);
            this.jointGraphWrapper.setListenPositionChange(false);
            this.jointGraphWrapper.setElementPosition(operatorID, offsetX, offsetY);
            this.operatorGroup.setSyncOperatorGroup(true);
            this.jointGraphWrapper.setListenPositionChange(listenPositionChange);
          }
        });
      });
  }

  /**
   * Handles operator layer change events.
   * If the operator is embedded in some group, update its layer stored in the group.
   */
  private handleOperatorLayerChange(): void {
    this.jointGraphWrapper
      .getCellLayerChangeEvent()
      .pipe(filter(movedOperator => this.texeraGraph.hasOperator(movedOperator.cellID)))
      .subscribe(movedOperator => {
        this.operatorGroup.getAllGroups().forEach(group => {
          const operatorInfo = group.operators.get(movedOperator.cellID);
          if (operatorInfo) {
            operatorInfo.layer = movedOperator.newLayer;
          }
        });
      });
  }

  /**
   * Handles link layer change events.
   * If the link is embedded in some group, update its layer stored in the group.
   */
  private handleLinkLayerChange(): void {
    this.jointGraphWrapper
      .getCellLayerChangeEvent()
      .pipe(filter(movedLink => this.texeraGraph.hasLinkWithID(movedLink.cellID)))
      .subscribe(movedLink => {
        this.operatorGroup.getAllGroups().forEach(group => {
          const linkInfo = group.links.get(movedLink.cellID);
          if (linkInfo) {
            linkInfo.layer = movedLink.newLayer;
          }
        });
      });
  }
}
