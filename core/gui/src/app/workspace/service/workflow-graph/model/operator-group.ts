import { Point, OperatorPredicate, OperatorLink } from "../../../types/workflow-common.interface";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { OperatorStatistics } from "src/app/workspace/types/execute-workflow.interface";

export interface Group
  extends Readonly<{
    groupID: string;
    operators: Map<string, OperatorInfo>;
    links: Map<string, LinkInfo>;
    inLinks: string[];
    outLinks: string[];
    collapsed: boolean;
  }> {}

export interface PlainGroup {
  groupID: string;
  operators: Record<string, OperatorInfo>;
  links: Record<string, LinkInfo>;
  inLinks: string[];
  outLinks: string[];
  collapsed: boolean;
}

export type OperatorInfo = {
  operator: OperatorPredicate;
  position: Point;
  layer: number;
  statistics?: OperatorStatistics;
};

export type LinkInfo = {
  link: OperatorLink;
  layer: number;
};

export type GroupBoundingBox = {
  topLeft: Point;
  bottomRight: Point;
};

type restrictedMethods =
  | "addGroup"
  | "deleteGroup"
  | "collapseGroup"
  | "expandGroup"
  | "setGroupCollapsed"
  | "setSyncTexeraGraph"
  | "hideOperatorsAndLinks"
  | "showOperatorsAndLinks";

// readonly version of OperatorGroup
export type OperatorGroupReadonly = Omit<OperatorGroup, restrictedMethods>;

export class OperatorGroup {
  private groupIDMap = new Map<string, Group>();
  private syncTexeraGraph = true;

  constructor(
    private jointGraphWrapper: JointGraphWrapper,
  ) {}


  /**
   * Returns whether the group exists in the graph.
   * @param groupID
   */
  public hasGroup(groupID: string): boolean {
    return this.groupIDMap.has(groupID);
  }

  /**
   * Gets the group with the groupID.
   * Throws an error if the group doesn't exist.
   *
   * @param groupID
   */
  public getGroup(groupID: string): Group {
    const group = this.groupIDMap.get(groupID);
    if (!group) {
      throw new Error(`group with ID ${groupID} doesn't exist`);
    }
    return group;
  }

  /**
   * Gets the group that the given operator resides in.
   * Returns undefined if there's no such group.
   *
   * @param operatorID
   */
  public getGroupByOperator(operatorID: string): Group | undefined {
    for (const group of this.getAllGroups()) {
      if (group.operators.has(operatorID)) {
        return group;
      }
    }
  }

  /**
   * Gets the group that the given link resides in.
   * Returns undefined if there's no such group.
   *
   * @param linkID
   */
  public getGroupByLink(linkID: string): Group | undefined {
    for (const group of this.getAllGroups()) {
      if (group.links.has(linkID)) {
        return group;
      }
    }
  }


  /**
   * Returns an array of all groups in the graph.
   */
  public getAllGroups(): Group[] {
    return Array.from(this.groupIDMap.values());
  }

  /**
   * Returns the boolean value that indicates whether
   * or not sync JointJS changes to texera graph.
   */
  public getSyncTexeraGraph(): boolean {
    return this.syncTexeraGraph;
  }

  /**
   * Gets the given operator's position on the JointJS graph, or its
   * supposed-to-be position if the operator is in a collapsed group.
   *
   * For operators that are supposed to be on the JointJS graph, use
   * getElementPosition() from JointGraphWrapper instead.
   *
   * @param operatorID
   */
  public getOperatorPositionByGroup(operatorID: string): Point {
    const group = this.getGroupByOperator(operatorID);
    if (group && group.collapsed) {
      const operatorInfo = group.operators.get(operatorID);
      if (operatorInfo) {
        return operatorInfo.position;
      } else {
        throw Error(`Internal error: can't find operator ${operatorID} in group ${group.groupID}`);
      }
    } else {
      return this.jointGraphWrapper.getElementPosition(operatorID);
    }
  }

  /**
   * Gets the given link's layer on the JointJS graph, or its
   * supposed-to-be layer if the link is in a collapsed group.
   *
   * For links that are supposed to be on the JointJS graph, use
   * getCellLayer() from JointGraphWrapper instead.
   *
   * @param linkID
   */
  public getLinkLayerByGroup(linkID: string): number {
    const group = this.getGroupByLink(linkID);
    if (group && group.collapsed) {
      const linkInfo = group.links.get(linkID);
      if (linkInfo) {
        return linkInfo.layer;
      } else {
        throw Error(`Internal error: can't find link ${linkID} in group ${group.groupID}`);
      }
    } else {
      return this.jointGraphWrapper.getCellLayer(linkID);
    }
  }
}
