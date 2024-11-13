import { Point, OperatorPredicate, OperatorLink } from "../../../types/workflow-common.interface";
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

  constructor() {}

  /**
   * Returns whether the group exists in the graph.
   * @param groupID
   */
  public hasGroup(groupID: string): boolean {
    return this.groupIDMap.has(groupID);
  }
}
