import { Subject } from "rxjs";
import { Observable } from "rxjs";
import { WorkflowUtilService } from "../util/workflow-util.service";
import { Point, OperatorPredicate, OperatorLink } from "../../../types/workflow-common.interface";
import { WorkflowGraph } from "./workflow-graph";
import { JointGraphWrapper } from "./joint-graph-wrapper";
import { JointUIService } from "../../joint-ui/joint-ui.service";
import { environment } from "./../../../../../environments/environment";
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

type groupSizeType = {
  groupID: string;
  width: number;
  height: number;
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
  private syncOperatorGroup = true;

  private readonly groupAddStream = new Subject<Group>();
  private readonly groupDeleteStream = new Subject<Group>();
  private readonly groupCollapseStream = new Subject<Group>();
  private readonly groupExpandStream = new Subject<Group>();
  private readonly groupResizeStream = new Subject<groupSizeType>();

  constructor(
    private texeraGraph: WorkflowGraph,
    private jointGraph: joint.dia.Graph,
    private jointGraphWrapper: JointGraphWrapper,
    private workflowUtilService: WorkflowUtilService,
    private jointUIService: JointUIService
  ) {}

  /**
   * Adds a new group to the graph.
   * Throws an error if the group has a duplicate groupID with an existing group.
   *
   * @param group
   */
  public addGroup(group: Group): void {
    this.assertGroupNotExists(group.groupID);
    this.assertGroupIsValid(group);
    this.groupIDMap.set(group.groupID, group);
    this.groupAddStream.next(group);
  }

  /**
   * Deletes the group from the graph by its groupID.
   * Throws an error if the group doesn't exist.
   *
   * @param groupID
   */
  public unGroup(groupID: string): void {
    const group = this.getGroup(groupID);
    this.groupIDMap.delete(groupID);
    this.groupDeleteStream.next(group);
  }

  /**
   * Collapses the given group on the graph.
   * Throws an error if the group is already collapsed.
   *
   * @param groupID
   */
  public collapseGroup(groupID: string): void {
    const group = this.getGroup(groupID);
    this.assertGroupNotCollapsed(group);
    this.setGroupCollapsed(groupID, true);
    this.groupCollapseStream.next(this.getGroup(groupID));
  }

  /**
   * Expands the given group on the graph.
   * Throws an error if the group is not collapsed.
   *
   * @param groupID
   */
  public expandGroup(groupID: string): void {
    const group = this.getGroup(groupID);
    this.assertGroupIsCollapsed(group);
    this.setGroupCollapsed(groupID, false);
    this.groupExpandStream.next(this.getGroup(groupID));
  }

  /**
   * Sets the collapsed flag of the group to the given value.
   * The collapsed flag of a group should only be changed using this method.
   *
   * @param groupID
   * @param collapsed
   */
  public setGroupCollapsed(groupID: string, collapsed: boolean): void {
    const group = this.getGroup(groupID);
    this.groupIDMap.set(groupID, { ...group, collapsed: collapsed });
  }

  /**
   * Adds the operator and links connected to it to the group.
   *
   * Hides the operator and its status tooltip from the JointJS graph
   * if the group is collapsed, otherwise repositions the group to fit
   * the newly added operator.
   *
   * Throws an error if the operator is already in a group.
   *
   * @param operatorID
   * @param groupID
   */
  public addOperatorToGroup(operatorID: string, groupID: string): void {
    const operator = this.texeraGraph.getOperator(operatorID);
    const group = this.getGroup(groupID);

    if (this.getGroupByOperator(operatorID)) {
      throw Error(`operator ${operatorID} already exists in a group`);
    }

    const operatorLayer = this.jointGraphWrapper.getCellLayer(operatorID);
    const groupLayer = this.jointGraphWrapper.getCellLayer(groupID);
    if (operatorLayer <= groupLayer) {
      this.jointGraphWrapper.setCellLayer(operatorID, groupLayer + operatorLayer);
    }

    const position = this.jointGraphWrapper.getElementPosition(operatorID);
    const layer = this.jointGraphWrapper.getCellLayer(operatorID);
    group.operators.set(operatorID, { operator, position, layer });

    this.texeraGraph
      .getAllLinks()
      .filter(link => link.source.operatorID === operatorID)
      .forEach(link => {
        if (group.operators.has(link.target.operatorID)) {
          group.inLinks.splice(group.inLinks.indexOf(link.linkID), 1);
          this.addLinkToGroup(link.linkID, groupID);
        } else {
          this.addOutLinkToGroup(link.linkID, groupID);
        }
      });

    this.texeraGraph
      .getAllLinks()
      .filter(link => link.target.operatorID === operatorID)
      .forEach(link => {
        if (group.operators.has(link.source.operatorID)) {
          group.outLinks.splice(group.outLinks.indexOf(link.linkID), 1);
          this.addLinkToGroup(link.linkID, groupID);
        } else {
          this.addInLinkToGroup(link.linkID, groupID);
        }
      });

    if (group.collapsed) {
      this.setSyncTexeraGraph(false);
      this.jointGraph.getCell(operatorID).remove();
      this.setSyncTexeraGraph(true);
    } else {
      this.repositionGroup(group);
    }
  }

  /**
   * Adds the link to the group as a 'link' of the group.
   *
   * Hides the link from the JointJS graph if the group is collapsed.
   * If the link is already hidden, just add it to the group ID map
   * and set it to the highest layer.
   *
   * Throws an error if the link is already in a group, or if its
   * source and target operators are not both in the group.
   *
   * @param linkID
   * @param groupID
   */
  public addLinkToGroup(linkID: string, groupID: string): void {
    const link = this.texeraGraph.getLinkWithID(linkID);
    const group = this.getGroup(groupID);

    if (this.getGroupByLink(linkID) || this.getGroupByInLink(linkID) || this.getGroupByOutLink(linkID)) {
      throw Error(`link with ID ${linkID} already exists in a group`);
    }
    if (!group.operators.has(link.source.operatorID) || !group.operators.has(link.target.operatorID)) {
      throw Error(`link ${linkID} doesn't qualify as a link of group ${groupID}`);
    }

    if (this.jointGraph.getCell(linkID)) {
      const linkLayer = this.jointGraphWrapper.getCellLayer(linkID);
      const groupLayer = this.jointGraphWrapper.getCellLayer(groupID);
      if (linkLayer <= groupLayer) {
        this.jointGraphWrapper.setCellLayer(linkID, groupLayer + linkLayer);
      }

      const layer = this.jointGraphWrapper.getCellLayer(linkID);
      group.links.set(linkID, { link, layer });

      if (group.collapsed) {
        this.setSyncTexeraGraph(false);
        this.jointGraph.getCell(linkID).remove();
        this.setSyncTexeraGraph(true);
      }
    } else {
      group.links.set(linkID, { link, layer: this.getHighestLayer() + 1 });
    }
  }

  /**
   * Adds the link to the group as an 'in-link' of the group.
   *
   * If the group is collapsed, connect the link's target port to the
   * group on the JointJS graph.
   *
   * Throws an error if the link is already a link or in-link of
   * a group, or if it's not actually an in-link of the group.
   *
   * @param linkID
   * @param groupID
   */
  public addInLinkToGroup(linkID: string, groupID: string): void {
    const link = this.texeraGraph.getLinkWithID(linkID);
    const group = this.getGroup(groupID);

    if (this.getGroupByLink(linkID) || this.getGroupByInLink(linkID)) {
      throw Error(`link with ID ${linkID} already exists in a group`);
    }
    if (group.operators.has(link.source.operatorID) || !group.operators.has(link.target.operatorID)) {
      throw Error(`link ${linkID} doesn't qualify as an in-link of group ${groupID}`);
    }

    const linkLayer = this.jointGraphWrapper.getCellLayer(linkID);
    const groupLayer = this.jointGraphWrapper.getCellLayer(groupID);
    if (linkLayer <= groupLayer) {
      this.jointGraphWrapper.setCellLayer(linkID, groupLayer + linkLayer);
    }

    group.inLinks.push(linkID);

    if (group.collapsed) {
      this.setSyncTexeraGraph(false);
      const jointLinkCell = <joint.dia.Link>this.jointGraph.getCell(linkID);
      jointLinkCell.set("target", { id: groupID });
      this.setSyncTexeraGraph(true);
    }
  }

  /**
   * Adds the link to the group as an 'out-link' of the group.
   *
   * If the group is collapsed, connect the link's source port to the
   * group on the JointJS graph.
   *
   * Throws an error if the link is already a link or out-link of
   * a group, or if it's not actually an out-link of the group.
   *
   * @param linkID
   * @param groupID
   */
  public addOutLinkToGroup(linkID: string, groupID: string): void {
    const link = this.texeraGraph.getLinkWithID(linkID);
    const group = this.getGroup(groupID);

    if (this.getGroupByLink(linkID) || this.getGroupByOutLink(linkID)) {
      throw Error(`link with ID ${linkID} already exists in a group`);
    }
    if (!group.operators.has(link.source.operatorID) || group.operators.has(link.target.operatorID)) {
      throw Error(`link ${linkID} doesn't qualify as an out-link of group ${groupID}`);
    }

    const linkLayer = this.jointGraphWrapper.getCellLayer(linkID);
    const groupLayer = this.jointGraphWrapper.getCellLayer(groupID);
    if (linkLayer <= groupLayer) {
      this.jointGraphWrapper.setCellLayer(linkID, groupLayer + linkLayer);
    }

    group.outLinks.push(linkID);

    if (group.collapsed) {
      this.setSyncTexeraGraph(false);
      const jointLinkCell = <joint.dia.Link>this.jointGraph.getCell(linkID);
      jointLinkCell.set("source", { id: groupID });
      this.setSyncTexeraGraph(true);
    }
  }

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
   * Gets the group of which the given link is an in-link.
   * Returns undefined if there's no such group.
   *
   * @param linkID
   */
  public getGroupByInLink(linkID: string): Group | undefined {
    for (const group of this.getAllGroups()) {
      if (group.inLinks.includes(linkID)) {
        return group;
      }
    }
  }

  /**
   * Gets the group of which the given link is an out-link.
   * Returns undefined if there's no such group.
   *
   * @param linkID
   */
  public getGroupByOutLink(linkID: string): Group | undefined {
    for (const group of this.getAllGroups()) {
      if (group.outLinks.includes(linkID)) {
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
   * Sets the boolean value that indicates whether
   * or not sync JointJS changes to texera graph.
   */
  public setSyncTexeraGraph(syncTexeraGraph: boolean): void {
    this.syncTexeraGraph = syncTexeraGraph;
  }

  /**
   * Returns the boolean value that indicates whether
   * or not sync JointJS changes to group ID map.
   */
  public getSyncOperatorGroup(): boolean {
    return this.syncOperatorGroup;
  }

  /**
   * Sets the boolean value that indicates whether
   * or not sync JointJS changes to group ID map.
   */
  public setSyncOperatorGroup(syncOperatorGroup: boolean): void {
    this.syncOperatorGroup = syncOperatorGroup;
  }

  /**
   * Gets the event stream of a group being added.
   */
  public getGroupAddStream(): Observable<Group> {
    return this.groupAddStream.asObservable();
  }

  /**
   * Gets the event stream of a group being deleted.
   */
  public getGroupDeleteStream(): Observable<Group> {
    return this.groupDeleteStream.asObservable();
  }

  /**
   * Gets the event stream of a group being collapsed.
   */
  public getGroupCollapseStream(): Observable<Group> {
    return this.groupCollapseStream.asObservable();
  }

  /**
   * Gets the event stream of a group being expanded.
   */
  public getGroupExpandStream(): Observable<Group> {
    return this.groupExpandStream.asObservable();
  }

  /**
   * Gets the event stream of a group being resized.
   */
  public getGroupResizeStream(): Observable<groupSizeType> {
    return this.groupResizeStream.asObservable();
  }

  /**
   * Asserts that the group doesn't exist in the graph.
   * Throws an error if there's a duplicate group ID.
   *
   * @param groupID
   */
  public assertGroupNotExists(groupID: string): void {
    if (this.hasGroup(groupID)) {
      throw new Error(`group with ID ${groupID} already exists`);
    }
  }

  /**
   * Asserts that the group is collapsed on the graph.
   * Throws an error if the group is not flaged as collapsed.
   *
   * @param group
   */
  public assertGroupIsCollapsed(group: Group): void {
    if (!group.collapsed) {
      throw Error(`group with ID ${group.groupID} is not collapsed`);
    }
  }

  /**
   * Asserts that the group is not collapsed on the graph.
   * Throws an error if the group is flaged as collapsed.
   *
   * @param group
   */
  public assertGroupNotCollapsed(group: Group): void {
    if (group.collapsed) {
      throw Error(`group with ID ${group.groupID} is already collapsed`);
    }
  }

  /**
   * Checks if it's valid to add the given group to the graph.
   *
   * Throws an error if it's not a valid group because there are:
   *  - less than two operators in the group
   *  - operators that exist in another group
   *  - links that exist in another group
   *
   * @param group
   */
  public assertGroupIsValid(group: Group): void {
    if (group.operators.size < 2) {
      throw Error("group has less than two operators");
    }

    // checks if the group contains operators from another group
    for (const operatorID of Array.from(group.operators.keys())) {
      const duplicateGroup = this.getGroupByOperator(operatorID);
      if (duplicateGroup && duplicateGroup.groupID !== group.groupID) {
        throw Error(`operator ${operatorID} exists in another group`);
      }
    }

    // checks if the group contains links from another group
    for (const linkID of Array.from(group.links.keys())) {
      const duplicateGroup = this.getGroupByLink(linkID);
      if (duplicateGroup && duplicateGroup.groupID !== group.groupID) {
        throw Error(`link ${linkID} exists in another group`);
      }
    }
  }

  /**
   * Returns true if there're at least two operators in the operator
   * list and none of them is already embedded in a group.
   *
   * @param operatorIDs
   */
  public operatorsGroupable(operatorIDs: readonly string[]): boolean {
    for (const operatorID of operatorIDs) {
      if (this.getGroupByOperator(operatorID)) {
        return false;
      }
    }
    return operatorIDs.length > 1;
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
   * Gets the given operator's layer on the JointJS graph, or its
   * supposed-to-be layer if the operator is in a collapsed group.
   *
   * For operators that are supposed to be on the JointJS graph, use
   * getCellLayer() from JointGraphWrapper instead.
   *
   * @param operatorID
   */
  public getOperatorLayerByGroup(operatorID: string): number {
    const group = this.getGroupByOperator(operatorID);
    if (group && group.collapsed) {
      const operatorInfo = group.operators.get(operatorID);
      if (operatorInfo) {
        return operatorInfo.layer;
      } else {
        throw Error(`Internal error: can't find operator ${operatorID} in group ${group.groupID}`);
      }
    } else {
      return this.jointGraphWrapper.getCellLayer(operatorID);
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

  /**
   * Creates a new group for given operators.
   *
   * A new group contains the following:
   *  - groupID: the identifier of the group
   *  - operators: a map of all the operators in the group, including each operator and their position and layer
   *  - links: a map of all the links in the group, including each link and their corresponding layer
   *  - in-links: an array of all the links whose target operators are in the group
   *  - out-links: an array of all the links whose source operators are in the group
   *  - collapsed: a boolean value that indicates whether the group is collaped or expanded
   *
   * @param operatorIDs
   */
  public getNewGroup(operatorIDs: readonly string[], groupID?: string): Group {
    if (!this.operatorsGroupable(operatorIDs)) {
      throw Error("given operators are not groupable");
    }

    if (!groupID) {
      groupID = this.workflowUtilService.getGroupRandomUUID();
    }

    const operators = new Map<string, OperatorInfo>();
    operatorIDs.forEach(operatorID => {
      const operator = this.texeraGraph.getOperator(operatorID);
      const position = this.jointGraphWrapper.getElementPosition(operatorID);
      const layer = this.jointGraphWrapper.getCellLayer(operatorID);
      operators.set(operatorID, { operator, position, layer });
    });

    const links = new Map<string, LinkInfo>();
    this.texeraGraph
      .getAllLinks()
      .filter(link => operators.has(link.source.operatorID) && operators.has(link.target.operatorID))
      .forEach(link => {
        const layer = this.jointGraphWrapper.getCellLayer(link.linkID);
        links.set(link.linkID, { link, layer });
      });

    const inLinks = this.texeraGraph
      .getAllLinks()
      .filter(link => !operators.has(link.source.operatorID) && operators.has(link.target.operatorID))
      .map(link => link.linkID);
    const outLinks = this.texeraGraph
      .getAllLinks()
      .filter(link => operators.has(link.source.operatorID) && !operators.has(link.target.operatorID))
      .map(link => link.linkID);

    return { groupID, operators, links, inLinks, outLinks, collapsed: false };
  }

  /**
   * Gets the bounding box of the group.
   *
   * A bounding box contains two points, defining the group's position and size.
   *  - topLeft indicates the position of the operator (if there was one) that's in
   *    the top left corner of the group
   *  - bottomRight indicates the position of the operator (if there was one) that's
   *    in the bottom right corner of the group
   *
   * @param group
   */
  public getGroupBoundingBox(group: Group): GroupBoundingBox {
    const randomOperator = group.operators.get(Array.from(group.operators.keys())[0]);
    if (!randomOperator) {
      throw new Error(`Internal error: group with ID ${group.groupID} is invalid`);
    }

    const topLeft = {
      x: randomOperator.position.x,
      y: randomOperator.position.y,
    };
    const bottomRight = {
      x: randomOperator.position.x,
      y: randomOperator.position.y,
    };

    group.operators.forEach(operatorInfo => {
      if (operatorInfo.position.x < topLeft.x) {
        topLeft.x = operatorInfo.position.x;
      }
      if (operatorInfo.position.y < topLeft.y) {
        topLeft.y = operatorInfo.position.y;
      }
      if (operatorInfo.position.x > bottomRight.x) {
        bottomRight.x = operatorInfo.position.x;
      }
      if (operatorInfo.position.y > bottomRight.y) {
        bottomRight.y = operatorInfo.position.y;
      }
    });

    return { topLeft, bottomRight };
  }

  /**
   * Gets the layer of the frontmost cell in the graph.
   */
  public getHighestLayer(): number {
    let highestLayer = 0;

    this.texeraGraph.getAllOperators().forEach(operator => {
      const layer = this.getOperatorLayerByGroup(operator.operatorID);
      if (layer > highestLayer) {
        highestLayer = layer;
      }
    });
    this.texeraGraph.getAllLinks().forEach(link => {
      const layer = this.getLinkLayerByGroup(link.linkID);
      if (layer > highestLayer) {
        highestLayer = layer;
      }
    });

    return highestLayer;
  }

  /**
   * Moves the given group to the given layer. All related cells (embedded operators,
   * links, in-links, and out-links) will be moved to corresponding new layers:
   *    own layer + group's new layer
   *
   * @param group
   * @param groupLayer
   */
  public moveGroupToLayer(group: Group, groupLayer: number): void {
    group.operators.forEach((operatorInfo, operatorID) => {
      if (!group.collapsed) {
        this.jointGraphWrapper.setCellLayer(operatorID, operatorInfo.layer + groupLayer);
      }
      operatorInfo.layer += groupLayer;
    });

    group.links.forEach((linkInfo, linkID) => {
      if (!group.collapsed) {
        this.jointGraphWrapper.setCellLayer(linkID, linkInfo.layer + groupLayer);
      }
      linkInfo.layer += groupLayer;
    });

    group.inLinks.forEach(linkID => {
      const layer = this.jointGraphWrapper.getCellLayer(linkID);
      this.jointGraphWrapper.setCellLayer(linkID, layer + groupLayer);
    });

    group.outLinks.forEach(linkID => {
      const layer = this.jointGraphWrapper.getCellLayer(linkID);
      this.jointGraphWrapper.setCellLayer(linkID, layer + groupLayer);
    });

    this.jointGraphWrapper.setCellLayer(group.groupID, groupLayer);
  }

  /**
   * Repositions and resizes the given group to fit its embedded operators.
   * @param group
   */
  public repositionGroup(group: Group): void {
    const { topLeft, bottomRight } = this.getGroupBoundingBox(group);

    // calculate group's new position
    const originalPosition = this.jointGraphWrapper.getElementPosition(group.groupID);
    const offsetX = topLeft.x - JointUIService.DEFAULT_GROUP_MARGIN - originalPosition.x;
    const offsetY = topLeft.y - JointUIService.DEFAULT_GROUP_MARGIN - originalPosition.y;

    // calculate group's new height & width
    const width =
      bottomRight.x - topLeft.x + JointUIService.DEFAULT_OPERATOR_WIDTH + 2 * JointUIService.DEFAULT_GROUP_MARGIN;
    const height =
      bottomRight.y -
      topLeft.y +
      JointUIService.DEFAULT_OPERATOR_HEIGHT +
      JointUIService.DEFAULT_GROUP_MARGIN +
      JointUIService.DEFAULT_GROUP_MARGIN_BOTTOM;

    // reposition the group according to the new position
    const listenPositionChange = this.jointGraphWrapper.getListenPositionChange();
    this.setSyncOperatorGroup(false);
    this.jointGraphWrapper.setListenPositionChange(false);
    this.jointGraphWrapper.setElementPosition(group.groupID, offsetX, offsetY);
    this.setSyncOperatorGroup(true);
    this.jointGraphWrapper.setListenPositionChange(listenPositionChange);

    // resize the group according to the new size
    this.jointGraphWrapper.setElementSize(group.groupID, width, height);
    this.groupResizeStream.next({
      groupID: group.groupID,
      height: height,
      width: width,
    });
  }

  /**
   * Hides operators and links embedded in the given group.
   * in-links and out-links will be reconnected to the group element.
   *
   * Sync texera graph is turned off to prevent JointJS graph changes from
   * propagating to texera graph.
   *
   * @param group
   */
  public hideOperatorsAndLinks(group: Group): void {
    this.setSyncTexeraGraph(false);

    group.links.forEach((linkInfo, linkID) => this.jointGraph.getCell(linkID).remove());

    group.inLinks.forEach(linkID => {
      const jointLinkCell = <joint.dia.Link>this.jointGraph.getCell(linkID);
      jointLinkCell.set("target", { id: group.groupID });
    });

    group.outLinks.forEach(linkID => {
      const jointLinkCell = <joint.dia.Link>this.jointGraph.getCell(linkID);
      jointLinkCell.set("source", { id: group.groupID });
    });

    group.operators.forEach((operatorInfo, operatorID) => {
      this.jointGraph.getCell(operatorID).remove();
    });

    this.setSyncTexeraGraph(true);
  }

  /**
   * Shows operators and links embedded in the group.
   * in-links and out-links will be reconnected back to corresponding operators.
   *
   * Sync texera graph is turned off to prevent JointJS graph changes from
   * propagating to texera graph.
   *
   * @param group
   */
  public showOperatorsAndLinks(group: Group): void {
    this.setSyncTexeraGraph(false);

    group.operators.forEach((operatorInfo, operatorID) => {
      const operatorJointElement = this.jointUIService.getJointOperatorElement(
        operatorInfo.operator,
        operatorInfo.position
      );
      this.jointGraph.addCell(operatorJointElement);
      this.jointGraphWrapper.setCellLayer(operatorID, operatorInfo.layer);
    });

    group.links.forEach((linkInfo, linkID) => {
      const jointLinkCell = JointUIService.getJointLinkCell(linkInfo.link);
      this.jointGraph.addCell(jointLinkCell);
      this.jointGraphWrapper.setCellLayer(linkID, linkInfo.layer);
    });

    group.inLinks.forEach(linkID => {
      const link = this.texeraGraph.getLinkWithID(linkID);
      const jointLinkCell = <joint.dia.Link>this.jointGraph.getCell(linkID);
      jointLinkCell.set("target", {
        id: link.target.operatorID,
        port: link.target.portID,
      });
    });

    group.outLinks.forEach(linkID => {
      const link = this.texeraGraph.getLinkWithID(linkID);
      const jointLinkCell = <joint.dia.Link>this.jointGraph.getCell(linkID);
      jointLinkCell.set("source", {
        id: link.source.operatorID,
        port: link.source.portID,
      });
    });

    this.setSyncTexeraGraph(true);
  }
}
