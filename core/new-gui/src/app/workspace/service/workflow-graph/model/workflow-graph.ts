import { Observable, Subject } from "rxjs";
import {
  Comment,
  CommentBox,
  LogicalPort,
  OperatorLink,
  OperatorPredicate,
  PartitionInfo,
  PortDescription,
  PortProperty,
} from "../../../types/workflow-common.interface";
import { isEqual } from "lodash-es";
import { SharedModel } from "./shared-model";
import { CoeditorState, User } from "../../../../common/type/user";
import { createYTypeFromObject, updateYTypeFromObject, YType } from "../../../types/shared-editing.interface";
import { Awareness } from "y-protocols/awareness";
import * as Y from "yjs";

// define the restricted methods that could change the graph
type restrictedMethods =
  | "sharedModel"
  | "newYDocLoadedSubject"
  | "addOperator"
  | "deleteOperator"
  | "addLink"
  | "deleteLink"
  | "deleteLinkWithID"
  | "setOperatorProperty"
  | "addPort"
  | "removePort"
  | "setLinkBreakpoint"
  | "operatorAddSubject"
  | "operatorDeleteSubject"
  | "operatorDisplayNameChangedSubject"
  | "linkAddSubject"
  | "linkDeleteSubject"
  | "operatorPropertyChangeSubject"
  | "breakpointChangeStream"
  | "commentBoxAddSubject"
  | "commentBoxDeleteSubject"
  | "commentBoxAddCommentSubject"
  | "commentBoxDeleteCommentSubject"
  | "commentBoxEditCommentSubject";

/**
 * WorkflowGraphReadonly is a type that only contains the readonly methods of WorkflowGraph.
 *
 * Methods that could alter the graph: add/delete operator or link, set operator property
 *  are omitted from this type.
 */
export type WorkflowGraphReadonly = Omit<WorkflowGraph, restrictedMethods>;
type OperatorPropertiesType = Readonly<{ [key: string]: any }>;

export const PYTHON_UDF_V2_OP_TYPE = "PythonUDFV2";
export const PYTHON_UDF_SOURCE_V2_OP_TYPE = "PythonUDFSourceV2";
export const DUAL_INPUT_PORTS_PYTHON_UDF_V2_OP_TYPE = "DualInputPortsPythonUDFV2";
export const VIEW_RESULT_OP_TYPE = "SimpleSink";
export const VIEW_RESULT_OP_NAME = "View Results";

export function isSink(operator: OperatorPredicate): boolean {
  return operator.operatorType.toLocaleLowerCase().includes("sink");
}

export function isPythonUdf(operator: OperatorPredicate): boolean {
  return [PYTHON_UDF_V2_OP_TYPE, PYTHON_UDF_SOURCE_V2_OP_TYPE, DUAL_INPUT_PORTS_PYTHON_UDF_V2_OP_TYPE].includes(
    operator.operatorType
  );
}

/**
 * WorkflowGraph represents the Texera's logical WorkflowGraph,
 *  it's a graph consisted of operators <OperatorPredicate> and links <OperatorLink>,
 *  each operator and link has its own unique ID.
 *
 */
export class WorkflowGraph {
  public sharedModel: SharedModel = new SharedModel();
  public newYDocLoadedSubject = new Subject();

  public readonly operatorAddSubject = new Subject<OperatorPredicate>();

  public readonly operatorDeleteSubject = new Subject<{
    deletedOperatorID: string;
  }>();
  public readonly disabledOperatorChangedSubject = new Subject<{
    newDisabled: string[];
    newEnabled: string[];
  }>();
  public readonly viewResultOperatorChangedSubject = new Subject<{
    newViewResultOps: string[];
    newUnviewResultOps: string[];
  }>();
  public readonly reuseOperatorChangedSubject = new Subject<{
    newReuseCacheOps: string[];
    newUnreuseCacheOps: string[];
  }>();
  public readonly operatorDisplayNameChangedSubject = new Subject<{
    operatorID: string;
    newDisplayName: string;
  }>();
  public readonly linkAddSubject = new Subject<OperatorLink>();
  public readonly linkDeleteSubject = new Subject<{
    deletedLink: OperatorLink;
  }>();
  public readonly operatorVersionChangedSubject = new Subject<{
    operatorID: string;
    newOperatorVersion: string;
  }>();
  public readonly operatorPropertyChangeSubject = new Subject<{
    operator: OperatorPredicate;
  }>();
  public readonly breakpointChangeStream = new Subject<{
    oldBreakpoint: object | undefined;
    linkID: string;
  }>();
  public readonly portAddedOrDeletedSubject = new Subject<{
    newOperator: OperatorPredicate;
  }>();
  public readonly commentBoxAddSubject = new Subject<CommentBox>();
  public readonly commentBoxDeleteSubject = new Subject<{ deletedCommentBox: CommentBox }>();
  public readonly commentBoxAddCommentSubject = new Subject<{ addedComment: Comment; commentBox: CommentBox }>();
  public readonly commentBoxDeleteCommentSubject = new Subject<{ commentBox: CommentBox }>();
  public readonly commentBoxEditCommentSubject = new Subject<{ commentBox: CommentBox }>();

  public readonly portDisplayNameChangedSubject = new Subject<{
    operatorID: string;
    portID: string;
    newDisplayName: string;
  }>();

  public readonly portPropertyChangedSubject = new Subject<{
    operatorPortID: LogicalPort;
    newProperty: PortProperty;
  }>();

  private syncTexeraGraph = true;
  private syncJointGraph = true;

  constructor(
    operatorPredicates: OperatorPredicate[] = [],
    operatorLinks: OperatorLink[] = [],
    commentBoxes: CommentBox[] = []
  ) {
    operatorPredicates.forEach(op => this.sharedModel.operatorIDMap.set(op.operatorID, createYTypeFromObject(op)));
    operatorLinks.forEach(link => this.sharedModel.operatorLinkMap.set(link.linkID, link));
    commentBoxes.forEach(commentBox =>
      this.sharedModel.commentBoxMap.set(commentBox.commentBoxID, createYTypeFromObject(commentBox))
    );
    this.newYDocLoadedSubject.next(undefined);
  }

  /**
   * Returns the boolean value that indicates whether
   * or not sync JointJS changes to texera graph.
   */
  public getSyncTexeraGraph(): boolean {
    return this.syncTexeraGraph;
  }

  public setSyncJointGraph(syncJointGraph: boolean): void {
    this.syncJointGraph = syncJointGraph;
  }

  public getSyncJointGraph(): boolean {
    return this.syncJointGraph;
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                                     Below are shared-editing-related methods.                                    //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Exposes a shared operator type for fine-grained control. Do not use if not familiar with yjs.
   * @param operatorID
   */
  public getSharedOperatorType(operatorID: string): YType<OperatorPredicate> {
    this.assertOperatorExists(operatorID);
    return this.sharedModel.operatorIDMap.get(operatorID) as YType<OperatorPredicate>;
  }

  public getSharedOperatorPropertyType(operatorID: string): YType<OperatorPropertiesType> {
    return this.getSharedOperatorType(operatorID).get("operatorProperties") as YType<OperatorPropertiesType>;
  }

  public getSharedPortDescriptionType(operatorPortID: LogicalPort): YType<PortDescription> | undefined {
    const isInput = operatorPortID?.portID.includes("input");
    const portListObject = isInput
      ? this.getOperator(operatorPortID.operatorID).inputPorts
      : this.getOperator(operatorPortID.operatorID).outputPorts;
    const portIdx = portListObject.findIndex(portDescription => portDescription.portID === operatorPortID?.portID);
    if (portIdx === -1) return undefined;
    return this.getSharedOperatorType(<string>operatorPortID?.operatorID)
      .get(isInput ? "inputPorts" : "outputPorts")
      .get(portIdx) as YType<PortDescription>;
  }

  /**
   * Exposes a shared comment box type for fine-grained control. Do not use if not familiar with yjs.
   * @param commentBoxID
   */
  public getSharedCommentBoxType(commentBoxID: string): YType<CommentBox> {
    this.assertCommentBoxExists(commentBoxID);
    return this.sharedModel.commentBoxMap.get(commentBoxID) as YType<CommentBox>;
  }

  /**
   * Get the awareness API to connect a shared type to other third-party shared-editing libraries.
   */
  public getSharedModelAwareness(): Awareness {
    return this.sharedModel.awareness;
  }

  /**
   * Updates a particular field of local awareness state info. Will only execute update when user info is provided.
   * @param field the name of the particular state info.
   * @param value the updated state info.
   */
  public updateSharedModelAwareness<K extends keyof CoeditorState>(field: K, value: CoeditorState[K]) {
    this.sharedModel.updateAwareness(field, value);
  }

  /**
   * Replaces current <code>{@link sharedModel}</code>  with a new one and destroy the old model if any.
   * @param workflowId optional, but needed if you want to join shared editing.
   * @param user optional, but needed if you want to have user presence.
   */
  public loadNewYModel(workflowId?: number, user?: User) {
    this.destroyYModel();
    this.sharedModel = new SharedModel(workflowId, user);
    this.newYDocLoadedSubject.next(undefined);
  }

  /**
   * Destroys shared-editing related structures and quits the shared editing session.
   */
  public destroyYModel(): void {
    this.sharedModel.destroy();
  }

  /**
   * Sets the boolean value that specifies whether sync JointJS changes to texera graph.
   */
  public setSyncTexeraGraph(syncTexeraGraph: boolean): void {
    this.syncTexeraGraph = syncTexeraGraph;
  }

  /**
   * Groups a bunch of actions into one atomic transaction, so that they can be undone/redone in one call.
   * @param callback Put whatever need to be atomically done within this callback function.
   */
  public bundleActions(callback: Function) {
    this.sharedModel.transact(callback);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //                                           Below are action methods.                                              //
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Adds a new operator to the graph.
   * Throws an error the operator has a duplicate operatorID with an existing operator.
   * @param operator <code>{@link OperatorPredicate}</code> will be converted to a <code>{@link YType}</code> brefore
   * adding to the internal Y-graph.
   */
  public addOperator(operator: OperatorPredicate): void {
    this.assertOperatorNotExists(operator.operatorID);
    const newOp = createYTypeFromObject(operator);
    this.sharedModel.operatorIDMap.set(operator.operatorID, newOp);
  }

  /**
   * Adds a comment box to the graph.
   * @param commentBox <code>{@link CommentBox}</code> will be converted to a <code>{@link YType}</code> before adding
   * to the internal Y-graph.
   */
  public addCommentBox(commentBox: CommentBox): void {
    this.assertCommentBoxNotExists(commentBox.commentBoxID);
    const newCommentBox = createYTypeFromObject(commentBox);
    this.sharedModel.commentBoxMap.set(commentBox.commentBoxID, newCommentBox);
  }

  /**
   * Adds a single comment to an existing comment box.
   * @param comment the comment's content encapsulated in the <code>{@link Comment}</code> structure. It will be added
   * as-is to the list of comments, i.e., it won't be converted to <code>{@link YType}</code>.
   * @param commentBoxID the id of the comment box to add comment to.
   */
  public addCommentToCommentBox(comment: Comment, commentBoxID: string): void {
    this.assertCommentBoxExists(commentBoxID);
    const commentBox = this.sharedModel.commentBoxMap.get(commentBoxID) as YType<CommentBox>;
    if (commentBox != null) {
      commentBox.get("comments").push([comment]);
    }
  }

  /**
   * Searches the comment list by <code>creatorID</code> and <code>creationTime</code> and deletes the comment if found.
   * The deletion is on a y-list.
   * @param creatorID
   * @param creationTime
   * @param commentBoxID
   */
  public deleteCommentFromCommentBox(creatorID: number, creationTime: string, commentBoxID: string): void {
    this.assertCommentBoxExists(commentBoxID);
    const commentBox = this.sharedModel.commentBoxMap.get(commentBoxID) as YType<CommentBox>;
    if (commentBox != null) {
      commentBox.get("comments").forEach((comment, index) => {
        if (comment.creatorID === creatorID && comment.creationTime === creationTime) {
          commentBox.get("comments").delete(index);
        }
      });
    }
  }

  /**
   * Edits a given comment. Due to yjs's limitation, the modification is actually done by
   * deleting and adding (in place).
   * @param creatorID
   * @param creationTime
   * @param commentBoxID
   * @param content
   */
  public editCommentInCommentBox(creatorID: number, creationTime: string, commentBoxID: string, content: string): void {
    this.assertCommentBoxExists(commentBoxID);
    const commentBox = this.sharedModel.commentBoxMap.get(commentBoxID);
    if (commentBox != null) {
      commentBox.get("comments").forEach((comment, index) => {
        if (comment.creatorID === creatorID && comment.creationTime === creationTime) {
          let creatorName = comment.creatorName;
          let newComment: Comment = { content, creationTime, creatorName, creatorID };
          this.sharedModel.yDoc.transact(() => {
            commentBox.get("comments").delete(index);
            commentBox.get("comments").insert(index, [newComment]);
          });
        }
      });
    }
  }

  /**
   * Deletes the operator from the graph by its ID. The deletion is on a y-map.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  public deleteOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    this.sharedModel.operatorIDMap.delete(operatorID);
  }

  /**
   * Deletes the comment box from the model's <code>{@link commentBoxMap}</code>.
   * @param commentBoxID
   */
  public deleteCommentBox(commentBoxID: string): void {
    const commentBox = this.getCommentBox(commentBoxID);
    if (!commentBox) {
      throw new Error(`CommentBox with ID ${commentBoxID} does not exist`);
    }
    this.sharedModel.commentBoxMap.delete(commentBoxID);
  }

  /**
   * Disables the operator by setting the <code>isDisabled</code> attribute in the corresponding operator from the map.
   * @param operatorID
   */
  public disableOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (this.isOperatorDisabled(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("isDisabled", true);
  }

  /**
   * Enables the operator by setting the <code>isDisabled</code> attribute in the corresponding operator from the map.
   * @param operatorID
   */
  public enableOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (!this.isOperatorDisabled(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("isDisabled", false);
  }

  /**
   * Will use string instead of Y.Text since this is not supposed to be shared-editable. Also the event stream
   * is emitted synchronously since this does not need to be shared-edited.
   */
  public changeOperatorVersion(operatorID: string, newOperatorVersion: string): void {
    const operator = this.getOperator(operatorID);
    if (operator.operatorVersion === newOperatorVersion) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("operatorVersion", newOperatorVersion as any);
    this.operatorVersionChangedSubject.next({ operatorID, newOperatorVersion });
  }

  /**
   * This method gets this status from readonly object version of the operator data as opposed to y-type data.
   * @param operatorID
   */
  public isOperatorDisabled(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.isDisabled ?? false;
  }

  /**
   * Gets disabled operators by filtering from all <code>operatorIDs</code> in the <code>OperatorIDMap</code>.
   */
  public getDisabledOperators(): ReadonlySet<string> {
    return new Set(
      Array.from(this.sharedModel.operatorIDMap.keys() as IterableIterator<string>).filter(op =>
        this.isOperatorDisabled(op)
      )
    );
  }

  /**
   * Changes <code>isViewingResult</code> status which is an atomic boolean value as opposed to y-type data.
   * @param operatorID
   */
  public setViewOperatorResult(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (isSink(operator)) {
      return;
    }
    if (this.isViewingResult(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("viewResult", true);
  }

  /**
   * Changes <code>isViewingResult</code> status which is an atomic boolean value as opposed to y-type data.
   * @param operatorID
   */
  public unsetViewOperatorResult(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (!this.isViewingResult(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("viewResult", false);
  }

  /**
   * This method gets this status from readonly object version of the operator data as opposed to y-type data.
   * @param operatorID
   */
  public isViewingResult(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.viewResult ?? false;
  }

  public getOperatorsToViewResult(): ReadonlySet<string> {
    return new Set(
      Array.from(this.sharedModel.operatorIDMap.keys() as IterableIterator<string>).filter(op =>
        this.isViewingResult(op)
      )
    );
  }

  /**
   * Changes <code>markedForReuse</code> status which is an atomic boolean value as opposed to y-type data.
   * @param operatorID
   */
  public markReuseResult(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (isSink(operator)) {
      return;
    }
    if (this.isMarkedForReuseResult(operatorID)) {
      return;
    }
    console.log("seeting marked for reuse in shared model");
    this.sharedModel.operatorIDMap.get(operatorID)?.set("markedForReuse", true);
  }

  /**
   * Changes <code>markedForReuse</code> status which is an atomic boolean value as opposed to y-type data.
   * @param operatorID
   */
  public removeMarkReuseResult(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (!this.isMarkedForReuseResult(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("markedForReuse", false);
  }

  /**
   * This method gets this status from readonly object version of the operator data as opposed to y-type data.
   * @param operatorID
   */
  public isMarkedForReuseResult(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.markedForReuse ?? false;
  }

  public getOperatorsMarkedForReuseResult(): ReadonlySet<string> {
    return new Set(
      Array.from(this.sharedModel.operatorIDMap.keys() as IterableIterator<string>).filter(op =>
        this.isMarkedForReuseResult(op)
      )
    );
  }

  /**
   * Returns whether the operator exists in the graph.
   * @param operatorID operator ID
   */
  public hasOperator(operatorID: string): boolean {
    return this.sharedModel.operatorIDMap.has(operatorID) as boolean;
  }

  /**
   * Returns whether the comment box exists in the graph.
   * @param commentBoxId
   */
  public hasCommentBox(commentBoxId: string): boolean {
    return this.sharedModel.commentBoxMap.has(commentBoxId);
  }

  /**
   * Returns whether the element exists in the graph.
   * Can be an operator, comment box, or link.
   * @param id element ID
   */
  public hasElementWithID(id: string): boolean {
    return this.hasOperator(id) || this.hasCommentBox(id) || this.hasLinkWithID(id);
  }

  /**
   * Gets the operator with the operatorID. The object version of the operator is returned, as opposed to y-type data.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  public getOperator(operatorID: string): OperatorPredicate {
    if (!this.sharedModel.operatorIDMap.has(operatorID)) {
      throw new Error(`operator ${operatorID} does not exist`);
    }
    const yoperator = this.sharedModel.operatorIDMap.get(operatorID) as YType<OperatorPredicate>;
    return yoperator.toJSON();
  }

  /**
   * Gets the comment box with the commentBoxID. The object version is returned, as opposed to y-type data.
   * Throws an Error if the comment box doesn't exist.
   * @param commentBoxID
   */
  public getCommentBox(commentBoxID: string): CommentBox {
    const commentBox = this.sharedModel.commentBoxMap.get(commentBoxID) as YType<CommentBox>;
    if (!commentBox) {
      throw new Error(`commentBox ${commentBoxID} does not exist`);
    }
    return commentBox.toJSON();
  }

  /**
   * Returns an array of all operators in the graph.
   */
  public getAllOperators(): OperatorPredicate[] {
    return Array.from(this.sharedModel.operatorIDMap.values() as IterableIterator<YType<OperatorPredicate>>).map(v =>
      v.toJSON()
    );
  }

  /**
   * Returns an array of all enabled operators in the graph.
   */
  public getAllEnabledOperators(): ReadonlyArray<OperatorPredicate> {
    return Array.from(this.sharedModel.operatorIDMap.values() as IterableIterator<YType<OperatorPredicate>>)
      .map(v => v.toJSON())
      .filter(op => !this.isOperatorDisabled(op.operatorID));
  }

  /**
   * Returns an array of all the comment boxes in the graph.
   */
  public getAllCommentBoxes(): CommentBox[] {
    return Array.from(this.sharedModel.commentBoxMap.values() as IterableIterator<YType<CommentBox>>).map(v =>
      v.toJSON()
    );
  }

  public addPort(operatorID: string, port: PortDescription, isInput: boolean): void {
    this.assertOperatorExists(operatorID);
    if (isInput) {
      const inputPorts = this.sharedModel.operatorIDMap.get(operatorID)?.get("inputPorts") as Y.Array<
        YType<PortDescription>
      >;
      inputPorts.push([createYTypeFromObject<PortDescription>(port)]);
    } else {
      const outputPorts = this.sharedModel.operatorIDMap.get(operatorID)?.get("outputPorts") as Y.Array<
        YType<PortDescription>
      >;
      outputPorts.push([createYTypeFromObject<PortDescription>(port)]);
    }
  }

  public removePort(operatorID: string, isInput: boolean): void {
    this.assertOperatorExists(operatorID);
    if (isInput) {
      const inputPorts = this.sharedModel.operatorIDMap.get(operatorID)?.get("inputPorts") as Y.Array<
        YType<PortDescription>
      >;
      inputPorts.delete(inputPorts.length - 1, 1);
    } else {
      const outputPorts = this.sharedModel.operatorIDMap.get(operatorID)?.get("outputPorts") as Y.Array<
        YType<PortDescription>
      >;
      outputPorts.delete(outputPorts.length - 1, 1);
    }
  }

  public hasPort(operatorPortID: LogicalPort): boolean {
    if (!this.hasOperator(operatorPortID.operatorID)) return false;
    const operator = this.getOperator(operatorPortID.operatorID);
    if (operatorPortID.portID.includes("input")) {
      return (
        operator.inputPorts.find(portDescription => portDescription.portID === operatorPortID.portID) !== undefined
      );
    } else if (operatorPortID.portID.includes("output")) {
      return (
        operator.outputPorts.find(portDescription => portDescription.portID === operatorPortID.portID) !== undefined
      );
    } else return false;
  }

  public getPortDescription(operatorPortID: LogicalPort): PortDescription | undefined {
    if (!this.hasPort(operatorPortID))
      throw new Error(`operator port ${(operatorPortID.operatorID, operatorPortID.portID)} does not exist`);
    const operator = this.getOperator(operatorPortID.operatorID);
    if (operatorPortID.portID.includes("input")) {
      return operator.inputPorts.find(portDescription => portDescription.portID === operatorPortID.portID);
    } else if (operatorPortID.portID.includes("output")) {
      return operator.outputPorts.find(portDescription => portDescription.portID === operatorPortID.portID);
    } else return undefined;
  }

  /**
   * Adds a link to the operator graph.
   * Throws an error if
   *  - the link already exists in the graph (duplicate ID or source-target)
   *  - the link is invalid (invalid source or target operator/port)
   * @param link
   */
  public addLink(link: OperatorLink): void {
    this.assertLinkNotExists(link);
    this.assertLinkIsValid(link);
    this.sharedModel.operatorLinkMap.set(link.linkID, link);
  }

  /**
   * Deletes a link by the linkID.
   * Throws an error if the linkID doesn't exist in the graph
   * @param linkID link ID
   */
  public deleteLinkWithID(linkID: string): void {
    const link = this.getLinkWithID(linkID);
    if (!link) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    this.sharedModel.operatorLinkMap.delete(linkID);
  }

  /**
   * Deletes a link by the source and target of the link.
   * Throws an error if the link doesn't exist in the graph
   * @param source source port
   * @param target target port
   */
  public deleteLink(source: LogicalPort, target: LogicalPort): void {
    const link = this.getLink(source, target);
    if (!link) {
      throw new Error(`link from ${source.operatorID}.${source.portID}
        to ${target.operatorID}.${target.portID} doesn't exist`);
    }
    this.sharedModel.operatorLinkMap.delete(link.linkID);
  }

  /**
   * Returns whether the graph contains the link with the linkID
   * @param linkID link ID
   */
  public hasLinkWithID(linkID: string): boolean {
    return this.sharedModel.operatorLinkMap.has(linkID);
  }

  /**
   * Returns whether the graph contains the link with the source and target
   * @param source source operator and port of the link
   * @param target target operator and port of the link
   */
  public hasLink(source: LogicalPort, target: LogicalPort): boolean {
    try {
      const link = this.getLink(source, target);
      return true;
    } catch (e) {
      return false;
    }
  }

  public isLinkEnabled(linkID: string): boolean {
    const link = this.getLinkWithID(linkID);
    return !this.isOperatorDisabled(link.source.operatorID) && !this.isOperatorDisabled(link.target.operatorID);
  }

  /**
   * Returns a link with the linkID from operatorLinkMap.
   * Throws an error if the link doesn't exist.
   * @param linkID link ID
   */
  public getLinkWithID(linkID: string): OperatorLink {
    const link = this.sharedModel.operatorLinkMap.get(linkID);
    if (!link) {
      throw new Error(`link ${linkID} does not exist`);
    }
    return link;
  }

  /**
   * Returns a link with the source and target from operatorLinkMap.
   * Returns undefined if the link doesn't exist.
   * @param source source operator and port of the link
   * @param target target operator and port of the link
   */
  public getLink(source: LogicalPort, target: LogicalPort): OperatorLink {
    const links = this.getAllLinks().filter(value => isEqual(value.source, source) && isEqual(value.target, target));
    if (links.length === 0) {
      throw new Error(`link with source ${source} and target ${target} does not exist`);
    }
    if (links.length > 1) {
      throw new Error("WorkflowGraph inconsistency: find duplicate links with same source and target");
    }
    return links[0];
  }

  /**
   * Returns an array of all the links in the graph.
   */
  public getAllLinks(): OperatorLink[] {
    return Array.from(this.sharedModel.operatorLinkMap.values());
  }

  /**
   * Returns an array of all the enabled links in the graph.
   */
  public getAllEnabledLinks(): ReadonlyArray<OperatorLink> {
    return Array.from(this.sharedModel.operatorLinkMap.values()).filter(link => this.isLinkEnabled(link.linkID));
  }

  /**
   * Returns an array of all input links of an operator in the graph.
   * @param operatorID
   */
  public getInputLinksByOperatorId(operatorID: string): OperatorLink[] {
    return this.getAllLinks().filter(link => link.target.operatorID === operatorID);
  }

  /**
   * Returns an array of all output links of an operator in the graph.
   * @param operatorID
   */
  public getOutputLinksByOperatorId(operatorID: string): OperatorLink[] {
    return this.getAllLinks().filter(link => link.source.operatorID === operatorID);
  }

  /**
   * Sets the property of the operator to use the newProperty object.
   * Will create a new y-object based on the new property, so <b>the old y-object will be replaced</b> and as such
   * fine-grained shared-editing will <b>NOT</b> be enabled.
   *
   * Also updates local awareness for changed property status.
   *
   * Throws an error if the operator doesn't exist.
   * @param operatorID operator ID
   * @param newProperty new property to set, the new y-object created from this will replace the old structure.
   */
  public setOperatorProperty(operatorID: string, newProperty: object): void {
    if (!this.hasOperator(operatorID)) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    //
    // const previousProperty = this.getSharedOperatorType(operatorID).get(
    //   "operatorProperties"
    // ) as YType<OperatorPropertiesType>;
    // set the new copy back to the operator ID map
    // TODO: we temporarily disable this due to Yjs update causing issues in Formly.
    this.getSharedOperatorType(operatorID).set("operatorProperties", createYTypeFromObject(newProperty));
    // updateYTypeFromObject(previousProperty, newProperty);
  }

  public setPortProperty(operatorPortID: LogicalPort, newProperty: object) {
    newProperty = newProperty as PortProperty;
    if (!this.hasPort(operatorPortID))
      throw new Error(`operator port ${(operatorPortID.operatorID, operatorPortID.portID)} does not exist`);
    const portDescriptionSharedType = this.getSharedPortDescriptionType(operatorPortID);
    if (portDescriptionSharedType === undefined) return;
    portDescriptionSharedType.set(
      "partitionRequirement",
      createYTypeFromObject<PartitionInfo>((newProperty as PortProperty).partitionInfo) as unknown as PartitionInfo
    );
    portDescriptionSharedType.set(
      "dependencies",
      createYTypeFromObject<Array<{ id: number; internal: boolean }>>(
        (newProperty as PortProperty).dependencies
      ) as unknown as Y.Array<number>
    );
  }

  /**
   * Gets the observable event stream of an operator being added into the graph.
   */
  public getOperatorAddStream(): Observable<OperatorPredicate> {
    return this.operatorAddSubject.asObservable();
  }

  /**
   * Gets the observable event stream of an operator being deleted from the graph.
   * The observable value is only the deleted operator's ID since a deleted YMap
   * (the internal structure of the operator) cannot be retrieved.
   */
  public getOperatorDeleteStream(): Observable<{
    deletedOperatorID: string;
  }> {
    return this.operatorDeleteSubject.asObservable();
  }

  public getDisabledOperatorsChangedStream(): Observable<{
    newDisabled: ReadonlyArray<string>;
    newEnabled: ReadonlyArray<string>;
  }> {
    return this.disabledOperatorChangedSubject.asObservable();
  }

  public getCommentBoxAddStream(): Observable<CommentBox> {
    return this.commentBoxAddSubject.asObservable();
  }

  public getCommentBoxDeleteStream(): Observable<{ deletedCommentBox: CommentBox }> {
    return this.commentBoxDeleteSubject.asObservable();
  }

  public getCommentBoxAddCommentStream(): Observable<{ addedComment: Comment; commentBox: CommentBox }> {
    return this.commentBoxAddCommentSubject.asObservable();
  }

  public getCommentBoxDeleteCommentStream(): Observable<{ commentBox: CommentBox }> {
    return this.commentBoxDeleteCommentSubject.asObservable();
  }

  public getCommentBoxEditCommentStream(): Observable<{ commentBox: CommentBox }> {
    return this.commentBoxEditCommentSubject.asObservable();
  }

  public getViewResultOperatorsChangedStream(): Observable<{
    newViewResultOps: ReadonlyArray<string>;
    newUnviewResultOps: ReadonlyArray<string>;
  }> {
    return this.viewResultOperatorChangedSubject.asObservable();
  }

  public getReuseCacheOperatorsChangedStream(): Observable<{
    newReuseCacheOps: ReadonlyArray<string>;
    newUnreuseCacheOps: ReadonlyArray<string>;
  }> {
    return this.reuseOperatorChangedSubject.asObservable();
  }

  public getOperatorDisplayNameChangedStream(): Observable<{
    operatorID: string;
    newDisplayName: string;
  }> {
    return this.operatorDisplayNameChangedSubject.asObservable();
  }

  public getOperatorVersionChangedStream(): Observable<{
    operatorID: string;
    newOperatorVersion: string;
  }> {
    return this.operatorVersionChangedSubject.asObservable();
  }

  /**
   *ets the observable event stream of a link being added into the graph.
   */
  public getLinkAddStream(): Observable<OperatorLink> {
    return this.linkAddSubject.asObservable();
  }

  /**
   * Gets the observable event stream of a link being deleted from the graph.
   * The observable value is the deleted link.
   */
  public getLinkDeleteStream(): Observable<{ deletedLink: OperatorLink }> {
    return this.linkDeleteSubject.asObservable();
  }

  /**
   * Gets the observable event stream of a change in operator's properties.
   * The observable value includes the operator with new property.
   */
  public getOperatorPropertyChangeStream(): Observable<{
    operator: OperatorPredicate;
  }> {
    return this.operatorPropertyChangeSubject.asObservable();
  }

  /**
   * Gets the observable event stream of a link breakpoint is changed.
   */
  public getBreakpointChangeStream(): Observable<{
    oldBreakpoint: object | undefined;
    linkID: string;
  }> {
    return this.breakpointChangeStream.asObservable();
  }

  public getPortAddedOrDeletedStream(): Observable<{
    newOperator: OperatorPredicate;
  }> {
    return this.portAddedOrDeletedSubject.asObservable();
  }

  public getPortDisplayNameChangedSubject(): Observable<{
    operatorID: string;
    portID: string;
    newDisplayName: string;
  }> {
    return this.portDisplayNameChangedSubject;
  }

  public getPortPropertyChangedStream(): Observable<{
    operatorPortID: LogicalPort;
    newProperty: PortProperty;
  }> {
    return this.portPropertyChangedSubject.asObservable();
  }

  /**
   * Checks if an operator with the OperatorID already exists in the graph.
   * Throws an Error if the operator doesn't exist.
   * @param graph
   * @param operator
   */
  public assertOperatorExists(operatorID: string): void {
    if (!this.hasOperator(operatorID)) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
  }

  public assertCommentBoxExists(commentBoxID: string): void {
    if (!this.hasCommentBox(commentBoxID)) {
      throw new Error(`commentBox with ID ${commentBoxID} does not exist`);
    }
  }

  /**
   * Checks if an operator
   * Throws an Error if there's a duplicate operator ID
   * @param graph
   * @param operator
   */
  public assertOperatorNotExists(operatorID: string): void {
    if (this.hasOperator(operatorID)) {
      throw new Error(`operator with ID ${operatorID} already exists`);
    }
  }

  public assertCommentBoxNotExists(commentBoxID: string): void {
    if (this.hasCommentBox(commentBoxID)) {
      throw new Error(`commentBox with ID ${commentBoxID} already exists`);
    }
  }

  /**
   * Asserts that the link doesn't exists in the graph by checking:
   *  - duplicate link ID
   *  - duplicate link source and target
   * Throws an Error if the link already exists.
   * @param graph
   * @param link
   */
  public assertLinkNotExists(link: OperatorLink): void {
    if (this.hasLinkWithID(link.linkID)) {
      throw new Error(`link with ID ${link.linkID} already exists`);
    }
    if (this.hasLink(link.source, link.target)) {
      throw new Error(`link from ${link.source.operatorID}.${link.source.portID}
        to ${link.target.operatorID}.${link.target.portID} already exists`);
    }
  }

  public assertLinkWithIDExists(linkID: string): void {
    if (!this.hasLinkWithID(linkID)) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
  }

  public assertLinkExists(source: LogicalPort, target: LogicalPort): void {
    if (!this.hasLink(source, target)) {
      throw new Error(`link from ${source.operatorID}.${source.portID}
        to ${target.operatorID}.${target.portID} already exists`);
    }
  }

  /**
   * Checks if it's valid to add the given link to the graph.
   * Throws an Error if it's not a valid link because of:
   *  - invalid source operator or port
   *  - invalid target operator or port
   * @param graph
   * @param link
   */
  public assertLinkIsValid(link: OperatorLink): void {
    const sourceOperator = this.getOperator(link.source.operatorID);
    if (!sourceOperator) {
      throw new Error(`link's source operator ${link.source.operatorID} doesn't exist`);
    }

    const targetOperator = this.getOperator(link.target.operatorID);
    if (!targetOperator) {
      throw new Error(`link's target operator ${link.target.operatorID} doesn't exist`);
    }

    if (sourceOperator.outputPorts.find(port => port.portID === link.source.portID) === undefined) {
      throw new Error(`link's source port ${link.source.portID} doesn't exist
          on output ports of the source operator ${link.source.operatorID}`);
    }
    if (targetOperator.inputPorts.find(port => port.portID === link.target.portID) === undefined) {
      throw new Error(`link's target port ${link.target.portID} doesn't exist
          on input ports of the target operator ${link.target.operatorID}`);
    }
  }

  /**
   * Retrieves a subgraph (subDAG) from the workflow graph. This method excludes disabled operators and links.
   *
   * This method can operate in two modes:
   * 1. If a `targetOperatorId` is provided, it performs a depth-first search (DFS) starting from
   *    the specified operator to construct the subDAG.
   * 2. If no `targetOperatorId` is provided, it starts from all terminal operators (operators with no
   *    outgoing links) and aggregates the paths from these sinks to construct the subDAG, potentially
   *    covering the entire DAG if all paths are interconnected.
   *
   * @param targetOperatorId - The unique identifier of the operator from which to start the DFS.
   *                           This parameter is optional. If omitted, the search starts from all
   *                           terminal operators within the graph.
   * @returns An object containing two arrays: `operators` and `links`. The `operators` array
   *          includes all operator objects that are part of the subDAG, and the `links` array
   *          contains all the operator links that connect these operators within the subDAG.
   *
   */
  public getSubDAG(targetOperatorId?: string) {
    const visited: Set<string> = new Set();
    const subDagOperators: OperatorPredicate[] = [];
    const subDagLinks: OperatorLink[] = [];

    function dfs(currentOperatorId: string, graph: WorkflowGraph) {
      if (visited.has(currentOperatorId)) {
        return;
      }

      visited.add(currentOperatorId);

      const currentOperator = graph.getOperator(currentOperatorId);
      if (currentOperator && !currentOperator.isDisabled) {
        subDagOperators.push(currentOperator);

        // Find links connected to the current operator
        const connectedLinks = graph.getAllEnabledLinks().filter(link => link.target.operatorID === currentOperatorId);
        connectedLinks.forEach(link => {
          subDagLinks.push(link);
          dfs(link.source.operatorID, graph);
        });
      }
    }

    if (targetOperatorId !== undefined) {
      dfs(targetOperatorId, this);
    } else {
      // When no target operator ID is provided, start DFS from all terminal operators
      const allOperators = this.getAllOperators();
      const allLinks = this.getAllEnabledLinks();
      const terminalOperators = allOperators.filter(
        operator => !allLinks.some(link => link.source.operatorID === operator.operatorID)
      );

      terminalOperators.forEach(terminalOperator => dfs(terminalOperator.operatorID, this));
    }

    return { operators: subDagOperators, links: subDagLinks };
  }
}
