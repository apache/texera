import { Subject } from "rxjs";
import { Observable } from "rxjs";
import {
  OperatorPredicate,
  OperatorLink,
  OperatorPort,
  Breakpoint,
  Point,
  CommentBox,
  Comment,
  YType,
  createYTypeFromObject,
} from "../../../types/workflow-common.interface";
import { isEqual } from "lodash-es";
import {SharedModel} from "./shared-model";
import {User} from "../../../../common/type/user";
import * as _ from "lodash";
import {YText} from "yjs/dist/src/types/YText";

// define the restricted methods that could change the graph
type restrictedMethods =
  | "addOperator"
  | "deleteOperator"
  | "addLink"
  | "deleteLink"
  | "deleteLinkWithID"
  | "setOperatorProperty"
  | "setLinkBreakpoint"
  | "yDoc"
  | "wsProvider"
  | "sharedModel"
  | "newYDocLoadedSubject"
  | "operatorAddSubject"
  | "operatorDeleteSubject"
  | "disabledOperatorChangedSubject"
  | "cachedOperatorChangedSubject"
  | "operatorDisplayNameChangedSubject"
  | "linkAddSubject"
  | "linkDeleteSubject"
  | "operatorPropertyChangeSubject"
  | "breakpointChangeStream"
  | "commentBoxAddSubject"
  | "commentBoxDeleteSubject"
  | "commentBoxAddCommentSubject"
  | "commentBoxDeleteCommentSubject"
  | "commentBoxEditCommentSubject"
  | "coeditorOperatorHighlightSubject"
  | "coeditorCurrentlyEditingSubject";

/**
 * WorkflowGraphReadonly is a type that only contains the readonly methods of WorkflowGraph.
 *
 * Methods that could alter the graph: add/delete operator or link, set operator property
 *  are omitted from this type.
 */
export type WorkflowGraphReadonly = Omit<WorkflowGraph, restrictedMethods>;

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

  private syncTexeraGraph = true;

  public sharedModel: SharedModel = new SharedModel();

  // Shared-editing related observables.

  public newYDocLoadedSubject = new Subject();
  public readonly coeditorOperatorHighlightSubject = new Subject<{coeditor: User, clientId: number, operatorIds: string[]}[]>();
  public readonly coeditorCurrentlyEditingSubject = new Subject<{coeditor: User, clientId: number, operatorId?: string}[]>();

  // Workflow-graph related observables.

  public readonly operatorAddSubject = new Subject<OperatorPredicate>();

  public readonly operatorDeleteSubject = new Subject<{
    deletedOperatorID: string;
  }>();
  public readonly disabledOperatorChangedSubject = new Subject<{
    newDisabled: string[];
    newEnabled: string[];
  }>();
  public readonly cachedOperatorChangedSubject = new Subject<{
    newCached: string[];
    newUnCached: string[];
  }>();
  public readonly operatorDisplayNameChangedSubject = new Subject<{
    operatorID: string;
    newDisplayName: string;
  }>();
  public readonly linkAddSubject = new Subject<OperatorLink>();
  public readonly linkDeleteSubject = new Subject<{
    deletedLink: OperatorLink;
  }>();
  public readonly operatorPropertyChangeSubject = new Subject<{
    operator: OperatorPredicate;
  }>();
  public readonly breakpointChangeStream = new Subject<{
    oldBreakpoint: object | undefined;
    linkID: string;
  }>();
  public readonly commentBoxAddSubject = new Subject<CommentBox>();
  public readonly commentBoxDeleteSubject = new Subject<{ deletedCommentBox: CommentBox }>();
  public readonly commentBoxAddCommentSubject = new Subject<{ addedComment: Comment; commentBox: CommentBox }>();
  public readonly commentBoxDeleteCommentSubject = new Subject<{ commentBox: CommentBox }>();
  public readonly commentBoxEditCommentSubject = new Subject<{ commentBox: CommentBox }>();

  constructor(
    operatorPredicates: OperatorPredicate[] = [],
    operatorLinks: OperatorLink[] = [],
    commentBoxes: CommentBox[] = []
  ) {
    operatorPredicates.forEach(op => this.sharedModel.operatorIDMap.set(op.operatorID, createYTypeFromObject(op)));
    operatorLinks.forEach(link => this.sharedModel.operatorLinkMap.set(link.linkID, link));
    commentBoxes.forEach(commentBox => this.sharedModel.commentBoxMap.set(commentBox.commentBoxID, createYTypeFromObject(commentBox)));
  }

  /**
   * Returns the boolean value that indicates whether
   * or not sync JointJS changes to texera graph.
   */
  public getSyncTexeraGraph(): boolean {
    return this.syncTexeraGraph;
  }

  // Shared-editing related methods

  /**
   * Exposes the internal <code>{@link SharedModel}</code>.
   */
  public getSharedModel(): SharedModel {
    return this.sharedModel;
  }

  /**
   * Replaces current <code>{@link sharedModel}</code>  with a new one.
   * Note this does NOT destroy the old model explicitly, so <code>{@link destroyYModel}</code> might
   * still need to be called before this method.
   * @param workflowId optional, but needed if want to join shared editing.
   * @param user optional, but needed if want to have user presence.
   */
  public loadNewYModel(workflowId?: number,
                       user?: User) {
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
          this.sharedModel.yDoc.transact(()=>{
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
   * Atomically sets the display name (<code>YText</code>) given a string. Do NOT use if character-level shared editing
   * for this field is needed.
   * @param operatorID
   * @param newDisplayName will be converted to <code>YText</code> before setting.
   */
  public changeOperatorDisplayName(operatorID: string, newDisplayName: string): void {
    const operator = this.getOperator(operatorID);
    if (operator.customDisplayName === newDisplayName) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("customDisplayName", createYTypeFromObject(new String(newDisplayName)) as unknown as YText);
  }

  /**
   * This method gets this status from readonly object version of the operator data (as opposed to y-type data).
   * @param operatorID
   */
  public isOperatorDisabled(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.isDisabled ?? false;
  }

  public getDisabledOperators(): ReadonlySet<string> {
    return new Set(Array.from(this.sharedModel.operatorIDMap.keys() as IterableIterator<string>).filter(op => this.isOperatorDisabled(op)));
  }

  public cacheOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (isSink(operator)) {
      return;
    }
    if (this.isOperatorCached(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("isCached", true);

  }

  public unCacheOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (!this.isOperatorCached(operatorID)) {
      return;
    }
    this.sharedModel.operatorIDMap.get(operatorID)?.set("isCached", false);
  }

  public isOperatorCached(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.isCached ?? false;
  }

  public getCachedOperators(): ReadonlySet<string> {
    return new Set(Array.from(this.sharedModel.operatorIDMap.keys() as IterableIterator<string>).filter(op => this.isOperatorCached(op)));
  }

  /**
   * Returns whether the operator exists in the graph.
   * @param operatorID operator ID
   */
  public hasOperator(operatorID: string): boolean {
    return this.sharedModel.operatorIDMap.has(operatorID) as boolean;
  }

  public hasCommentBox(commentBoxId: string): boolean {
    return this.sharedModel.commentBoxMap.has(commentBoxId);
  }

  /**
   * Gets the operator with the operatorID.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  public getOperator(operatorID: string): OperatorPredicate {
    if (!this.sharedModel.operatorIDMap.has(operatorID)) {
      throw new Error(`operator ${operatorID} does not exist`);
    }
    const yoperator = this.sharedModel.operatorIDMap.get(operatorID) as YType<OperatorPredicate>;
    const operator = yoperator.toJSON();
    return operator;
  }

  public getCommentBox(commentBoxID: string): CommentBox {
    const commentBox = this.sharedModel.commentBoxMap.get(commentBoxID) as YType<CommentBox>;
    if (!commentBox) {
      throw new Error(`commentBox ${commentBoxID} does not exist`);
    }
    return commentBox.toJSON();
  }

  /**
   * Returns an array of all operators in the graph
   */
  public getAllOperators(): OperatorPredicate[] {
    return Array.from(this.sharedModel.operatorIDMap.values() as IterableIterator<YType<OperatorPredicate>>)
      .map(v => v.toJSON());
  }

  public getAllEnabledOperators(): ReadonlyArray<OperatorPredicate> {
    return Array.from(this.sharedModel.operatorIDMap.values() as IterableIterator<YType<OperatorPredicate>>)
      .map(v => v.toJSON())
      .filter(op => !this.isOperatorDisabled(op.operatorID));
  }

  public getAllCommentBoxes(): CommentBox[] {
    return Array.from(this.sharedModel.commentBoxMap.values() as IterableIterator<YType<CommentBox>>)
      .map(v => v.toJSON());
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
    // delete its breakpoint
    this.sharedModel.linkBreakpointMap.delete(linkID);
  }

  /**
   * Deletes a link by the source and target of the link.
   * Throws an error if the link doesn't exist in the graph
   * @param source source port
   * @param target target port
   */
  public deleteLink(source: OperatorPort, target: OperatorPort): void {
    const link = this.getLink(source, target);
    if (!link) {
      throw new Error(`link from ${source.operatorID}.${source.portID}
        to ${target.operatorID}.${target.portID} doesn't exist`);
    }
    this.sharedModel.operatorLinkMap.delete(link.linkID);
    // delete its breakpoint
    this.sharedModel.linkBreakpointMap.delete(link.linkID);
  }

  /**
   * Returns whether the graph contains the link with the linkID
   * @param linkID link ID
   */
  public hasLinkWithID(linkID: string): boolean {
    return this.sharedModel.operatorLinkMap.has(linkID);
  }

  /**
   * Returns wheter the graph contains the link with the source and target
   * @param source source operator and port of the link
   * @param target target operator and port of the link
   */
  public hasLink(source: OperatorPort, target: OperatorPort): boolean {
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
  public getLink(source: OperatorPort, target: OperatorPort): OperatorLink {
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

  public getAllEnabledLinks(): ReadonlyArray<OperatorLink> {
    return Array.from(this.sharedModel.operatorLinkMap.values()).filter(link => this.isLinkEnabled(link.linkID));
  }

  /**
   * Return an array of all input links of an operator in the graph.
   * @param operatorID
   */
  public getInputLinksByOperatorId(operatorID: string): OperatorLink[] {
    return this.getAllLinks().filter(link => link.target.operatorID === operatorID);
  }

  /**
   * Returna an array of all output links of an operator in the graph.
   * @param operatorID
   */
  public getOutputLinksByOperatorId(operatorID: string): OperatorLink[] {
    return this.getAllLinks().filter(link => link.source.operatorID === operatorID);
  }

  /**
   * Sets the property of the operator to use the newProperty object.
   *
   * Throws an error if the operator doesn't exist.
   * @param operatorID operator ID
   * @param newProperty new property to set
   */
  public setOperatorProperty(operatorID: string, newProperty: object): void {
    const previousProperty = this.sharedModel.operatorIDMap.get(operatorID)?.get("operatorProperties").toJSON();
    if (!_.isEqual(previousProperty, newProperty)) {
      const localState = this.sharedModel.awareness.getLocalState();
      if (localState && localState["currentlyEditing"] === operatorID) {
        this.sharedModel.updateAwareness("changed", operatorID);
        this.sharedModel.updateAwareness("changed", undefined);
      }
    }
    // set the new copy back to the operator ID map
    this.sharedModel.operatorIDMap.get(operatorID)?.set("operatorProperties", createYTypeFromObject(newProperty));
  }

  /**
   * set the breakpoint property of a link to be newBreakpoint
   * Throws an error if link doesn't exist
   *
   * @param linkID linkID
   * @param breakpoint
   */
  public setLinkBreakpoint(linkID: string, breakpoint: Breakpoint | undefined): void {
    this.assertLinkWithIDExists(linkID);
    if (breakpoint === undefined || Object.keys(breakpoint).length === 0) {
      this.sharedModel.linkBreakpointMap.delete(linkID);
    } else {
      this.sharedModel.linkBreakpointMap.set(linkID, breakpoint);
    }
  }

  /**
   * get the breakpoint property of a link
   * returns an empty object if the link has no property
   *
   * @param linkID
   */
  public getLinkBreakpoint(linkID: string): Breakpoint | undefined {
    return this.sharedModel.linkBreakpointMap.get(linkID);
  }

  public getAllLinkBreakpoints(): ReadonlyMap<string, Breakpoint> {
    return this.sharedModel.linkBreakpointMap;
  }

  public getAllEnabledLinkBreakpoints(): ReadonlyMap<string, Breakpoint> {
    const enabledBreakpoints = new Map();
    this.sharedModel.linkBreakpointMap.forEach((breakpoint, linkID) => {
      if (this.isLinkEnabled(linkID)) {
        enabledBreakpoints.set(linkID, breakpoint);
      }
    });
    return enabledBreakpoints;
  }

  /**
   * Gets the observable event stream of an operator being added into the graph.
   */
  public getOperatorAddStream(): Observable<OperatorPredicate> {
    return this.operatorAddSubject.asObservable();
  }

  /**
   * Gets the observable event stream of an operator being deleted from the graph.
   * The observable value is the deleted operator.
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

  public getCachedOperatorsChangedStream(): Observable<{
    newCached: ReadonlyArray<string>;
    newUnCached: ReadonlyArray<string>;
  }> {
    return this.cachedOperatorChangedSubject.asObservable();
  }

  public getOperatorDisplayNameChangedStream(): Observable<{
    operatorID: string;
    newDisplayName: string;
  }> {
    return this.operatorDisplayNameChangedSubject.asObservable();
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
   * The observable value includes the old property that is replaced, and the operator with new property.
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

  public getCoeditorOperatorHighlightStream(): Observable<{coeditor: User, clientId: number, operatorIds: string[]}[]> {
    return this.coeditorOperatorHighlightSubject.asObservable();
  }

  public getCoeditorCurrentlyEditingStream(): Observable<{coeditor: User, clientId: number, operatorId?: string}[]> {
    return this.coeditorCurrentlyEditingSubject.asObservable();
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

  public assertLinkExists(source: OperatorPort, target: OperatorPort): void {
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
}
