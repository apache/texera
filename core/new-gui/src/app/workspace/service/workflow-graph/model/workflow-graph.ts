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
} from "../../../types/workflow-common.interface";
import { isEqual } from "lodash-es";

// define the restricted methods that could change the graph
type restrictedMethods =
  | "addOperator"
  | "deleteOperator"
  | "addLink"
  | "deleteLink"
  | "deleteLinkWithID"
  | "setOperatorProperty"
  | "setLinkBreakpoint";

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
  private readonly operatorIDMap = new Map<string, OperatorPredicate>();
  private readonly operatorLinkMap = new Map<string, OperatorLink>();
  private readonly commentBoxMap = new Map<string, CommentBox>();
  private readonly linkBreakpointMap = new Map<string, Breakpoint>();

  private readonly operatorAddSubject = new Subject<OperatorPredicate>();

  private readonly operatorDeleteSubject = new Subject<{
    deletedOperator: OperatorPredicate;
  }>();
  private readonly disabledOperatorChangedSubject = new Subject<{
    newDisabled: string[];
    newEnabled: string[];
  }>();
  private readonly cachedOperatorChangedSubject = new Subject<{
    newCached: string[];
    newUnCached: string[];
  }>();
  private readonly operatorDisplayNameChangedSubject = new Subject<{
    operatorID: string;
    newDisplayName: string;
  }>();
  private readonly linkAddSubject = new Subject<OperatorLink>();
  private readonly linkDeleteSubject = new Subject<{
    deletedLink: OperatorLink;
  }>();
  private readonly operatorPropertyChangeSubject = new Subject<{
    oldProperty: object;
    operator: OperatorPredicate;
  }>();
  private readonly breakpointChangeStream = new Subject<{
    oldBreakpoint: object | undefined;
    linkID: string;
  }>();
  private readonly commentBoxAddSubject = new Subject<CommentBox>();
  private readonly commentBoxDeleteSubject = new Subject<{ deletedCommentBox: CommentBox }>();
  private readonly commentBoxAddCommentSubject = new Subject<{ addedComment: Comment; commentBox: CommentBox }>();

  constructor(
    operatorPredicates: OperatorPredicate[] = [],
    operatorLinks: OperatorLink[] = [],
    commentBoxes: CommentBox[] = []
  ) {
    operatorPredicates.forEach(op => this.operatorIDMap.set(op.operatorID, op));
    operatorLinks.forEach(link => this.operatorLinkMap.set(link.linkID, link));
    commentBoxes.forEach(commentBox => this.commentBoxMap.set(commentBox.commentBoxID, commentBox));
  }

  /**
   * Adds a new operator to the graph.
   * Throws an error the operator has a duplicate operatorID with an existing operator.
   * @param operator OperatorPredicate
   */
  public addOperator(operator: OperatorPredicate): void {
    this.assertOperatorNotExists(operator.operatorID);
    this.operatorIDMap.set(operator.operatorID, operator);
    this.operatorAddSubject.next(operator);
  }

  public addCommentBox(commentBox: CommentBox): void {
    this.assertCommentBoxNotExists(commentBox.commentBoxID);
    this.commentBoxMap.set(commentBox.commentBoxID, commentBox);
    this.commentBoxAddSubject.next(commentBox);
  }

  public addCommentToCommentBox(comment: Comment, commentBoxID: string): void {
    this.assertCommentBoxExists(commentBoxID);
    const commentBox = this.commentBoxMap.get(commentBoxID);
    if (commentBox != null) {
      commentBox.comments.push(comment);
      this.commentBoxAddCommentSubject.next({ addedComment: comment, commentBox: commentBox });
    }
  }

  /**
   * Deletes the operator from the graph by its ID.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  public deleteOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    this.operatorIDMap.delete(operatorID);
    this.operatorDeleteSubject.next({ deletedOperator: operator });
  }

  public deleteCommentBox(commentBoxID: string): void {
    const commentBox = this.getCommentBox(commentBoxID);
    if (!commentBox) {
      throw new Error(`CommentBox with ID ${commentBoxID} does not exist`);
    }
    this.commentBoxMap.delete(commentBoxID);
    this.commentBoxDeleteSubject.next({ deletedCommentBox: commentBox });
  }

  public disableOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (this.isOperatorDisabled(operatorID)) {
      return;
    }
    this.operatorIDMap.set(operatorID, { ...operator, isDisabled: true });
    this.disabledOperatorChangedSubject.next({
      newDisabled: [operatorID],
      newEnabled: [],
    });
  }

  public enableOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (!this.isOperatorDisabled(operatorID)) {
      return;
    }
    this.operatorIDMap.set(operatorID, { ...operator, isDisabled: false });
    this.disabledOperatorChangedSubject.next({
      newDisabled: [],
      newEnabled: [operatorID],
    });
  }

  public changeOperatorDisplayName(operatorID: string, newDisplayName: string): void {
    const operator = this.getOperator(operatorID);
    if (operator.customDisplayName === newDisplayName) {
      return;
    }
    this.operatorIDMap.set(operatorID, {
      ...operator,
      customDisplayName: newDisplayName,
    });
    this.operatorDisplayNameChangedSubject.next({ operatorID, newDisplayName });
  }

  public isOperatorDisabled(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.isDisabled ?? false;
  }

  public getDisabledOperators(): ReadonlySet<string> {
    return new Set(Array.from(this.operatorIDMap.keys()).filter(op => this.isOperatorDisabled(op)));
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
    this.operatorIDMap.set(operatorID, { ...operator, isCached: true });
    this.cachedOperatorChangedSubject.next({
      newCached: [operatorID],
      newUnCached: [],
    });
  }

  public unCacheOperator(operatorID: string): void {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    if (!this.isOperatorCached(operatorID)) {
      return;
    }
    this.operatorIDMap.set(operatorID, { ...operator, isCached: false });
    this.cachedOperatorChangedSubject.next({
      newCached: [],
      newUnCached: [operatorID],
    });
  }

  public isOperatorCached(operatorID: string): boolean {
    const operator = this.getOperator(operatorID);
    if (!operator) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    return operator.isCached ?? false;
  }

  public getCachedOperators(): ReadonlySet<string> {
    return new Set(Array.from(this.operatorIDMap.keys()).filter(op => this.isOperatorCached(op)));
  }

  /**
   * Returns whether the operator exists in the graph.
   * @param operatorID operator ID
   */
  public hasOperator(operatorID: string): boolean {
    return this.operatorIDMap.has(operatorID);
  }

  public hasCommentBox(commentBoxId: string): boolean {
    return this.commentBoxMap.has(commentBoxId);
  }

  /**
   * Gets the operator with the operatorID.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  public getOperator(operatorID: string): OperatorPredicate {
    const operator = this.operatorIDMap.get(operatorID);
    if (!operator) {
      throw new Error(`operator ${operatorID} does not exist`);
    }
    return operator;
  }

  public getCommentBox(commentBoxID: string): CommentBox {
    const commentBox = this.commentBoxMap.get(commentBoxID);
    if (!commentBox) {
      throw new Error(`commentBox ${commentBoxID} does not exist`);
    }
    return commentBox;
  }

  /**
   * Returns an array of all operators in the graph
   */
  public getAllOperators(): OperatorPredicate[] {
    return Array.from(this.operatorIDMap.values());
  }

  public getAllEnabledOperators(): ReadonlyArray<OperatorPredicate> {
    return Array.from(this.operatorIDMap.values()).filter(op => !this.isOperatorDisabled(op.operatorID));
  }

  public getAllCommentBoxes(): CommentBox[] {
    return Array.from(this.commentBoxMap.values());
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
    this.operatorLinkMap.set(link.linkID, link);
    this.linkAddSubject.next(link);
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
    this.operatorLinkMap.delete(linkID);
    this.linkDeleteSubject.next({ deletedLink: link });
    // delete its breakpoint
    this.linkBreakpointMap.delete(linkID);
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
    this.operatorLinkMap.delete(link.linkID);
    this.linkDeleteSubject.next({ deletedLink: link });
    // delete its breakpoint
    this.linkBreakpointMap.delete(link.linkID);
  }

  /**
   * Returns whether the graph contains the link with the linkID
   * @param linkID link ID
   */
  public hasLinkWithID(linkID: string): boolean {
    return this.operatorLinkMap.has(linkID);
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
    const link = this.operatorLinkMap.get(linkID);
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
    return Array.from(this.operatorLinkMap.values());
  }

  public getAllEnabledLinks(): ReadonlyArray<OperatorLink> {
    return Array.from(this.operatorLinkMap.values()).filter(link => this.isLinkEnabled(link.linkID));
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
    const originalOperatorData = this.operatorIDMap.get(operatorID);
    if (originalOperatorData === undefined) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    const oldProperty = originalOperatorData.operatorProperties;

    // constructor a new copy with new operatorProperty and all other original attributes
    const operator = {
      ...originalOperatorData,
      operatorProperties: newProperty,
    };
    // set the new copy back to the operator ID map
    this.operatorIDMap.set(operatorID, operator);

    this.operatorPropertyChangeSubject.next({ oldProperty, operator });
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
    const oldBreakpoint = this.linkBreakpointMap.get(linkID);
    if (breakpoint === undefined || Object.keys(breakpoint).length === 0) {
      this.linkBreakpointMap.delete(linkID);
    } else {
      this.linkBreakpointMap.set(linkID, breakpoint);
    }
    this.breakpointChangeStream.next({ oldBreakpoint, linkID });
  }

  /**
   * get the breakpoint property of a link
   * returns an empty object if the link has no property
   *
   * @param linkID
   */
  public getLinkBreakpoint(linkID: string): Breakpoint | undefined {
    return this.linkBreakpointMap.get(linkID);
  }

  public getAllLinkBreakpoints(): ReadonlyMap<string, Breakpoint> {
    return this.linkBreakpointMap;
  }

  public getAllEnabledLinkBreakpoints(): ReadonlyMap<string, Breakpoint> {
    const enabledBreakpoints = new Map();
    this.linkBreakpointMap.forEach((breakpoint, linkID) => {
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
    deletedOperator: OperatorPredicate;
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
    oldProperty: object;
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
