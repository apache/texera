import { Subject } from 'rxjs/Subject';
import { Observable } from 'rxjs/Observable';
import { OperatorPredicate, OperatorLink, OperatorPort } from '../../../types/workflow-common.interface';
import { isEqual } from 'lodash-es';

// define the restricted methods that could change the graph
type restrictedMethods =
  'addOperator' | 'deleteOperator' | 'addLink' | 'deleteLink' | 'deleteLinkWithID' | 'setOperatorProperty';

// define a type Omit that creates a type with certain methods/properties omitted from it
type Omit<T, K extends keyof T> = Pick<T, Exclude<keyof T, K>>;

/**
 * WorkflowGraphReadonly is a type that only contains the readonly methods of WorkflowGraph.
 *
 * Methods that could alter the graph: add/delete operator or link, set operator property
 *  are omitted from this type.
 */
export type WorkflowGraphReadonly = Omit<WorkflowGraph, restrictedMethods>;

/**
 * WorkflowGraph represents the Texera's logical WorkflowGraph,
 *  it's a graph consisted of operators <OperatorPredicate> and links <OpreatorLink>,
 *  each operator and link has its own unique ID.
 *
 */
export class WorkflowGraph {

  private readonly operatorIDMap = new Map<string, OperatorPredicate>();
  private readonly operatorLinkMap = new Map<string, OperatorLink>();

  private readonly operatorAddSubject = new Subject<OperatorPredicate>();
  private readonly operatorDeleteSubject = new Subject<{ deletedOperator: OperatorPredicate }>();
  private readonly linkAddSubject = new Subject<OperatorLink>();
  private readonly linkDeleteSubject = new Subject<{ deletedLink: OperatorLink }>();
  private readonly operatorPropertyChangeSubject = new Subject<{ oldProperty: object, operator: OperatorPredicate }>();

  constructor(
    operatorPredicates: OperatorPredicate[] = [],
    operatorLinks: OperatorLink[] = []
  ) {
    operatorPredicates.forEach(op => this.operatorIDMap.set(op.operatorID, op));
    operatorLinks.forEach(link => this.operatorLinkMap.set(link.linkID, link));
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

  /**
   * Returns whether the operator exists in the graph.
   * @param operatorID operator ID
   */
  public hasOperator(operatorID: string): boolean {
    return this.operatorIDMap.has(operatorID);
  }

  /**
   * Gets the operator with the operatorID.
   * Throws an Error if the operator doesn't exist.
   * @param operatorID operator ID
   */
  public getOperator(operatorID: string): OperatorPredicate | undefined {
    return this.operatorIDMap.get(operatorID);
  }

  /**
   * Returns an array of all operators in the graph
   */
  public getAllOperators(): OperatorPredicate[] {
    return Array.from(this.operatorIDMap.values());
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
    const link = this.getLink(source, target);
    if (link === undefined) {
      return false;
    }
    return true;
  }

  /**
   * Returns a link with the linkID from operatorLinkMap.
   * Returns undefined if the link doesn't exist.
   * @param linkID link ID
   */
  public getLinkWithID(linkID: string): OperatorLink | undefined {
    return this.operatorLinkMap.get(linkID);
  }

  /**
   * Returns a link with the source and target from operatorLinkMap.
   * Returns undefined if the link doesn't exist.
   * @param source source operator and port of the link
   * @param target target operator and port of the link
   */
  public getLink(source: OperatorPort, target: OperatorPort): OperatorLink | undefined {
    const links = this.getAllLinks().filter(
      value => isEqual(value.source, source) && isEqual(value.target, target)
    );
    if (links.length === 0) {
      return undefined;
    }
    if (links.length > 1) {
      throw new Error(`WorkflowGraph inconsistency: find duplicate links with same source and target`);
    }
    return links[0];
  }

  /**
   * Returns an array of all the links in the graph.
   */
  public getAllLinks(): OperatorLink[] {
    return Array.from(this.operatorLinkMap.values());
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
   * Gets the observable event stream of an operator being added into the graph.
   */
  public getOperatorAddStream(): Observable<OperatorPredicate> {
    return this.operatorAddSubject.asObservable();
  }

  /**
 * Gets the observable event stream of an operator being deleted from the graph.
 * The observable value is the deleted operator.
 */
  public getOperatorDeleteStream(): Observable<{ deletedOperator: OperatorPredicate }> {
    return this.operatorDeleteSubject.asObservable();
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
   * Gets the observable event stream of a link being deleted from the graph.
   * The observable value includes the old property that is replaced, and the operator with new property.
   */
  public getOperatorPropertyChangeStream(): Observable<{ oldProperty: object, operator: OperatorPredicate }> {
    return this.operatorPropertyChangeSubject.asObservable();
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

    if (sourceOperator.outputPorts.find(
      (port) => port === link.source.portID) === undefined) {
      throw new Error(`link's source port ${link.source.portID} doesn't exist
          on output ports of the source operator ${link.source.operatorID}`);
    }
    if (targetOperator.inputPorts.find(
      (port) => port === link.target.portID) === undefined) {
      throw new Error(`link's target port ${link.target.portID} doesn't exist
          on input ports of the target operator ${link.target.operatorID}`);
    }
  }

}
