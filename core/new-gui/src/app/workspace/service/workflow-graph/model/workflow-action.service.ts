import { WorkflowGraph } from './../../../types/workflow-graph';
import { Observable } from 'rxjs/Observable';
import { Injectable } from '@angular/core';
import { JointModelService } from './joint-model.service';
import { TexeraModelService } from './texera-model.service';
import { Point, OperatorPredicate, OperatorLink } from '../../../types/common.interface';
import { Subject } from 'rxjs/Subject';

/**
 * WorkflowActionService exposes functions (actions) to modify the workflow graph,
 *  such as addOperator, deleteOperator, addLink, deleteLink, etc.
 * WorkflowActionService checks the validity of these actions,
 *  for example, check if adding two operators with the same ID.
 *
 * All changes(actions) to the workflow graph must go through WorkflowActionService,
 *  then WorkflowActionService will propagate these actions to the JointModelService and TexeraModelService,
 *  where the changes will be actually made.
 *
 * TexeraModelService exposes a read only version of the workflow graph, and provides
 *  the events related to workflow graph on a *logical* level.
 *
 * JointModelService is only used for handling the events only related to JointJS.
 *
 *
 *
 *
 */
@Injectable()
export class WorkflowActionService {

  private texeraGraph: WorkflowGraph = new WorkflowGraph();

  private addOperatorActionSubject: Subject<{ operator: OperatorPredicate, point: Point }> = new Subject();

  private deleteOperatorActionSubject: Subject<{ operatorID: string }> = new Subject();

  private addLinkActionSubject: Subject<{ link: OperatorLink }> = new Subject();

  private deleteLinkActionSubject: Subject<{ linkID: string }> = new Subject();

  constructor(
  ) { }

  /**
   * Adds an opreator to the workflow graph at a point.
   * @param operator
   * @param point
   */
  public addOperator(operator: OperatorPredicate, point: Point): void {
    WorkflowGraph.checkIsValidOperator(this.texeraGraph, operator);
    this.addOperatorActionSubject.next({ operator, point });
  }

  /**
   * Gets the event stream of the actions to add an operator.
   */
  _onAddOperatorAction(): Observable<{ operator: OperatorPredicate, point: Point }> {
    return this.addOperatorActionSubject.asObservable();
  }

  /**
   * Deletes an operator from the workflow graph
   * @param operatorID
   */
  public deleteOperator(operatorID: string): void {
    if (!this.texeraGraph.hasOperator(operatorID)) {
      throw new Error(`operator with ID ${operatorID} doesn't exist`);
    }
    this.deleteOperatorActionSubject.next({ operatorID });
  }

  /**
   * Gets the event stream of the actions to delete an operator
   */
  _onDeleteOperatorAction(): Observable<{ operatorID: string }> {
    return this.deleteOperatorActionSubject.asObservable();
  }

  /**
   * Adds a link to the workflow graph
   * @param link
   */
  public addLink(link: OperatorLink): void {
    WorkflowGraph.checkIsValidLink(this.texeraGraph, link);
    this.addLinkActionSubject.next({ link });
  }

  /**
   * Gets the event stream of the actions to add a link
   */
  _onAddLinkAction(): Observable<{ link: OperatorLink }> {
    return this.addLinkActionSubject.asObservable();
  }

  /**
   * Deletes a link from the workflow graph
   * @param linkID
   */
  public deleteLinkWithID(linkID: string): void {
    if (!this.texeraGraph.hasLinkWithID(linkID)) {
      throw new Error(`link with ID ${linkID} doesn't exist`);
    }
    this.deleteLinkActionSubject.next({ linkID });
  }

  /**
   * Gets the event stream of the actions to delete a link
   */
  _onDeleteLinkAction(): Observable<{ linkID: string }> {
    return this.deleteLinkActionSubject.asObservable();
  }

}
