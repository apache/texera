import { Injectable } from '@angular/core';

import { Subject } from 'rxjs/Rx';

import { WorkflowGraph } from '../../../model/workflow-graph';


@Injectable()
export class WorkflowSyncModelService {

  public _texeraGraph = new WorkflowGraph([], []);

  public _operatorAddedSubject = new Subject<{operator: OperatorPredicate}>();

  public _operatorDeletedSubject = new Subject<{operator: OperatorPredicate}>();

  public _linkAddedSubject = new Subject<{link: OperatorLink}>();

  public _linkDeletedSubject = new Subject<{link: OperatorLink}>();

  public _linkChangedSubject = new Subject<{link: OperatorLink}>();

  public _operatorPropertyChangedSubject = new Subject<{operatorID: string, newProperty: Object}>();


  constructor() { }

  public _handleAddOperator(operator: OperatorPredicate): void {
    this._texeraGraph.addOperator(operator);
    this._operatorAddedSubject.next({operator});
  }

  public _handleDeleteOperator(operatorID): void {
    const operator = this._texeraGraph.deleteOperator(operatorID);
    this._operatorDeletedSubject.next({operator});
  }

  public _handleAddLink(link: OperatorLink): void {
    this._texeraGraph.addLink(link);
    this._linkAddedSubject.next({link});
  }

  public _handleDeleteLink(linkID: string): void {
    const link = this._texeraGraph.deleteLink(linkID);
    this._linkDeletedSubject.next({link});
  }

  public _handleChangeLink(link: OperatorLink): void {
    this._texeraGraph.changeLink(link);
    this._linkChangedSubject.next({link});
  }

  public _handleChangeOperatorProperty(operatorID: string, newProperty: Object): void {
    this._texeraGraph.changeOperatorProperty(operatorID, newProperty);

  }

}
