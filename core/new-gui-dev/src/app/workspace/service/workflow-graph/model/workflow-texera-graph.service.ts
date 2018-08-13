import { Injectable } from '@angular/core';

import { WorkflowGraph } from '../../../model/workflow-graph';
import { WorkflowSyncModelService } from './workflow-sync-model.service';


@Injectable()
export class WorkflowTexeraGraphService {

  public texeraWorkflowGraph: WorkflowGraphReadonly = this.workflowSyncModelService._texeraGraph;


  public operatorAddedObservable = this.workflowSyncModelService._operatorAddedSubject.asObservable();

  public operatorDeletedObservable = this.workflowSyncModelService._operatorDeletedSubject.asObservable();

  public linkAddedObservable = this.workflowSyncModelService._linkAddedSubject.asObservable().distinctUntilChanged();

  public linkDeletedObservable = this.workflowSyncModelService._linkDeletedSubject.asObservable();

  public linkChangedObservable = this.workflowSyncModelService._linkChangedSubject.asObservable().distinctUntilChanged();

  public operatorPropertyChangedObservable = this.workflowSyncModelService._operatorPropertyChangedSubject
    .asObservable().distinctUntilChanged();

  constructor(
    private workflowSyncModelService: WorkflowSyncModelService
  ) {
  }

}
