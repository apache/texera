import { Injectable } from '@angular/core';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { Observable } from '../../../../../node_modules/rxjs';
import { OperatorLink, OperatorPredicate, Point } from '../../types/workflow-common.interface';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';


export interface SavedWorkflow {
  operators: OperatorPredicate[];
  operatorPositions: {[key: string]: Point | undefined};
  links: OperatorLink[];
}

@Injectable({
  providedIn: 'root'
})
export class SaveWorkflowService {

  private static localStorageKey: string = 'workflow';

  constructor(
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.handleAutoSaveWorkFlow();
    this.operatorMetadataService.getOperatorMetadata().subscribe(metadata => {
      if (metadata.operators.length !== 0) {
        this.loadWorkflow();
      }
    });
  }

  public loadWorkflow(): void {
    this.workflowActionService.getTexeraGraph().getAllOperators().forEach(op => {
      this.workflowActionService.deleteOperator(op.operatorID);
    });

    const savedWorkflowJson = sessionStorage.getItem(SaveWorkflowService.localStorageKey);
    if (! savedWorkflowJson) {
      return;
    }

    const savedWorkflow: SavedWorkflow = JSON.parse(savedWorkflowJson);

    savedWorkflow.operators.forEach(op => {
      const opPosition = savedWorkflow.operatorPositions[op.operatorID];
      if (! opPosition) {
        throw new Error('position error');
      }
      this.workflowActionService.addOperator(op, opPosition);
    });

    savedWorkflow.links.forEach(link => {
      this.workflowActionService.addLink(link);
    });
  }


  private handleAutoSaveWorkFlow(): void {
    Observable.merge(
      this.workflowActionService.getTexeraGraph().getOperatorAddStream(),
      this.workflowActionService.getTexeraGraph().getOperatorDeleteStream(),
      this.workflowActionService.getTexeraGraph().getLinkAddStream(),
      this.workflowActionService.getTexeraGraph().getLinkDeleteStream(),
      this.workflowActionService.getTexeraGraph().getOperatorPropertyChangeStream(),
      this.workflowActionService.getJointGraphWrapper().getOperatorPositionChangeEvent()
    ).debounceTime(100).subscribe(() => {
      const workflow = this.workflowActionService.getTexeraGraph();

      const operators = workflow.getAllOperators();
      const links = workflow.getAllLinks();
      const operatorPositions: {[key: string]: Point} = {};
      workflow.getAllOperators().forEach(op => operatorPositions[op.operatorID] =
        this.workflowActionService.getJointGraphWrapper().getOperatorPosition(op.operatorID));
      const savedWorkflow: SavedWorkflow = {
        operators, operatorPositions, links
      };

      sessionStorage.setItem(SaveWorkflowService.localStorageKey, JSON.stringify(savedWorkflow));
    });
  }




}
