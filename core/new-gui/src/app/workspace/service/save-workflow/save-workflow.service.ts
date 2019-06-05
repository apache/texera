import { Injectable } from '@angular/core';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { Observable } from '../../../../../node_modules/rxjs';
import { OperatorLink, OperatorPredicate, Point } from '../../types/workflow-common.interface';
import { OperatorMetadataService } from '../operator-metadata/operator-metadata.service';

/**
 * SavedWorkflow is used to store the information of the workflow
 *  1. all existing operators and their properties
 *  2. operator's position on the JointJS paper
 *  3. operator link predicates
 *
 * When the user refreshes the browser, the SaveWorkflow interface will be
 *  automatically saved and loaded once the refresh completes. This information
 *  will then be used to reload the entire workflow.
 *
 */
export interface SavedWorkflow {
  operators: OperatorPredicate[];
  operatorPositions: {[key: string]: Point | undefined};
  links: OperatorLink[];
}


/**
 * SaveWorkflowService is responsible for saving the existing workflow and
 *  reloading back to the JointJS paper when the browser refreshes.
 *
 * It will listens to all the browser action events to update the saved workflow plan.
 * These actions include:
 *  1. operator add
 *  2. operator delete
 *  3. link add
 *  4. link delete
 *  5. operator property change
 *  6. operator position change
 *
 * @author Simon Zhou
 */
@Injectable({
  providedIn: 'root'
})
export class SaveWorkflowService {

  private static readonly LOCAL_STORAGE_KEY: string = 'workflow';

  constructor(
    private workflowActionService: WorkflowActionService,
    private operatorMetadataService: OperatorMetadataService
  ) {
    this.handleAutoSaveWorkFlow();

    this.operatorMetadataService.getOperatorMetadata()
      .filter(metadata => metadata.operators.length !== 0)
      .subscribe(() => this.loadWorkflow());
  }

  /**
   * When the browser reloads, this method will be called to reload
   *  previously created workflow stored in the local storage onto
   *  the JointJS paper.
   */
  public loadWorkflow(): void {
    // remove the existing operators on the paper currently
    this.workflowActionService.getTexeraGraph().getAllOperators().forEach(op => {
      this.workflowActionService.deleteOperator(op.operatorID);
    });

    // get items in the storage
    const savedWorkflowJson = localStorage.getItem(SaveWorkflowService.LOCAL_STORAGE_KEY);
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

  /**
   * This method will listen to all the workflow change event happening
   *  on the property panel and the worfklow editor paper.
   */
  public handleAutoSaveWorkFlow(): void {
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

      localStorage.setItem(SaveWorkflowService.LOCAL_STORAGE_KEY, JSON.stringify(savedWorkflow));
    });
  }




}
