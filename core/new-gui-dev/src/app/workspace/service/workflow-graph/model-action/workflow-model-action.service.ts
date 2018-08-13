import { Injectable } from '@angular/core';
import { WorkflowJointGraphService } from '../model/workflow-joint-graph.service';
import { OperatorUIElementService } from '../../operator-ui-element/operator-ui-element.service';
import { WorkflowSyncModelService } from '../model/workflow-sync-model.service';

@Injectable()
export class WorkflowModelActionService {

  constructor(
    private workflowJointGraphService: WorkflowJointGraphService,
    private workflowSyncModelService: WorkflowSyncModelService,
    private operatorUIElementService: OperatorUIElementService
  ) { }

  public addOperator(operator: OperatorPredicate, xOffset: number, yOffset: number): void {
    // get operaotr UI element
    const operatorUIElement = this.operatorUIElementService.getOperatorUIElement(
      operator.operatorType, operator.operatorID);
    // change its position
    operatorUIElement.position(
      xOffset - this.workflowJointGraphService.uiPaper.pageOffset().x,
      yOffset - this.workflowJointGraphService.uiPaper.pageOffset().y);

    // add the operator UI element to the UI model
    this.workflowJointGraphService.uiGraph.addCell(operatorUIElement);

    // notify the sync model service about addOperator
    this.workflowSyncModelService._handleAddOperator(operator);

  }

  public deleteOperator(operatorID: string): void {
    // get the cell from jointJS graph by ID and then remove it
    this.workflowJointGraphService.uiGraph.getCell(operatorID).remove();
  }

  public changeOperatorProperty(operatorID: string, newProperty: Object): void {
    this.workflowSyncModelService._handleChangeOperatorProperty(operatorID, newProperty);
  }

}
