import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs/Rx';
import '../../../../common/rxjs-operators.ts';
import { WorkflowViewEventService } from '../view-event/workflow-view-event.service';
import { OperatorDragDropService } from '../../operator-drag-drop/operator-drag-drop.service';
import { WorkflowModelActionService } from '../model-action/workflow-model-action.service';
import { WorkflowGraphUtilsService } from '../utils/workflow-graph-utils.service';

@Injectable()
export class WorkflowViewObserverService {

  constructor(
    private workflowGraphUtilsService: WorkflowGraphUtilsService,
    private workflowViewEventService: WorkflowViewEventService,
    private operatorDragDropSerivce: OperatorDragDropService,
    private workflowModelActionService: WorkflowModelActionService
  ) {
    // handle operator is dropped
    this.operatorDragDropSerivce.operatorDroppedInEditor.subscribe(
      (value) => this.handleOperatorDropped(value.operator, value.offset.x, value.offset.y));

    // handle delete operator is clicked
    this.workflowViewEventService.deleteOperatorClickedInEditor.subscribe(
      value => this.handleDeleteOperatorClicked(value));
  }

  private handleOperatorDropped(operator: OperatorPredicate, xOffset: number, yOffset: number) {
    this.workflowModelActionService.addOperator(operator, xOffset, yOffset);
  }

  private handleDeleteOperatorClicked(data: {operatorID: string}): void {
    this.workflowModelActionService.deleteOperator(data.operatorID);
  }

}
