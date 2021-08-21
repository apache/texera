import { Injectable } from '@angular/core';
import { environment } from '../../../../environments/environment';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { Observable } from 'rxjs/Observable';
import { ResultDownloadResponse } from '../../types/workflow-websocket.interface';
import { NotificationService } from '../../../common/service/notification/notification.service';
import { ExecuteWorkflowService } from '../execute-workflow/execute-workflow.service';
import { ExecutionState } from '../../types/execute-workflow.interface';

@Injectable({
  providedIn: 'root'
})
export class WorkflowResultExportService {
  private hasResultToExport: boolean = false;

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService,
    private executeWorkflowService: ExecuteWorkflowService
  ) {
    this.workflowWebsocketService.subscribeToEvent('ResultDownloadResponse').subscribe((response: ResultDownloadResponse) => {
      this.notificationService.success(response.message);
    });

    Observable.merge(
      this.executeWorkflowService.getExecutionStateStream().filter(
        ({ previous, current }) => current.state === ExecutionState.Completed),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorHighlightStream(),
      this.workflowActionService.getJointGraphWrapper().getJointOperatorUnhighlightStream()
    )
      .subscribe(() => {

        this.hasResultToExport = this.executeWorkflowService.getExecutionState().state === ExecutionState.Completed &&
          this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().filter((operatorId) =>
            this.workflowActionService.getTexeraGraph().getOperator(operatorId).operatorType.toLowerCase().includes('sink')).length > 0;
      });
  }


  /**
   * export the workflow execution result according the download type
   */
  public exportWorkflowExecutionResult(downloadType: string, workflowName: string): void {
    if (this.isResultExportDisabled()) {
      return;
    }

    this.notificationService.loading('exporting...');
    this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs().filter((operatorId) =>
      this.workflowActionService.getTexeraGraph().getOperator(operatorId).operatorType.toLowerCase().includes('sink'))
      .forEach(operatorId => {
        this.workflowWebsocketService.send('ResultDownloadRequest', {
          downloadType: downloadType,
          workflowName: workflowName,
          operatorId: operatorId
        });
      });

  }

  public isResultExportDisabled(): boolean {
    return !environment.downloadExecutionResultEnabled || !this.hasResultToExport;
  }
}
