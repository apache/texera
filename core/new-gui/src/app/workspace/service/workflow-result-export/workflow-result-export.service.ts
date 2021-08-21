import { Injectable } from '@angular/core';
import { environment } from '../../../../environments/environment';
import { WorkflowWebsocketService } from '../workflow-websocket/workflow-websocket.service';
import { WorkflowActionService } from '../workflow-graph/model/workflow-action.service';
import { Observable } from 'rxjs/Observable';
import { ResultDownloadResponse } from '../../types/workflow-websocket.interface';
import { Subject } from 'rxjs/Subject';
import { NotificationService } from '../../../common/service/notification/notification.service';

@Injectable({
  providedIn: 'root'
})
export class WorkflowResultExportService {
  private resultDownloadStream = new Subject<ResultDownloadResponse>();

  constructor(
    private workflowWebsocketService: WorkflowWebsocketService,
    private workflowActionService: WorkflowActionService,
    private notificationService: NotificationService
  ) {
    this.workflowWebsocketService.subscribeToEvent('ResultDownloadResponse').subscribe((response: ResultDownloadResponse) => {
      this.notificationService.info(response.message);
    });
  }

  public getResultDownloadStream(): Observable<ResultDownloadResponse> {
    return this.resultDownloadStream.asObservable();
  }

  /**
   * download the workflow execution result according the download type
   */
  public downloadWorkflowExecutionResult(downloadType: string, workflowName: string): void {
    if (!environment.downloadExecutionResultEnabled) {
      return;
    }
    const currentHighlightedOperatorIds = this.workflowActionService.getJointGraphWrapper().getCurrentHighlightedOperatorIDs();
    if (currentHighlightedOperatorIds.length !== 1) {
      return;
    }
    const resultOperatorId = currentHighlightedOperatorIds[0];
    this.workflowWebsocketService.send('ResultDownloadRequest', {
      downloadType: downloadType,
      workflowName: workflowName,
      operatorId: resultOperatorId
    });
  }
}
