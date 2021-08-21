import { Component, Input } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";

/**
 * ResultDownloadComponent is the popup when the download finish
 */
@Component({
  selector: 'texera-result-download',
  templateUrl: './result-download.component.html',
  styleUrls: ['./result-download.component.scss']
})
export class ResultDownloadComponent {
  @Input() message: string | undefined;
  @Input() downloadType: String | undefined;
  @Input() link: string | undefined;
  @Input() workflowName: string | undefined;

  constructor(
    public activeModal: NgbActiveModal,
    private executeWorkflowService: ExecuteWorkflowService,
    private workflowActionService: WorkflowActionService
  ) {

  }

  /**
   * open the link in the new tab
   * @param href the link
   */
  private openInNewTab() {
    Object.assign(document.createElement('a'), {
      target: '_blank',
      href: this.link,
    }).click();
  }

}
