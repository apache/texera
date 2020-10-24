import {Component, Input} from '@angular/core';
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';
import {SavedWorkflow} from '../../../../type/saved-workflow';

/**
 * NgbdModalDeleteProjectComponent is the pop-up component
 * for undoing the delete. User may cancel a project deletion.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-resource-section-delete-project-modal',
  templateUrl: './ngbd-modal-delete-workflow.component.html',
  styleUrls: ['./ngbd-modal-delete-workflow.component.scss', '../../../dashboard.component.scss']
})
export class NgbdModalDeleteWorkflowComponent {
  defaultSavedWorkflow: SavedWorkflow = {
    name: '',
    id: '',
    creationTime: '',
    lastModifiedTime: ''
  };
  @Input() workflow: SavedWorkflow = this.defaultSavedWorkflow;

  constructor(public activeModal: NgbActiveModal) {
  }

  /**
   * deleteSavedProject sends the user
   * confirm to the main component. It does not call any method in service.
   *
   * @param
   */
  public deleteSavedWorkflow(): void {
    this.activeModal.close(true);
  }

}
