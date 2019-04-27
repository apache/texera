import { Component, Input, Output, EventEmitter } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import { SavedProject } from '../../../../type/saved-project';

/**
 * NgbdModalDeleteProjectComponent is the pop-up component
 * for undoing the delete. User may cancel a project deletion.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-resource-section-delete-project-modal',
  templateUrl: './ngbd-modal-delete-project.component.html',
  styleUrls: ['./ngbd-modal-delete-project.component.scss', '../../../dashboard.component.scss']
})
export class NgbdModalDeleteProjectComponent {
  defaultSavedProject: SavedProject = {
    name: '',
    id: '',
    creationTime: '',
    lastModifiedTime: ''
  };
  @Input() project: SavedProject = this.defaultSavedProject;
  @Output() deleteProject =  new EventEmitter<boolean>();

  constructor(public activeModal: NgbActiveModal) {
  }

  /**
  * deleteSavedProject sends the user
  * confirm to the main component. It does not call any method in service.
  *
  * @param
  */
  public deleteSavedProject(): void {
    this.activeModal.close(true);
  }

}
