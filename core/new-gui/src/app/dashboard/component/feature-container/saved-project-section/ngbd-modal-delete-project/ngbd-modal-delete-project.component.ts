import { Component, Input, ComponentRef, Optional, Inject } from '@angular/core';
import { SavedProject } from '../../../../type/saved-project';
import { ZorroModalDialogueComponent } from '../../../../../abstract-component/zorro-modal.component';

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
export class NgbdModalDeleteProjectComponent extends ZorroModalDialogueComponent<boolean> {
  project: SavedProject = {
    name: '',
    id: '',
    creationTime: '',
    lastModifiedTime: ''
  };
  constructor(@Optional() @Inject('ComponentRefPromise') ref: Promise<ComponentRef<NgbdModalDeleteProjectComponent>>) {
    super(ref);
  }

  /**
  * deleteSavedProject sends the user
  * confirm to the main component. It does not call any method in service.
  *
  * @param
  */
  public deleteSavedProject(): void {
    this.closeModal(true);
  }
}
