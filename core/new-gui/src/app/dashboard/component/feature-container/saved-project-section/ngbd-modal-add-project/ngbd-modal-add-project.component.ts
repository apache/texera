import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

/**
 * NgbdModalAddProjectComponent is the pop-up component
 * to let user create new project. User needs to specify
 * the project name.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: 'texera-add-project-section-modal',
  templateUrl: 'ngbd-modal-add-project.component.html',
  styleUrls: ['./ngbd-modal-add-project.component.scss', '../../../dashboard.component.scss']
})
export class NgbdModalAddProjectComponent {
  @Output() newProject = new EventEmitter<string>();

  public name: string = '';

  constructor(public activeModal: NgbActiveModal) { }

  /**
  * addProject records the project information and return
  * it to the main component. It does not call any method in service.
  *
  * @param
  */
  public addProject(): void {
    if (this.name !== '') {
      this.newProject.emit(this.name);
      this.name = '';
    }
    this.activeModal.close();
  }
}
