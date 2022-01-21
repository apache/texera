import { Component } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";

/**
 * NgbdModalAddProjectComponent is the pop-up component
 * to let user create new project. User needs to specify
 * the project name.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: "texera-add-workflow-section-modal",
  templateUrl: "ngbd-modal-add-workflow.component.html",
  styleUrls: ["../../../dashboard.component.scss", "ngbd-modal-add-workflow.component.scss"],
})
export class NgbdModalAddWorkflowComponent {
  public name: string = "";

  constructor(public activeModal: NgbActiveModal) {}

  /**
   * addWorkflow records the workflow information and return
   * it to the main component. It does not call any method in service.
   *
   * @param
   */
  public addWorkflow(): void {
    if (this.name !== "") {
      this.activeModal.close(this.name);
    } else {
      $("#warning").text("Please input the workflow name!");
    }
  }
}
