import { Component, Input } from "@angular/core";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { Workflow } from "../../../../../common/type/workflow";

/**
 * NgbdModalDeleteProjectComponent is the pop-up component
 * for undoing the delete. User may cancel a project deletion.
 *
 * @author Zhaomin Li
 */
@Component({
  selector: "texera-resource-section-delete-project-modal",
  templateUrl: "./ngbd-modal-delete-workflow.component.html",
  styleUrls: ["./ngbd-modal-delete-workflow.component.scss", "../../../dashboard.component.scss"],
})
export class NgbdModalDeleteWorkflowComponent {
  @Input() workflow!: Workflow;

  constructor(public activeModal: NgbActiveModal) {}

  /**
   * deleteSavedWorkflow sends the user
   * confirm to the main component. It does not call any method in service.
   */
  public deleteSavedWorkflow(): void {
    this.activeModal.close(true);
  }
}
