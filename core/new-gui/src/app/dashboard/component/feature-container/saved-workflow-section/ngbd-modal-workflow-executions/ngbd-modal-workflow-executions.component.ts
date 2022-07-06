import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbModal, NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { cloneDeep } from "lodash-es";
import { from } from "rxjs";
import { Workflow } from "../../../../../common/type/workflow";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../../service/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../../../../workspace/types/execute-workflow.interface";
import { NgbdModalDeleteWorkflowComponent } from "../ngbd-modal-delete-workflow/ngbd-modal-delete-workflow.component";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-workflow-executions",
  templateUrl: "./ngbd-modal-workflow-executions.component.html",
  styleUrls: ["./ngbd-modal-workflow-executions.component.scss"],
})
export class NgbdModalWorkflowExecutionsComponent implements OnInit {
  @Input() workflow!: Workflow;

  public workflowExecutionsList: WorkflowExecutionsEntry[] | undefined;

  public executionsTableHeaders: string[] = ["", "", "Execution#", "Starting Time", "Updated Time", "Status", ""];
  public currentlyHoveredExecution: WorkflowExecutionsEntry | undefined;

  constructor(
    public activeModal: NgbActiveModal,
    private workflowExecutionsService: WorkflowExecutionsService,
    private modalService: NgbModal
  ) {}

  ngOnInit(): void {
    // gets the workflow executions and display the runs in the table on the form
    this.displayWorkflowExecutions();
  }

  /**
   * calls the service to display the workflow executions on the table
   */
  displayWorkflowExecutions(): void {
    if (this.workflow.wid === undefined) {
      return;
    }
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(this.workflow.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowExecutions => {
        this.workflowExecutionsList = workflowExecutions;
      });
  }

  getExecutionStatus(statusCode: number): String {
    switch (statusCode) {
      case 0:
        return ExecutionState.Initializing.toString();
      case 1:
        return ExecutionState.Running.toString();
      case 2:
        return ExecutionState.Paused.toString();
      case 3:
        return ExecutionState.Completed.toString();
      case 4:
        return ExecutionState.Aborted.toString();
    }
    return "";
  }

  onBookmarkToggle(row: WorkflowExecutionsEntry) {
    if (this.workflow.wid === undefined) return;
    const wasPreviouslyBookmarked = row.bookmarked;

    // Update bookmark state locally.
    row.bookmarked = !wasPreviouslyBookmarked;

    // Update on the server.
    this.workflowExecutionsService
      .setIsBookmarked(this.workflow.wid, row.eId, !wasPreviouslyBookmarked)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (_: unknown) => (row.bookmarked = wasPreviouslyBookmarked),
      });
  }

  /* delete a single execution and display current workflow execution */

  onDelete(row: WorkflowExecutionsEntry) {
    // Confirmation prompt for deletion
    const modalRef = this.modalService.open(NgbdModalDeleteWorkflowComponent);
    modalRef.componentInstance.workflow = cloneDeep(row);

    from(modalRef.result)
      .pipe(untilDestroyed(this))
      .subscribe((confirmToDelete: boolean) => {
        if (confirmToDelete && this.workflow.wid !== undefined) {
          this.workflowExecutionsService
            .deleteWorkflowExecutions(this.workflow.wid, row.eId)
            .pipe(untilDestroyed(this))
            .subscribe({
              complete: () => this.workflowExecutionsList?.splice(this.workflowExecutionsList.indexOf(row), 1),
            });
        }
      });
  }
}
