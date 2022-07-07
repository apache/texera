import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { Workflow } from "../../../../../common/type/workflow";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../../service/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../../../../workspace/types/execute-workflow.interface";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-workflow-executions",
  templateUrl: "./ngbd-modal-workflow-executions.component.html",
  styleUrls: ["./ngbd-modal-workflow-executions.component.scss"],
})
export class NgbdModalWorkflowExecutionsComponent implements OnInit {
  @Input() workflow!: Workflow;

  public workflowExecutionsList: WorkflowExecutionsEntry[] | undefined;
  public workflowExecutionsIsEditingName: number[] = [];
  private defaultWorkflowExecutionsName: string = "Untitled Execution";

  public executionsTableHeaders: string[] = [
    "",
    "",
    "Execution#",
    "Name",
    "Starting Time",
    "Last Status Updated Time",
    "Status",
    "",
  ];

  /*Tooltip for each header in execution table*/
  public executionTooltip: Record<string, string> = {
    "Execution#": "Workflow Execution ID",
    Name: "Workflow Name",
    "Starting Time": "Stariting Time of Workflow Execution",
    "Last Status Updated Time": "Latest Status Updated Tme of Workflow Execution",
    Status: "Current Status of Workflow Execution",
  };

  public currentlyHoveredExecution: WorkflowExecutionsEntry | undefined;

  constructor(public activeModal: NgbActiveModal, private workflowExecutionsService: WorkflowExecutionsService) {}

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

  /**
   * display icons corresponding to workflow execution status
   *
   * NOTES: Colors match with new-gui/src/app/workspace/service/joint-ui/joint-ui.service.ts line 347
   * TODO: Move colors to a config file for changing them once for many files
   */
  getExecutionStatus(statusCode: number): string[] {
    switch (statusCode) {
      case 0:
        return [ExecutionState.Initializing.toString(), "sync", "#a6bd37"];
      case 1:
        return [ExecutionState.Running.toString(), "play-circle", "orange"];
      case 2:
        return [ExecutionState.Paused.toString(), "pause-circle", "magenta"];
      case 3:
        return [ExecutionState.Completed.toString(), "check-circle", "green"];
      case 4:
        return [ExecutionState.Aborted.toString(), "exclamation-circle", "gray"];
    }
    return ["", "question-circle", "gray"];
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
    if (this.workflow.wid === undefined) {
      return;
    }
    this.workflowExecutionsService
      .deleteWorkflowExecutions(this.workflow.wid, row.eId)
      .pipe(untilDestroyed(this))
      .subscribe({
        complete: () => this.workflowExecutionsList?.splice(this.workflowExecutionsList.indexOf(row), 1),
      });
  }

  confirmUpdateWorkflowExecutionsCustomName(row: WorkflowExecutionsEntry, name: string, index: number): void {
    if (this.workflow.wid === undefined) {
      return;
    }

    this.workflowExecutionsService
      .updateWorkflowExecutionsName(this.workflow.wid, row.eId, name || this.defaultWorkflowExecutionsName)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        if (this.workflowExecutionsList === undefined) {
          return;
        }
        this.workflowExecutionsList[index].name = name;
      })
      .add(() => {
        this.workflowExecutionsIsEditingName = this.workflowExecutionsIsEditingName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }
}
