import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { Workflow } from "../../../../../common/type/workflow";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../../service/workflow-executions/workflow-executions.service";
import { ChartType } from "../../../../../workspace/types/visualization.interface";
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

  public executionsTableHeaders: string[] = ["Execution#", "Starting Time", "Completion Time", "Status"];

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

  getExecutionStatus(statusCode: number): String {
    console.log("test");
    switch (statusCode) {
      case 0:
        return ExecutionState.Initializing.toString();
        break;
      case 1:
        return ExecutionState.Running.toString();
        break;
      case 2:
        return ExecutionState.Paused.toString();
        break;
      case 3:
        return ExecutionState.Completed.toString();
        break;
      case 4:
        return ExecutionState.Aborted.toString();
        break;
    }
    return "";
  }
}
