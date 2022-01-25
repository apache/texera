import {Component, Input, OnInit} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { Workflow } from "../../../../../common/type/workflow";
import {WorkflowExecutionsEntry} from "../../../../type/workflow-executions-entry";
import {WorkflowExecutionsService} from "../../../../service/workflow-executions/workflow-executions.service";


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

  constructor(
    public activeModal: NgbActiveModal,
    private workflowExecutionsService: WorkflowExecutionsService
  ) {}

  ngOnInit(): void {
    // gets the workflow executions and display the runs in the table on the form
    this.displayWorkflowExecutions();
  }

  getExecution(eid: number) {
    // this.retrieveWorkflowByExecution(<number>this.workflow.wid, eid)
    //   .pipe(untilDestroyed(this))
    //   .subscribe(result => {
    //     console.log(result);
    //   });
  }

  /**
   * calls the service to display the workflow executions on the table
   */
  displayWorkflowExecutions(): void {
    if (this.workflow.wid === undefined) {
      return;
    }
    this.workflowExecutionsService.retrieveWorkflowExecutions(this.workflow.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowExecutions => {
        this.workflowExecutionsList = workflowExecutions;
      });
  }

  // /**
  //  * retrieves details of a particular execution of the workflow from backend database
  //  */
  // retrieveWorkflowByExecution(wid: number, eid: number): Observable<WorkflowExecutionsEntry> {
  //   return this.http
  //     .get<Workflow>(`${AppSettings.getApiEndpoint()}/${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}/${eid}`)
  //     .pipe(
  //     );
  // }
}
