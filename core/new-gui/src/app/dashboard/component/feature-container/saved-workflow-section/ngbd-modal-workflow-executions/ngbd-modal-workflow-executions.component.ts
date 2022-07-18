import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbModal, NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { from } from "rxjs";
import { Workflow } from "../../../../../common/type/workflow";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../../service/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../../../../workspace/types/execute-workflow.interface";
import { DeletePromptComponent } from "../../../delete-prompt/delete-prompt.component";
import Fuse from "fuse.js";

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

  public executionsTableHeaders: string[] = [
    "",
    "",
    "Execution#",
    "Username",
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
    Username: "The User Who Runs This Execution",
    "Starting Time": "Starting Time of Workflow Execution",
    "Last Status Updated Time": "Latest Status Updated Time of Workflow Execution",
    Status: "Current Status of Workflow Execution",
  };

  public executionEntries: ReadonlyArray<WorkflowExecutionsEntry> = [];
  public allExecutionEntries: WorkflowExecutionsEntry[] = [];
  public filteredExecutionNames: Array<string> = [];
  public executionSearchValue: string = "";
  public searchCriteria: string[] = ["owner", "id"];
  public fuse = new Fuse([] as ReadonlyArray<WorkflowExecutionsEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["execution.wid", "execution.name", "ownerName"],
  });
  public searchCriteriaPathMapping: Map<string, string[]> = new Map([
    ["executionName", ["workflow", "name"]],
    ["id", ["workflow", "wid"]],
    ["owner", ["ownerName"]],
  ]);

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

  /* delete a single execution */

  onDelete(row: WorkflowExecutionsEntry) {
    const modalRef = this.modalService.open(DeletePromptComponent);
    modalRef.componentInstance.deletionType = "execution";
    modalRef.componentInstance.deletionName = row.name;

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

  /* rename a single execution */

  confirmUpdateWorkflowExecutionsCustomName(row: WorkflowExecutionsEntry, name: string, index: number): void {
    if (this.workflow.wid === undefined) {
      return;
    }
    // if name doesn't change, no need to call API
    if (name === row.name) {
      this.workflowExecutionsIsEditingName = this.workflowExecutionsIsEditingName.filter(
        entryIsEditingIndex => entryIsEditingIndex != index
      );
      return;
    }

    this.workflowExecutionsService
      .updateWorkflowExecutionsName(this.workflow.wid, row.eId, name)
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

  public searchInputOnChange(value: string): void {
    // enable autocomplete only when searching for execution name
    if (!value.includes(":")) {
      const filteredExecutionNames: string[] = [];
      this.allExecutionEntries.forEach(executionEntry => {
        const executionName = executionEntry.name;
        if (executionName.toLowerCase().indexOf(value.toLowerCase()) !== -1) {
          filteredExecutionNames.push(executionName);
        }
      });
      this.filteredExecutionNames = filteredExecutionNames;
    }
  }

  // check https://fusejs.io/api/query.html#logical-query-operators for logical query operators rule
  public buildAndPathQuery(
    executionSearchField: string,
    executionSearchValue: string
  ): {
    $path: ReadonlyArray<string>;
    $val: string;
  } {
    return {
      $path: this.searchCriteriaPathMapping.get(executionSearchField) as ReadonlyArray<string>,
      $val: executionSearchValue,
    };
  }

  // /**
  //  * Search workflows by owner name, workflow name or workflow id
  //  * Use fuse.js https://fusejs.io/ as the tool for searching
  //  */
  //  public searchWorkflow(): void {
  //   let andPathQuery: Object[] = [];
  //   // empty search value, return all workflow entries
  //   if (this.executionSearchValue.trim() === "") {
  //     this.executionEntries = [...this.allExecutionEntries];
  //     return;
  //   } else if (!this.executionSearchValue.includes(":")) {
  //     // search only by workflow name
  //     andPathQuery.push(this.buildAndPathQuery("executionName", this.executionSearchValue));
  //     this.executionEntries = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
  //     return;
  //   }
  //   const searchConsitionsSet = new Set(this.workflowSearchValue.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g));
  //   searchConsitionsSet.forEach(condition => {
  //     // field search
  //     if (condition.includes(":")) {
  //       const conditionArray = condition.split(":");
  //       if (conditionArray.length !== 2) {
  //         this.notificationService.error("Please check the format of the search query");
  //         return;
  //       }
  //       const workflowSearchField = conditionArray[0];
  //       const workflowSearchValue = conditionArray[1];
  //       if (!this.searchCriteria.includes(workflowSearchField)) {
  //         this.notificationService.error("Cannot search by " + workflowSearchField);
  //         return;
  //       }
  //       andPathQuery.push(this.buildAndPathQuery(workflowSearchField, workflowSearchValue));
  //     } else {
  //       //search by workflow name
  //       andPathQuery.push(this.buildAndPathQuery("workflowName", condition));
  //     }
  //   });
  //   this.dashboardWorkflowEntries = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
  // }
}
