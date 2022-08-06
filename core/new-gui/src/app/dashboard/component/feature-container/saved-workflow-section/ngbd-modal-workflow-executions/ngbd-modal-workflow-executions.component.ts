import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbModal, NgbActiveModal } from "@ng-bootstrap/ng-bootstrap";
import { from } from "rxjs";
import { Workflow } from "../../../../../common/type/workflow";
import { WorkflowExecutionsEntry } from "../../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../../service/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../../../../workspace/types/execute-workflow.interface";
import { DeletePromptComponent } from "../../../delete-prompt/delete-prompt.component";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import Fuse from "fuse.js";

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-workflow-executions",
  templateUrl: "./ngbd-modal-workflow-executions.component.html",
  styleUrls: ["./ngbd-modal-workflow-executions.component.scss"],
})
export class NgbdModalWorkflowExecutionsComponent implements OnInit {
  @Input() workflow!: Workflow;
  @Input() workflowName!: string;

  public workflowExecutionsDisplayedList: WorkflowExecutionsEntry[] | undefined;
  public workflowExecutionsIsEditingName: number[] = [];
  public currentlyHoveredExecution: WorkflowExecutionsEntry | undefined;
  public executionsTableHeaders: string[] = [
    "",
    "",
    "Username",
    "Name",
    "Starting Time",
    "Last Status Updated Time",
    "Status",
    "",
  ];
  /*Tooltip for each header in execution table*/
  public executionTooltip: Record<string, string> = {
    Name: "Workflow Name",
    Username: "The User Who Runs This Execution",
    "Starting Time": "Starting Time of Workflow Execution",
    "Last Status Updated Time": "Latest Status Updated Time of Workflow Execution",
    Status: "Current Status of Workflow Execution",
  };

  /** variables related to executions filtering
   */
  public allExecutionEntries: WorkflowExecutionsEntry[] = [];
  public filteredExecutionInfo: Array<string> = [];
  public executionSearchValue: string = "";
  public searchCriteria: string[] = ["user", "status"];
  public fuse = new Fuse([] as ReadonlyArray<WorkflowExecutionsEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["name", "userName", "status"],
  });

  // Pagination attributes
  public isAscSort: boolean = true;
  public currentPageIndex: number = 1;
  public pageSize: number = 10;
  public pageSizeOptions: number[] = [5, 10, 20, 30, 40];
  public numOfExecutions: number = 0;
  public paginatedExecutionEntries: WorkflowExecutionsEntry[] = [];

  public searchCriteriaPathMapping: Map<string, string[]> = new Map([
    ["executionName", ["name"]],
    ["user", ["userName"]],
    ["status", ["status"]],
  ]);
  public statusMapping: Map<string, number> = new Map([
    ["initializing", 0],
    ["running", 1],
    ["paused", 2],
    ["completed", 3],
    ["aborted", 4],
  ]);

  constructor(
    public activeModal: NgbActiveModal,
    private workflowExecutionsService: WorkflowExecutionsService,
    private modalService: NgbModal,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    // gets the workflow executions and display the runs in the table on the form
    this.displayWorkflowExecutions();
  }

  /**
   * calls the service to display the workflow executions on the table
   */
  displayWorkflowExecutions(): void {
    if (this.workflow === undefined || this.workflow.wid === undefined) {
      return;
    }
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(this.workflow.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowExecutions => {
        this.allExecutionEntries = workflowExecutions;
        this.numOfExecutions = workflowExecutions.length;
        this.paginatedExecutionEntries = this.changePaginatedExecutions();
        this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
        this.fuse.setCollection(this.paginatedExecutionEntries);
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
              complete: () => {
                this.allExecutionEntries?.splice(this.allExecutionEntries.indexOf(row), 1);
                this.paginatedExecutionEntries?.splice(this.paginatedExecutionEntries.indexOf(row), 1);
                this.workflowExecutionsDisplayedList?.splice(this.workflowExecutionsDisplayedList.indexOf(row), 1);
                this.fuse.setCollection(this.paginatedExecutionEntries);
              },
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
        if (this.workflowExecutionsDisplayedList === undefined) {
          return;
        }
        // change the execution name globally
        this.allExecutionEntries[this.allExecutionEntries.indexOf(this.workflowExecutionsDisplayedList[index])].name =
          name;
        this.paginatedExecutionEntries[
          this.paginatedExecutionEntries.indexOf(this.workflowExecutionsDisplayedList[index])
        ].name = name;
        this.workflowExecutionsDisplayedList[index].name = name;
        this.fuse.setCollection(this.paginatedExecutionEntries);
      })
      .add(() => {
        this.workflowExecutionsIsEditingName = this.workflowExecutionsIsEditingName.filter(
          entryIsEditingIndex => entryIsEditingIndex != index
        );
      });
  }

  /* sort executions by name/username/start time/update time
   based in ascending alphabetical order */

  ascSort(type: string): void {
    if (type === "Name") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe1.name.toLowerCase().localeCompare(exe2.name.toLowerCase()));
    } else if (type === "Username") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe1.userName.toLowerCase().localeCompare(exe2.userName.toLowerCase()));
    } else if (type === "Starting Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.startingTime > exe2.startingTime ? 1 : exe2.startingTime > exe1.startingTime ? -1 : 0
        );
    } else if (type == "Last Status Updated Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.completionTime > exe2.completionTime ? 1 : exe2.completionTime > exe1.completionTime ? -1 : 0
        );
    }
  }

  /* sort executions by name/username/start time/update time
   based in descending alphabetical order */

  dscSort(type: string): void {
    if (type === "Name") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe2.name.toLowerCase().localeCompare(exe1.name.toLowerCase()));
    } else if (type === "Username") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) => exe2.userName.toLowerCase().localeCompare(exe1.userName.toLowerCase()));
    } else if (type === "Starting Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.startingTime < exe2.startingTime ? 1 : exe2.startingTime < exe1.startingTime ? -1 : 0
        );
    } else if (type == "Last Status Updated Time") {
      this.workflowExecutionsDisplayedList = this.workflowExecutionsDisplayedList
        ?.slice()
        .sort((exe1, exe2) =>
          exe1.completionTime < exe2.completionTime ? 1 : exe2.completionTime < exe1.completionTime ? -1 : 0
        );
    }
  }

  public searchInputOnChange(value: string): void {
    const searchConditionsSet = [...new Set(value.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g))];
    searchConditionsSet.forEach((condition, index) => {
      const preCondition = searchConditionsSet.slice(0, index);
      var executionSearchField = "";
      var executionSearchValue = "";
      if (condition.includes(":")) {
        const conditionArray = condition.split(":");
        executionSearchField = conditionArray[0];
        executionSearchValue = conditionArray[1];
      } else {
        executionSearchField = "executionName";
        executionSearchValue = preCondition
          ? value.slice(preCondition.map(c => c.length).reduce((a, b) => a + b, 0) + preCondition.length)
          : value;
      }
      const filteredExecutionInfo: string[] = [];
      this.paginatedExecutionEntries.forEach(executionEntry => {
        const searchField = this.searchCriteriaPathMapping.get(executionSearchField);
        var executionInfo = "";
        if (searchField === undefined) {
          return;
        } else {
          executionInfo =
            searchField[0] === "status"
              ? [...this.statusMapping.entries()]
                  .filter(({ 1: val }) => val === executionEntry.status)
                  .map(([key]) => key)[0]
              : Object.values(executionEntry)[Object.keys(executionEntry).indexOf(searchField[0])];
        }
        if (executionInfo.toLowerCase().indexOf(executionSearchValue.toLowerCase()) !== -1) {
          var filterQuery = "";
          if (preCondition.length !== 0) {
            filterQuery =
              executionSearchField === "executionName"
                ? preCondition.join(" ") + " " + executionInfo
                : preCondition.join(" ") + " " + executionSearchField + ":" + executionInfo;
          } else {
            filterQuery =
              executionSearchField === "executionName" ? executionInfo : executionSearchField + ":" + executionInfo;
          }
          filteredExecutionInfo.push(filterQuery);
        }
      });
      this.filteredExecutionInfo = [...new Set(filteredExecutionInfo)];
    });
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

  /**
   * Search executions by execution name, user name, or status
   * Use fuse.js https://fusejs.io/ as the tool for searching
   */
  public searchExecution(): void {
    // empty search value, return all execution entries
    if (this.executionSearchValue.trim() === "") {
      this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
      return;
    }
    let andPathQuery: Object[] = [];
    const searchConditionsSet = new Set(this.executionSearchValue.trim().split(/ +(?=(?:(?:[^"]*"){2})*[^"]*$)/g));
    searchConditionsSet.forEach(condition => {
      // field search
      if (condition.includes(":")) {
        const conditionArray = condition.split(":");
        if (conditionArray.length !== 2) {
          this.notificationService.error("Please check the format of the search query");
          return;
        }
        const executionSearchField = conditionArray[0];
        const executionSearchValue = conditionArray[1].toLowerCase();
        if (!this.searchCriteria.includes(executionSearchField)) {
          this.notificationService.error("Cannot search by " + executionSearchField);
          return;
        }
        if (executionSearchField === "status") {
          var statusSearchValue = this.statusMapping.get(executionSearchValue)?.toString();
          // check if user type correct status
          if (statusSearchValue === undefined) {
            this.notificationService.error("Status " + executionSearchValue + " is not available to execution");
            return;
          }
          andPathQuery.push(this.buildAndPathQuery(executionSearchField, statusSearchValue));
        } else {
          // handle all other searches
          andPathQuery.push(this.buildAndPathQuery(executionSearchField, executionSearchValue));
        }
      } else {
        //search by execution name
        andPathQuery.push(this.buildAndPathQuery("executionName", condition));
      }
    });
    this.workflowExecutionsDisplayedList = this.fuse.search({ $and: andPathQuery }).map(res => res.item);
  }

  /* Pagination handler */
  /* Assign new page index and change current list */
  onPageIndexChange(pageIndex: number): void {
    this.currentPageIndex = pageIndex;
    this.paginatedExecutionEntries = this.changePaginatedExecutions();
    this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
    this.fuse.setCollection(this.paginatedExecutionEntries);
  }

  /* Assign new page size and change current list */
  onPageSizeChange(pageSize: number): void {
    this.pageSize = pageSize;
    this.paginatedExecutionEntries = this.changePaginatedExecutions();
    this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
    this.fuse.setCollection(this.paginatedExecutionEntries);
  }

  /**
   * Change current page list everytime the page change
   */
  changePaginatedExecutions(): WorkflowExecutionsEntry[] {
    this.executionSearchValue = "";
    return this.allExecutionEntries?.slice(
      (this.currentPageIndex - 1) * this.pageSize,
      this.currentPageIndex * this.pageSize
    );
  }
}
