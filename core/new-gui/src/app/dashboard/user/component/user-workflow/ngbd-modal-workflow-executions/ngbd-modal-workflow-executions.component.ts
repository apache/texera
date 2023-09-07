import { AfterViewInit, Component, Input, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbActiveModal, NgbModal } from "@ng-bootstrap/ng-bootstrap";
import * as c3 from "c3";
import { Workflow } from "../../../../../common/type/workflow";
import { WorkflowExecutionsEntry } from "../../../type/workflow-executions-entry";
import { WorkflowExecutionsService } from "../../../service/workflow-executions/workflow-executions.service";
import { ExecutionState } from "../../../../../workspace/types/execute-workflow.interface";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import Fuse from "fuse.js";
import { ChartType } from "src/app/workspace/types/visualization.interface";
import { ceil } from "lodash";

const MAX_TEXT_SIZE = 20;
const MAX_RGB = 255;
const MAX_USERNAME_SIZE = 5;

@UntilDestroy()
@Component({
  selector: "texera-ngbd-modal-workflow-executions",
  templateUrl: "./ngbd-modal-workflow-executions.component.html",
  styleUrls: ["./ngbd-modal-workflow-executions.component.scss"],
})
export class NgbdModalWorkflowExecutionsComponent implements OnInit, AfterViewInit {
  public static readonly USERNAME_PIE_CHART_ID = "#execution-userName-pie-chart";
  public static readonly STATUS_PIE_CHART_ID = "#execution-status-pie-chart";
  public static readonly PROCESS_TIME_BAR_CHART = "#execution-average-process-time-bar-chart";

  public static readonly WIDTH = 300;
  public static readonly HEIGHT = 300;

  public static readonly BARCHARTSIZE = 600;

  @Input() workflow!: Workflow;
  @Input() workflowName!: string;

  public workflowExecutionsDisplayedList: WorkflowExecutionsEntry[] | undefined;
  public workflowExecutionsIsEditingName: number[] = [];
  public currentlyHoveredExecution: WorkflowExecutionsEntry | undefined;
  public executionsTableHeaders: string[] = [
    "",
    "Username",
    "Name (ID)",
    "Starting Time",
    "Last Status Updated Time",
    "Status",
    "",
  ];
  /*Tooltip for each header in execution table*/
  public executionTooltip: Record<string, string> = {
    "Name (ID)": "Execution Name",
    Username: "The User Who Ran This Execution",
    "Starting Time": "Starting Time of Workflow Execution",
    "Last Status Updated Time": "Latest Status Updated Time of Workflow Execution",
    Status: "Current Status of Workflow Execution",
    "Group Bookmarking": "Mark or Unmark the Selected Entries",
    "Group Deletion": "Delete the Selected Entries",
  };

  /*custom column width*/
  public customColumnWidth: Record<string, string> = {
    "": "70px",
    "Name (ID)": "230px",
    "Workflow Version Sample": "220px",
    Username: "150px",
    "Starting Time": "250px",
    "Last Status Updated Time": "250px",
    Status: "80px",
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
    ["failed", 4],
    ["killed", 5],
  ]);
  public showORhide: boolean[] = [false, false, false, false];
  public avatarColors: { [key: string]: string } = {};
  public checked: boolean = false;
  public setOfEid = new Set<number>();
  public setOfExecution = new Set<WorkflowExecutionsEntry>();
  public averageProcessingTimeDivider: number = 10;

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

  ngAfterViewInit() {
    if (this.workflow === undefined || this.workflow.wid === undefined) {
      return;
    }
    this.workflowExecutionsService
      .retrieveWorkflowExecutions(this.workflow.wid)
      .pipe(untilDestroyed(this))
      .subscribe(workflowExecutions => {
        // generate charts data
        let userNameData: { [key: string]: [string, number] } = {};
        const statusMap: { [key: number]: string } = {
          0: "initializing",
          1: "running",
          2: "paused",
          3: "completed",
          4: "failed",
          5: "killed",
        };
        let statusData: { [key: string]: [string, number] } = {};

        workflowExecutions.forEach(execution => {
          if (userNameData[execution.userName] === undefined) {
            userNameData[execution.userName] = [execution.userName, 0];
          }
          userNameData[execution.userName][1] += 1;
          if (statusData[statusMap[execution.status]] === undefined) {
            statusData[statusMap[execution.status]] = [statusMap[execution.status], 0];
          }
          statusData[statusMap[execution.status]][1] += 1;
        });

        this.generatePieChart(
          Object.values(userNameData),
          ["user name"],
          "Users who ran the execution",
          NgbdModalWorkflowExecutionsComponent.USERNAME_PIE_CHART_ID
        );

        this.generatePieChart(
          Object.values(statusData),
          ["status"],
          "Executions status",
          NgbdModalWorkflowExecutionsComponent.STATUS_PIE_CHART_ID
        );
        // generate an average processing time bar chart
        const processTimeData: Array<[string, ...c3.PrimitiveArray]> = [["processing time"]];
        const processTimeCategory: string[] = [];
        Object.entries(this.getBarChartProcessTimeData(workflowExecutions)).forEach(([eId, processTime]) => {
          processTimeData[0].push(processTime);
          processTimeCategory.push(eId);
        });
        this.generateBarChart(
          processTimeData,
          processTimeCategory,
          "Execution Numbers",
          "Average Processing Time (m)",
          "Execution performance",
          NgbdModalWorkflowExecutionsComponent.PROCESS_TIME_BAR_CHART
        );
      });
  }

  generatePieChart(
    dataToDisplay: Array<[string, ...c3.PrimitiveArray]>,
    category: string[],
    title: string,
    chart: string
  ) {
    c3.generate({
      size: {
        height: NgbdModalWorkflowExecutionsComponent.HEIGHT,
        width: NgbdModalWorkflowExecutionsComponent.WIDTH,
      },
      data: {
        columns: dataToDisplay,
        type: ChartType.PIE,
      },
      axis: {
        x: {
          type: "category",
          categories: category,
        },
      },
      title: {
        text: title,
      },
      bindto: chart,
    });
  }

  generateBarChart(
    dataToDisplay: Array<[string, ...c3.PrimitiveArray]>,
    category: string[],
    x_label: string,
    y_label: string,
    title: string,
    chart: string
  ) {
    c3.generate({
      size: {
        height: NgbdModalWorkflowExecutionsComponent.BARCHARTSIZE,
        width: NgbdModalWorkflowExecutionsComponent.BARCHARTSIZE,
      },
      data: {
        columns: dataToDisplay,
        type: ChartType.BAR,
      },
      axis: {
        x: {
          label: {
            text: x_label,
            position: "outer-right",
          },
          type: "category",
          categories: category, // this categories contain the corresponding row eid
        },
        y: {
          label: {
            text: y_label,
            position: "outer-top",
          },
        },
      },
      title: {
        text: title,
      },
      bindto: chart,
    });
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
        return [ExecutionState.Failed.toString(), "exclamation-circle", "gray"];
      case 5:
        return [ExecutionState.Killed.toString(), "minus-circle", "red"];
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
      .groupSetIsBookmarked(this.workflow.wid, [row.eId], wasPreviouslyBookmarked)
      .pipe(untilDestroyed(this))
      .subscribe({
        error: (_: unknown) => (row.bookmarked = wasPreviouslyBookmarked),
      });
  }

  setBookmarked(): void {
    if (this.workflow.wid === undefined) return;
    if (this.setOfExecution !== undefined) {
      // isBookmarked: true if all the execution are bookmarked, false if there is one that is unbookmarked
      const isBookmarked = !Array.from(this.setOfExecution).some(execution => {
        return execution.bookmarked === null || !execution.bookmarked;
      });
      // update the bookmark locally
      this.setOfExecution.forEach(execution => {
        execution.bookmarked = !isBookmarked;
      });
      this.workflowExecutionsService
        .groupSetIsBookmarked(this.workflow.wid, Array.from(this.setOfEid), isBookmarked)
        .pipe(untilDestroyed(this))
        .subscribe({});
    }
  }

  /* delete a single execution */

  onDelete(row: WorkflowExecutionsEntry) {
    if (this.workflow.wid == undefined) {
      return;
    }
    this.workflowExecutionsService
      .groupDeleteWorkflowExecutions(this.workflow.wid, [row.eId])
      .pipe(untilDestroyed(this))
      .subscribe({
        complete: () => {
          this.allExecutionEntries?.splice(this.allExecutionEntries.indexOf(row), 1);
          this.paginatedExecutionEntries?.splice(this.paginatedExecutionEntries.indexOf(row), 1);
          this.fuse.setCollection(this.paginatedExecutionEntries);
        },
      });
  }

  onGroupDelete() {
    if (this.workflow.wid !== undefined) {
      this.workflowExecutionsService
        .groupDeleteWorkflowExecutions(this.workflow.wid, Array.from(this.setOfEid))
        .pipe(untilDestroyed(this))
        .subscribe({
          complete: () => {
            this.allExecutionEntries = this.allExecutionEntries?.filter(
              execution => !Array.from(this.setOfExecution).includes(execution)
            );
            this.paginatedExecutionEntries = this.paginatedExecutionEntries?.filter(
              execution => !Array.from(this.setOfExecution).includes(execution)
            );
            this.workflowExecutionsDisplayedList = this.paginatedExecutionEntries;
            this.fuse.setCollection(this.paginatedExecutionEntries);
            this.setOfEid.clear();
            this.setOfExecution.clear();
          },
        });
    }
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
    if (type === "Name (ID)") {
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
    if (type === "Name (ID)") {
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

  /**
   *
   * @param name
   * @param nameFlag true for execution name and false for username
   */
  abbreviate(name: string, nameFlag: boolean): string {
    let maxLength = nameFlag ? MAX_TEXT_SIZE : MAX_USERNAME_SIZE;
    if (name.length <= maxLength) {
      return name;
    } else {
      return name.slice(0, maxLength);
    }
  }

  onHit(column: string, index: number): void {
    if (this.showORhide[index]) {
      this.ascSort(column);
    } else {
      this.dscSort(column);
    }
    this.showORhide[index] = !this.showORhide[index];
  }

  setAvatarColor(userName: string): string {
    if (userName in this.avatarColors) {
      return this.avatarColors[userName];
    } else {
      this.avatarColors[userName] = this.getRandomColor();
      return this.avatarColors[userName];
    }
  }

  getRandomColor(): string {
    const r = Math.floor(Math.random() * MAX_RGB);
    const g = Math.floor(Math.random() * MAX_RGB);
    const b = Math.floor(Math.random() * MAX_RGB);
    return "rgba(" + r + "," + g + "," + b + ",0.8)";
  }

  /**
   * Update the eId set to keep track of the status of the checkbox
   * @param eId
   * @param checked true if checked false if unchecked
   */
  updateEidSet(eId: number, checked: boolean): void {
    if (checked) {
      this.setOfEid.add(eId);
    } else {
      this.setOfEid.delete(eId);
    }
  }

  /**
   * Update the row set to keep track of the status of the checkbox
   * @param row
   * @param checked true if checked false if unchecked
   */
  updateRowSet(row: WorkflowExecutionsEntry, checked: boolean): void {
    if (checked) {
      this.setOfExecution.add(row);
    } else {
      this.setOfExecution.delete(row);
    }
  }

  /**
   * Mark all the checkboxes checked and check the status of the all check
   * @param value true if we need to check all false if we need to uncheck all
   */
  onAllChecked(value: boolean): void {
    if (this.paginatedExecutionEntries !== undefined) {
      for (let execution of this.paginatedExecutionEntries) {
        this.updateEidSet(execution.eId, value);
        this.updateRowSet(execution, value);
      }
    }
    this.refreshCheckedStatus();
  }

  /**
   * Update the eId and row set, and check the status of the all check
   * @param row
   * @param checked true if checked false if unchecked
   */
  onItemChecked(row: WorkflowExecutionsEntry, checked: boolean) {
    this.updateEidSet(row.eId, checked);
    this.updateRowSet(row, checked);
    this.refreshCheckedStatus();
  }

  /**
   * Check the status of the all check
   */
  refreshCheckedStatus(): void {
    if (this.paginatedExecutionEntries !== undefined) {
      this.checked = this.paginatedExecutionEntries.length === this.setOfEid.size;
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
          let filterQuery: string;
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

  getBarChartProcessTimeData(rows: WorkflowExecutionsEntry[]) {
    let processTimeData: { [key: string]: number } = {};
    let divider: number = ceil(rows.length / this.averageProcessingTimeDivider);
    let tracker = 0;
    let totProcessTime = 0;
    let category = "";
    let eIdToNumber = 1;
    rows.forEach(execution => {
      tracker++;

      let processTime = execution.completionTime - execution.startingTime;
      processTime = processTime / 60000;
      totProcessTime += processTime;
      if (tracker === 1) {
        category += String(eIdToNumber);
      }
      if (tracker === divider) {
        category += "-" + String(eIdToNumber);
        processTimeData[category] = totProcessTime / divider;
        tracker = 0;
        totProcessTime = 0;
        category = "";
      }
      eIdToNumber++;
    });
    return processTimeData;
  }
}
