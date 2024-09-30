import { Component, OnDestroy, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { AdminExecutionService } from "../../../service/admin/execution/admin-execution.service";
import { Execution } from "../../../../common/type/execution";
import { NzTableFilterFn, NzTableSortFn } from "ng-zorro-antd/table";
import { NzModalService } from "ng-zorro-antd/modal";
import { WorkflowExecutionHistoryComponent } from "../../user/user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { Workflow } from "../../../../common/type/workflow";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { environment } from "../../../../../environments/environment";

@UntilDestroy()
@Component({
  templateUrl: "./admin-execution.component.html",
})
export class AdminExecutionComponent implements OnInit, OnDestroy {
  Executions: ReadonlyArray<Execution> = [];
  workflowsCount: number = 0;
  listOfExecutions = [...this.Executions];
  workflows: Array<Workflow> = [];
  executionMap: Map<number, Execution> = new Map();

  // Set up an interval to periodically fetch and update execution data.
  // This interval function fetches the latest execution list and checks for updates.
  // If some execution's data has changed, it triggers a component refresh.
  // The interval runs every 1 second (1000 milliseconds).
  timer = setInterval(() => {
    this.adminExecutionService
      .getExecutionList()
      .pipe(untilDestroyed(this))
      .subscribe(executionList => {
        this.listOfExecutions.forEach((oldExecution, index) => {
          const updatedExecution = executionList.find(execution => execution.executionId === oldExecution.executionId);
          if (updatedExecution && this.dataCheck(this.listOfExecutions[index], updatedExecution)) {
            this.ngOnInit();
          } else if (!updatedExecution) {
            this.ngOnInit();

            // this if statement checks whether the workflow has no executions or the workflow has been deleted.
            let check_execution = this.executionMap.get(oldExecution.workflowId);
            if (check_execution && check_execution.executionId === oldExecution.executionId) {
              this.executionMap.delete(oldExecution.workflowId);
              this.listOfExecutions = [...this.executionMap.values()];
            }
          }
        });

        executionList.forEach(execution => {
          if (this.executionMap.has(execution.workflowId)) {
            let tempExecution = this.executionMap.get(execution.workflowId);
            if (tempExecution) {
              if (tempExecution.executionId < execution.executionId) {
                this.ngOnInit();
              }
            }
          } else if (!this.executionMap.has(execution.workflowId)) {
            this.ngOnInit();
          }
        });
        this.updateTimeDifferences();
      });
  }, 1000); // 1 second interval

  constructor(
    private adminExecutionService: AdminExecutionService,
    private modalService: NzModalService
  ) {}

  ngOnInit() {
    this.adminExecutionService
      .getExecutionList()
      .pipe(untilDestroyed(this))
      .subscribe(executionList => {
        this.Executions = executionList;
        this.listOfExecutions = [];
        this.reset();
        this.workflowsCount = this.listOfExecutions.length;
      });
  }

  ngOnDestroy(): void {
    clearInterval(this.timer);
  }

  maxStringLength(input: string, length: number): string {
    if (input.length > length) {
      return input.substring(0, length) + " . . . ";
    }
    return input;
  }

  dataCheck(oldExecution: Execution, newExecution: Execution): boolean {
    // Get the current time in seconds.
    const currentTime = Date.now() / 1000;
    // Check if the execution needed to be updated
    if (oldExecution.executionStatus === "JUST COMPLETED" && currentTime - newExecution.endTime / 1000 <= 5) {
      return false;
    } else if (oldExecution.executionStatus != newExecution.executionStatus) {
      return true;
    } else if (oldExecution.executionName != newExecution.executionName) {
      return true;
    } else if (oldExecution.workflowName != newExecution.workflowName) {
      return true;
    }
    return false;
  }

  initWorkflows() {
    for (let i = 0; i < this.listOfExecutions.length; i++) {
      const execution = this.listOfExecutions[i];
      let tempWorkflow: Workflow = {
        content: {
          operators: [],
          operatorPositions: {},
          links: [],
          groups: [],
          commentBoxes: [],
          settings: { dataTransferBatchSize: environment.defaultDataTransferBatchSize },
        },
        name: execution.workflowName,
        wid: execution.workflowId,
        description: "",
        creationTime: 0,
        lastModifiedTime: 0,
        isPublished: 0,
        readonly: false,
      };

      this.workflows.push(tempWorkflow);
    }
  }

  filterExecutions() {
    for (let i = 0; i < this.Executions.length; i++) {
      const execution = this.Executions[i];
      this.executionMap.set(execution.workflowId, execution);
    }
    this.listOfExecutions = [...this.executionMap.values()];
  }

  reset() {
    this.filterExecutions();
    this.initWorkflows();

    this.specifyCompletedStatus();
    this.updateTimeDifferences();
  }

  /**
   * Update the execution status of workflows in the list based on their completion time.
   * If a workflow was completed within the last 5 seconds, it is updated to "JUST COMPLETED."
   * If a workflow was completed more than 5 seconds, it is updated back to "COMPLETED."
   */
  specifyCompletedStatus() {
    const currentTime = Date.now() / 1000;
    this.listOfExecutions.forEach((workflow, index) => {
      if (workflow.executionStatus === "COMPLETED" && currentTime - workflow.endTime / 1000 <= 5) {
        this.listOfExecutions[index].executionStatus = "JUST COMPLETED";
      } else if (workflow.executionStatus === "JUST COMPLETED" && currentTime - workflow.endTime / 1000 > 5) {
        this.listOfExecutions[index].executionStatus = "COMPLETED";
      }
    });
  }

  /**
   * if status are "RUNNING", "READY", or "PAUSED", the time used would constantly increase.
   * if status are not list above, there would be a final time used.
   */
  calculateTime(LastUpdateTime: number, StartTime: number, executionStatus: string, name: string): number {
    if (executionStatus === "RUNNING" || executionStatus === "READY" || executionStatus === "PAUSED") {
      const currentTime = Date.now() / 1000; // Convert to seconds
      const currentTimeInteger = Math.floor(currentTime);
      return currentTimeInteger - StartTime / 1000;
    } else {
      return (LastUpdateTime - StartTime) / 1000;
    }
  }

  /**
   * Update the execution time differences for each execution in the list of executions.
   * This function calculates and assigns the elapsed time for each execution.
   */
  updateTimeDifferences() {
    this.listOfExecutions.forEach(workflow => {
      workflow.executionTime = this.calculateTime(
        workflow.endTime,
        workflow.startTime,
        workflow.executionStatus,
        workflow.workflowName
      );
    });
  }

  /**
   * Determine and return the color associated with a given execution status.
   */
  getStatusColor(status: string): string {
    switch (status) {
      case "READY":
        return "lightgreen";
      case "RUNNING":
        return "orange";
      case "PAUSED":
        return "purple";
      case "FAILED":
        return "gray";
      case "JUST COMPLETED":
        return "blue";
      case "COMPLETED":
        return "green";
      case "KILLED":
        return "red";
      default:
        return "black";
    }
  }

  /**
   * Convert a numeric timestamp to a human-readable time string.
   */
  convertTimeToTimestamp(executionStatus: string, timeValue: number): string {
    const date = new Date(timeValue);
    return date.toLocaleString("en-US", { timeZoneName: "short" });
  }

  /**
   * Convert a total number of seconds into a formatted time string (HH:MM:SS).
   */
  convertSecondsToTime(seconds: number): string {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainingSeconds = seconds % 60;

    return `${this.padZero(hours)}:${this.padZero(minutes)}:${this.padZero(remainingSeconds)}`;
  }

  /**
   * Pad a number with a leading zero if it is a single digit.
   */
  padZero(value: number): string {
    return value.toString().padStart(2, "0");
  }

  filterByStatus: NzTableFilterFn<Execution> = function (list: string[], execution: Execution) {
    return list.some(function (executionStatus) {
      return execution.executionStatus.indexOf(executionStatus) !== -1;
    });
  };

  clickToViewHistory(workflowId: number) {
    let wid!: number;
    let name!: string;
    for (let i = 0; i < this.workflows.length; i++) {
      const workflow = this.workflows[i];
      if (workflow.wid == workflowId) {
        wid = workflow.wid;
        name = workflow.name;
        break;
      }
    }

    this.modalService.create({
      nzContent: WorkflowExecutionHistoryComponent,
      nzData: { wid: wid },
      nzTitle: "Execution results of Workflow: " + name,
      nzFooter: null,
      nzWidth: "80%",
      nzCentered: true,
    });
  }

  /**
   Due to the Async nature when setting up the websocket, the socket would be closed before the connection is established.
   Therefore, commenting the code to ensure the connections is established and request has been sent.
   */
  killExecution(wid: number) {
    let socket = new WorkflowWebsocketService();
    socket.openWebsocket(wid);
    socket.send("WorkflowKillRequest", {});
    // socket.closeWebsocket();
  }

  /**
   Due to the Async nature when setting up the websocket, the socket would be closed before the connection is established.
   Therefore, commenting the code to ensure the connections is established and request has been sent.
   */
  pauseExecution(wid: number) {
    let socket = new WorkflowWebsocketService();
    socket.openWebsocket(wid);
    socket.send("WorkflowPauseRequest", {});
    // socket.closeWebsocket();
  }

  /**
   Due to the Async nature when setting up the websocket, the socket would be closed before the connection is established.
   Therefore, commenting the code to ensure the connections is established and request has been sent.
   */
  resumeExecution(wid: number) {
    let socket = new WorkflowWebsocketService();
    socket.openWebsocket(wid);
    socket.send("WorkflowResumeRequest", {});
    // socket.closeWebsocket();
  }

  public sortByWorkflowName: NzTableSortFn<Execution> = (a: Execution, b: Execution) =>
    (b.workflowName || "").localeCompare(a.workflowName);
  public sortByExecutionName: NzTableSortFn<Execution> = (a: Execution, b: Execution) =>
    (b.executionName || "").localeCompare(a.executionName);
  public sortByCompletedTime: NzTableSortFn<Execution> = (a: Execution, b: Execution) => b.endTime - a.endTime;
  public sortByInitiator: NzTableSortFn<Execution> = (a: Execution, b: Execution) =>
    (b.userName || "").localeCompare(a.userName);
}
