import { Component, OnInit} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { AdminExecutionService } from "../service/admin-execution.service";
import { Execution } from "../../../common/type/execution";
import { NzTableFilterFn, NzTableSortFn } from "ng-zorro-antd/table";

import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalWorkflowExecutionsComponent } from "../../user/component/user-workflow/ngbd-modal-workflow-executions/ngbd-modal-workflow-executions.component";
import {Workflow} from "../../../common/type/workflow";
import { WorkflowWebsocketService } from "src/app/workspace/service/workflow-websocket/workflow-websocket.service";


@UntilDestroy()
@Component({
  templateUrl: "./admin-dashboard.component.html",
  styleUrls: ["./admin-dashboard.component.scss"],
})
export class AdminDashboardComponent implements OnInit{

  Executions: ReadonlyArray<Execution> = [];
  workflowsCount: number = 0;
  usersCount: number = 0;
  listOfExecutions = [...this.Executions];
  workflows: Array<Workflow> = [];
  executionMap: Map<number, Execution> = new Map();

  intervals = setInterval(() => {
    this.adminExecutionService
      .getExecutionList()
      .pipe(untilDestroyed(this))
      .subscribe((executionList) => {
        this.listOfExecutions.forEach((oldExecution, index) =>
        {
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
        })
        this.updateTimeDifferences();
      });
  }, 1000); // 1 second interval

  constructor(private adminExecutionService: AdminExecutionService, private modalService: NgbModal,) {
  }

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

  maxStringLength(input: string, length:number):string {
    if (input.length > length) {
      return input.substring(0, length) + " . . . ";
    }
    return input;
  }

  dataCheck(oldExecution: Execution, newExecution: Execution): boolean {
    const currentTime = Date.now() / 1000;
    if (oldExecution.executionStatus === 'JUST COMPLETED' && currentTime - (newExecution.endTime/1000) <= 5){
      return false;
    }
    else if (oldExecution.executionStatus != newExecution.executionStatus) {
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
      let tempWorkflow: Workflow = {content:{ operators: [],
          operatorPositions: {},
          links: [],
          groups: [],
          breakpoints: {},
          commentBoxes: []},
        name: execution.workflowName,
        wid: execution.workflowId,
        description: "",
        creationTime: 0,
        lastModifiedTime: 0,
        readonly: false}

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

  specifyCompletedStatus() {
    const currentTime = Date.now() / 1000;
    this.listOfExecutions.forEach((workflow, index) => {
      if (workflow.executionStatus === 'COMPLETED' && (currentTime - (workflow.endTime / 1000)) <= 5) {
        this.listOfExecutions[index].executionStatus = 'JUST COMPLETED';
      }
      else if (workflow.executionStatus === 'JUST COMPLETED' && (currentTime - (workflow.endTime / 1000)) > 5){
        this.listOfExecutions[index].executionStatus = 'COMPLETED';
      }
    });
  }

  calculateTime(LastUpdateTime: number, StartTime: number, executionStatus: string, name: string): number {
    if (executionStatus === "RUNNING" || executionStatus === "READY" || executionStatus === "PAUSED") {
      const currentTime = Date.now() / 1000; // Convert to seconds
      const currentTimeInteger = Math.floor(currentTime);
      return currentTimeInteger - (StartTime/1000);
    } else {
      return (LastUpdateTime - StartTime)/1000;
    }
  }

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

  getStatusColor(status: string): string {
    switch (status) {
      case 'READY':
        return 'lightgreen';
      case 'RUNNING':
        return 'orange';
      case 'PAUSED':
        return 'purple';
      case 'FAILED':
        return 'gray';
      case 'JUST COMPLETED':
        return 'blue';
      case 'COMPLETED':
        return 'green';
      case 'KILLED':
        return 'red';
      default:
        return 'black';
    }
  }

  convertTimeToTimestamp(executionStatus: string, timeValue: number): string {
    const date = new Date(timeValue);
    const formattedTime = date.toLocaleString('en-US',{ timeZoneName: 'short' });
    return formattedTime;
  }

  convertSecondsToTime(seconds: number): string {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const remainingSeconds = seconds % 60;

    const formattedTime = `${this.padZero(hours)}:${this.padZero(minutes)}:${this.padZero(remainingSeconds)}`;
    return formattedTime;
  }

  padZero(value: number): string {
    return value.toString().padStart(2, '0');
  }

  filterByStatus: NzTableFilterFn<Execution> = function(list: string[], execution: Execution){
    return list.some(function(executionStatus) {
      return execution.executionStatus.indexOf(executionStatus) !== -1;
    });
  }

  clickToViewHistory(workflowId: number) {
    const modalRef = this.modalService.open(NgbdModalWorkflowExecutionsComponent, {
      size: "xl",
      modalDialogClass: "modal-dialog-centered",
    });

    for (let i = 0; i < this.workflows.length; i++) {
      const workflow = this.workflows[i];
      if (workflow.wid == workflowId) {
        let currentWorkflow: Workflow = workflow;
        modalRef.componentInstance.workflow = currentWorkflow;
        modalRef.componentInstance.workflowName = currentWorkflow.name;

        break;
      }
    }
  }

  killExecution(wid: number) {
    let socket = new WorkflowWebsocketService();
    socket.openWebsocket(wid);
    socket.send("WorkflowKillRequest", {}); 
    // socket.closeWebsocket();
  }

  pauseExecution(wid: number) {
    let socket = new WorkflowWebsocketService();
    socket.openWebsocket(wid);
    socket.send("WorkflowPauseRequest", {}); 
    // socket.closeWebsocket();
  }

  resumeExecution(wid: number) {
    let socket = new WorkflowWebsocketService();
    socket.openWebsocket(wid);
    socket.send("WorkflowResumeRequest", {}); 
    // socket.closeWebsocket();
  }

  public sortByWorkflowName: NzTableSortFn<Execution> = (a: Execution, b: Execution) => (b.workflowName || "").localeCompare(a.workflowName);
  public sortByExecutionName: NzTableSortFn<Execution> = (a: Execution, b: Execution) => (b.executionName || "").localeCompare(a.executionName);
  public sortByCompletedTime: NzTableSortFn<Execution> = (a: Execution, b: Execution) => b.endTime - a.endTime;
  public sortByInitiator: NzTableSortFn<Execution> = (a: Execution, b: Execution) => (b.userName || "").localeCompare(a.userName);
}