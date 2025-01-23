import { Component, OnDestroy, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowExecutionsEntry } from "../../../../dashboard/type/workflow-executions-entry";
import { ExecuteWorkflowService } from "../../../service/execute-workflow/execute-workflow.service";
import { WorkflowVersionService } from "../../../../dashboard/service/user/workflow-version/workflow-version.service";
import {
  WORKFLOW_EXECUTIONS_API_BASE_URL,
  WorkflowExecutionsService,
} from "../../../../dashboard/service/user/workflow-executions/workflow-executions.service";
import { HttpClient } from "@angular/common/http";
import { Observable, timer } from "rxjs";
import { map } from "rxjs/operators";
import { ReplayExecutionInfo } from "../../../types/workflow-websocket.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: "texera-time-travel",
  templateUrl: "time-travel.component.html",
  styleUrls: ["time-travel.component.scss"],
})
export class TimeTravelComponent implements OnInit, OnDestroy {
  interactionHistories: { [eid: number]: string[] } = {};
  public executionList: WorkflowExecutionsEntry[] = [];
  expandedRows = new Set<number>(); // Tracks expanded rows by execution ID
  public revertedToInteraction: ReplayExecutionInfo | undefined = undefined;

  constructor(
    private workflowActionService: WorkflowActionService,
    public executeWorkflowService: ExecuteWorkflowService,
    private workflowVersionService: WorkflowVersionService,
    private workflowExecutionsService: WorkflowExecutionsService,
    private notificationService: NotificationService,
    private http: HttpClient
  ) {}

  ngOnInit(): void {
    // gets the versions result and updates the workflow versions table displayed on the form
    timer(0, 5000) // trigger per 5 secs
      .pipe(untilDestroyed(this))
      .subscribe(e => {
        let wid = this.getWid();
        if (wid === undefined) {
          return;
        }
        this.displayExecutionWithLogs(wid);
      });
  }

  ngOnDestroy() {
    if (this.revertedToInteraction !== undefined) {
      this.workflowVersionService.closeReadonlyWorkflowDisplay();
      try {
        this.executeWorkflowService.killWorkflow();
      } catch (e) {
        // ignore exception.
      }
    }
  }

  public getWid(): number | undefined {
    return this.workflowActionService.getWorkflowMetadata()?.wid;
  }

  toggleRow(eId: number): void {
    if (this.expandedRows.has(eId)) {
      this.expandedRows.delete(eId);
    } else {
      this.expandedRows.add(eId);
      this.getInteractionHistory(eId); // Call only if needed
    }
  }

  retrieveInteractionHistory(wid: number, eid: number): Observable<string[]> {
    return this.http.get<string[]>(`${WORKFLOW_EXECUTIONS_API_BASE_URL}/${wid}/interactions/${eid}`);
  }

  public retrieveLoggedExecutions(wid: number): Observable<WorkflowExecutionsEntry[]> {
    return this.workflowExecutionsService.retrieveWorkflowExecutions(wid).pipe(
      map(executionList =>
        executionList.filter(execution => {
          return execution.logLocation ? execution.logLocation.length > 0 : false;
        })
      )
    );
  }

  getInteractionHistory(eid: number): void {
    let wid = this.getWid();
    if (wid === undefined) {
      return;
    }
    this.retrieveInteractionHistory(wid, eid)
      .pipe(untilDestroyed(this))
      .subscribe(data => {
        this.interactionHistories[eid] = data; // TODO:add FULL_REPLAY here to support fault tolerance.
      });
  }

  /**
   * calls the http get request service to display the versions result in the table
   */
  displayExecutionWithLogs(wid: number): void {
    this.retrieveLoggedExecutions(wid)
      .pipe(untilDestroyed(this))
      .subscribe(executions => {
        this.executionList = executions;
        this.expandedRows.forEach(row => this.getInteractionHistory(row));
      });
  }

  onInteractionClick(vid: number, eid: number, interaction: string) {
    let wid = this.getWid();
    if (wid === undefined) {
      return;
    }
    this.workflowVersionService
      .retrieveWorkflowByVersion(wid, vid)
      .pipe(untilDestroyed(this))
      .subscribe(workflow => {
        this.workflowVersionService.displayReadonlyWorkflow(workflow);
        let replayExecutionInfo = { eid: eid, interaction: interaction };
        this.revertedToInteraction = replayExecutionInfo;
        this.notificationService.info(`start replay to interaction ${interaction} at execution ${eid}`);
        this.executeWorkflowService.executeWorkflowWithReplay(replayExecutionInfo);
      });
  }

  protected readonly requestIdleCallback = requestIdleCallback;
}
