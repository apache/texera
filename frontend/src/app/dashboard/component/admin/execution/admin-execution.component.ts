/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component, OnDestroy, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { forkJoin, interval, Observable } from "rxjs";
import { switchMap } from "rxjs/operators";
import { AdminExecutionService } from "../../../service/admin/execution/admin-execution.service";
import { Execution } from "../../../../common/type/execution";
import {
  NzTableFilterFn,
  NzTableQueryParams,
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzThAddOnComponent,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { NzModalService } from "ng-zorro-antd/modal";
import { WorkflowExecutionHistoryComponent } from "../../user/user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { WorkflowWebsocketService } from "../../../../workspace/service/workflow-websocket/workflow-websocket.service";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NgFor, NgIf, NgStyle } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpinComponent } from "ng-zorro-antd/spin";

export const NO_SORT = "NO_SORTING";

// How often the on-screen elapsed-time / status of already-loaded rows is recomputed (client-side only).
const TIME_TICK_INTERVAL_MS = 1000;
// How often the current page's data is refreshed from the server (decoupled from the page bar).
const EXECUTION_REFRESH_INTERVAL_MS = 5000;

@UntilDestroy()
@Component({
  templateUrl: "./admin-execution.component.html",
  styleUrls: ["./admin-execution.component.scss"],
  imports: [
    NzCardComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzThAddOnComponent,
    NzTbodyComponent,
    NgFor,
    NgIf,
    NgStyle,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
    NzSpinComponent,
  ],
})
export class AdminExecutionComponent implements OnInit, OnDestroy {
  listOfExecutions: ReadonlyArray<Execution> = [];
  isLoading: boolean = true;
  totalWorkflows: number = 0;
  pageSize: number = 5;
  // CurrentPageIndex is 0-indexed, but pageIndex in HTML is 1-indexed.
  currentPageIndex: number = 0;
  sortField: string = NO_SORT;
  sortDirection: string = NO_SORT;
  filter: string[] = [];

  // Handle for the client-side time/status tick; cleared on destroy.
  private clockTimer?: ReturnType<typeof setInterval>;

  constructor(
    private adminExecutionService: AdminExecutionService,
    private modalService: NzModalService,
    private config: GuiConfigService
  ) {}

  ngOnInit() {
    // Initial load of the current page.
    this.fetchData();

    // Tick the elapsed-time / "JUST COMPLETED" status of already-loaded rows every
    // second. This is purely client-side (no server call) and never touches the page bar.
    this.clockTimer = setInterval(() => this.updateTimeStatus(), TIME_TICK_INTERVAL_MS);

    // Periodically refresh the current page's data so live status changes and new
    // executions stay fresh. switchMap cancels a stale in-flight request, and the page
    // index/size are not disturbed here, so the page bar stays stable while polling.
    interval(EXECUTION_REFRESH_INTERVAL_MS)
      .pipe(
        switchMap(() => this.currentPage$()),
        untilDestroyed(this)
      )
      .subscribe(result => this.applyCurrentPage(result));
  }

  ngOnDestroy(): void {
    if (this.clockTimer !== undefined) {
      clearInterval(this.clockTimer);
    }
  }

  /**
   * Fetch the current page of executions plus the total count, used on init and on
   * user-driven pagination / sort / filter changes (a brief loading state is fine here).
   */
  fetchData(): void {
    this.isLoading = true;
    this.currentPage$()
      .pipe(untilDestroyed(this))
      .subscribe(result => {
        this.applyCurrentPage(result);
        this.isLoading = false;
      });
  }

  /**
   * Refresh only the current page's rows without changing the page index/size — used by
   * the background poll and immediately after kill/pause/resume so the control surface
   * reflects the new status without waiting for the next refresh tick.
   */
  private fetchCurrentPage(): void {
    this.currentPage$()
      .pipe(untilDestroyed(this))
      .subscribe(result => this.applyCurrentPage(result));
  }

  /**
   * Build the request for the current page's executions and the total workflow count.
   */
  private currentPage$(): Observable<{ executions: ReadonlyArray<Execution>; total: number }> {
    return forkJoin({
      executions: this.adminExecutionService.getExecutionList(
        this.pageSize,
        this.currentPageIndex,
        this.sortField,
        this.sortDirection,
        this.filter
      ),
      total: this.adminExecutionService.getTotalWorkflows(),
    });
  }

  /**
   * Apply a fetched page to the view. The total is only reassigned when it actually
   * changed, so a background refresh never churns the page count mid-interaction.
   */
  private applyCurrentPage(result: { executions: ReadonlyArray<Execution>; total: number }): void {
    this.listOfExecutions = [...result.executions];
    if (result.total !== this.totalWorkflows) {
      this.totalWorkflows = result.total;
    }
    this.updateTimeStatus();
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

  updateTimeStatus() {
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

  clickToViewHistory(wid: number, name: string) {
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
    let socket = new WorkflowWebsocketService(this.config);
    socket.openWebsocket(wid);
    socket.send("WorkflowKillRequest", {});
    // Reflect the new status promptly instead of waiting for the next refresh tick.
    this.fetchCurrentPage();
    // socket.closeWebsocket();
  }

  /**
   Due to the Async nature when setting up the websocket, the socket would be closed before the connection is established.
   Therefore, commenting the code to ensure the connections is established and request has been sent.
   */
  pauseExecution(wid: number) {
    let socket = new WorkflowWebsocketService(this.config);
    socket.openWebsocket(wid);
    socket.send("WorkflowPauseRequest", {});
    // Reflect the new status promptly instead of waiting for the next refresh tick.
    this.fetchCurrentPage();
    // socket.closeWebsocket();
  }

  /**
   Due to the Async nature when setting up the websocket, the socket would be closed before the connection is established.
   Therefore, commenting the code to ensure the connections is established and request has been sent.
   */
  resumeExecution(wid: number) {
    let socket = new WorkflowWebsocketService(this.config);
    socket.openWebsocket(wid);
    socket.send("WorkflowResumeRequest", {});
    // Reflect the new status promptly instead of waiting for the next refresh tick.
    this.fetchCurrentPage();
    // socket.closeWebsocket();
  }

  /**
   * Reorder the executions based on the selected field and order.
   * The sorting function is implemented in the backend.
   * @param sortField
   * @param sortOrder
   */
  onSortChange(sortField: string, sortOrder: string | null): void {
    if (sortField == this.sortField && sortOrder == null) {
      this.sortField = NO_SORT;
      this.sortDirection = NO_SORT;
      this.fetchData();
    } else if (sortOrder != null) {
      this.sortField = sortField;
      this.sortDirection = sortOrder === "descend" ? "desc" : "asc";
      this.fetchData();
    }
  }

  /**
   * Function that displays executions in respond to page size and page index changes.
   * @param params
   */
  onQueryParamsChange(params: NzTableQueryParams): void {
    const { pageSize, pageIndex } = params;
    const pageSizeChanged = pageSize !== this.pageSize;
    const pageIndexChanged = pageIndex - 1 !== this.currentPageIndex;
    // Sort/filter changes also emit nzQueryParams, but they are handled by
    // onSortChange/onFilterChange; ignore them here to avoid a double fetch.
    if (!pageSizeChanged && !pageIndexChanged) {
      return;
    }
    this.pageSize = pageSize;
    // Read both the size and the (1-indexed) page from the event and clamp the page
    // into range, independent of the order ng-zorro reports size vs. index changes.
    const lastPageIndex = Math.max(1, Math.ceil(this.totalWorkflows / pageSize));
    this.currentPageIndex = Math.min(pageIndex, lastPageIndex) - 1;
    this.fetchData();
  }

  /**
   * Filter the executions based on the status user checked.
   * The filtering function in implemented in the backend.
   * @param filter
   */
  onFilterChange(filter: any[]): void {
    this.filter = filter.map(item => String(item));
    this.fetchData();
  }
}
