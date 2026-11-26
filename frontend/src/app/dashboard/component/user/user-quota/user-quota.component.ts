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

import { Component, inject, OnInit } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { ExecutionQuota, File, Workflow, WorkflowQuota } from "../../../../common/type/user";
import { DatasetQuota } from "src/app/dashboard/type/quota-statistic.interface";
import {
  NzTableSortFn,
  NzTableComponent,
  NzTheadComponent,
  NzTrDirective,
  NzTableCellDirective,
  NzThMeasureDirective,
  NzThAddOnComponent,
  NzTbodyComponent,
} from "ng-zorro-antd/table";
import { UserQuotaService } from "src/app/dashboard/service/user/quota/user-quota.service";
import { AdminUserService } from "src/app/dashboard/service/admin/user/admin-user.service";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import * as Plotly from "plotly.js-basic-dist-min";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzTabsComponent, NzTabComponent } from "ng-zorro-antd/tabs";
import { NzCollapseComponent, NzCollapsePanelComponent } from "ng-zorro-antd/collapse";
import { NgFor } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzIconDirective } from "ng-zorro-antd/icon";

type UserServiceType = AdminUserService | UserQuotaService;

@UntilDestroy()
@Component({
  templateUrl: "./user-quota.component.html",
  styleUrls: ["./user-quota.component.scss"],
  imports: [
    NzCardComponent,
    NzTabsComponent,
    NzTabComponent,
    NzCollapseComponent,
    NgFor,
    NzCollapsePanelComponent,
    NzTableComponent,
    NzTheadComponent,
    NzTrDirective,
    NzTableCellDirective,
    NzThMeasureDirective,
    NzThAddOnComponent,
    NzTbodyComponent,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    NzPopconfirmDirective,
    ɵNzTransitionPatchDirective,
    NzTooltipDirective,
    NzIconDirective,
  ],
})
export class UserQuotaComponent implements OnInit {
  readonly userId: number;
  backgroundColor: String = "white";
  textColor: String = "Black";
  dynamicHeight: string = "700px";

  totalFileSize: number = 0;
  totalQuotaSize: number = 0;
  totalUploadedDatasetSize: number = 0;
  totalUploadedDatasetCount: number = 0;
  createdFiles: ReadonlyArray<File> = [];
  createdWorkflows: ReadonlyArray<Workflow> = [];
  accessFiles: ReadonlyArray<number> = [];
  accessWorkflows: ReadonlyArray<number> = [];
  executionCollections: ReadonlyArray<ExecutionQuota> = [];
  datasetList: ReadonlyArray<DatasetQuota> = [];
  workflows: Array<WorkflowQuota> = [];
  UserService: UserServiceType;
  DEFAULT_PIE_CHART_WIDTH = 480;
  DEFAULT_PIE_CHART_HEIGHT = 340;
  DEFAULT_LINE_CHART_WIDTH = 480;
  DEFAULT_LINE_CHART_HEIGHT = 340;

  constructor(
    private adminUserService: AdminUserService,
    private regularUserService: UserQuotaService
  ) {
    this.UserService = adminUserService;
    if (inject(NZ_MODAL_DATA, { optional: true })) {
      this.userId = inject(NZ_MODAL_DATA).uid;
      this.UserService = this.adminUserService;
      this.backgroundColor = "lightcoral";
      this.textColor = "white";
    } else {
      this.userId = -1;
      this.UserService = this.regularUserService;
      this.dynamicHeight = "";
    }
  }
  ngOnInit(): void {
    this.refreshData();
  }
  /* takes in an array of tuple ('label', 'value') and generates the corresponding pie chart */
  generatePieChart(dataToDisplay: Array<[string, ...number[]]>, title: string, chart: string) {
    var data = [
      {
        values: dataToDisplay.map(d => d[1]),
        labels: dataToDisplay.map(d => d[0]),
        type: "pie" as const,
      },
    ];
    var layout = {
      height: this.DEFAULT_PIE_CHART_HEIGHT,
      width: this.DEFAULT_PIE_CHART_WIDTH,
      title: {
        text: title,
      },
    };
    Plotly.newPlot(chart, data, layout);
  }

  filterOutdatedData(data: Array<[string, number]>): Array<[string, number]> {
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    return data.filter(([date]) => new Date(date) >= oneYearAgo);
  }
  aggregateByMonth(data: Array<[string, number]>): Array<[string, number]> {
    const monthMap = new Map<string, number>();
    data.forEach(([date, value]) => {
      const month = date.substring(0, 7); // 'YYYY-MM'
      const previous = monthMap.get(month);
      monthMap.set(month, previous === undefined ? value : previous + value);
    });
    return Array.from(monthMap, ([date, value]) => [date, value]);
  }

  aggregateData(data: Array<[string, number]>, numGroup: number) {
    data = this.filterOutdatedData(data);

    if (data.length < 8) {
      return data;
    }

    const uniqueMonths = new Set(data.map(([date]) => date.substring(0, 7)));
    if (uniqueMonths.size >= 3) {
      return this.aggregateByMonth(data);
    }

    const startDate = new Date(data[0][0]);
    const endDate = new Date(data[data.length - 1][0]);
    const newOfDays = (endDate.getTime() - startDate.getTime()) / (1000 * 3600 * 24);
    const daysPerGroup = Math.ceil(newOfDays / numGroup);
    let aggData: Array<[string, number]> = [];

    let currentGroupStartDate = startDate;
    let sum = 0;
    let nextDate = new Date(currentGroupStartDate);
    nextDate.setDate(currentGroupStartDate.getDate() + daysPerGroup);
    data.forEach(([date, value]) => {
      const currentDate = new Date(date);
      if (currentDate < nextDate) {
        sum += value;
      } else {
        aggData.push([currentGroupStartDate.toISOString().split("T")[0], sum]);
        currentGroupStartDate = new Date(nextDate);
        nextDate.setDate(currentGroupStartDate.getDate() + daysPerGroup);
        sum = value;
      }
    });
    aggData.push([currentGroupStartDate.toISOString().split("T")[0], sum]);
    return aggData;
  }

  generateLineChart(
    dataToDisplay: Array<[string, number]>,
    x_label: string,
    y_label: string,
    title: string,
    chart: string
  ) {
    var data = [
      {
        x: dataToDisplay.map(d => d[0]),
        y: dataToDisplay.map(d => d[1]),
        type: "scatter" as const,
      },
    ];

    const yValues = dataToDisplay.map(d => d[1]);
    const maxY = Math.max(...yValues);
    const minY = Math.min(...yValues);
    const yRange = maxY - minY;

    var layout = {
      height: this.DEFAULT_LINE_CHART_HEIGHT,
      width: this.DEFAULT_LINE_CHART_WIDTH,
      title: {
        text: title,
      },
      xaxis: {
        title: {
          text: x_label,
        },
      },
      yaxis: {
        title: {
          text: y_label,
        },
        rangemode: "tozero" as const,
        zeroline: true,
        zerolinewidth: 2,
        zerolinecolor: "#000",
        tickmode: yRange <= 5 ? ("linear" as const) : undefined,
        dtick: yRange <= 5 ? 1 : undefined,
      },
    };

    Plotly.newPlot(chart, data, layout);
  }

  get quotaSummaryItems(): ReadonlyArray<{ heading: string; value: string | number }> {
    return [
      { heading: "Files Uploaded", value: this.createdFiles.length },
      { heading: "Datasets Uploaded", value: this.totalUploadedDatasetCount },
      { heading: "Total Size of Datasets", value: formatSize(this.totalUploadedDatasetSize) },
      { heading: "Workflows Created", value: this.createdWorkflows.length },
      { heading: "Files with Access", value: this.accessFiles.length },
      { heading: "Workflows with Access", value: this.accessWorkflows.length },
      { heading: "Total Size of the Files", value: formatSize(this.totalFileSize) },
      { heading: "Total Result Cache Size", value: formatSize(this.totalQuotaSize) },
    ];
  }

  executionSize(execution: ExecutionQuota): number {
    return execution.resultBytes + execution.logBytes + execution.runTimeStatsBytes;
  }

  private populateUploadLineChart(creationTimes: ReadonlyArray<number>, title: string, chart: string): void {
    const countsByDate = new Map<string, number>();
    creationTimes.forEach(creationTime => {
      const date = new Date(creationTime).toLocaleDateString();
      countsByDate.set(date, (countsByDate.get(date) ?? 0) + 1);
    });
    const aggregatedData = this.aggregateData(Array.from(countsByDate.entries()), 5);
    this.generateLineChart(aggregatedData, "Date", "Count", title, chart);
  }

  refreshData() {
    this.UserService.getCreatedDatasets(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(datasetList => {
        this.datasetList = datasetList;
        let totalDatasetSize = 0;
        this.totalUploadedDatasetCount = datasetList.length;
        const pieChartData: Array<[string, ...number[]]> = [];
        this.datasetList.forEach(dataset => {
          totalDatasetSize += dataset.size;
          pieChartData.push([dataset.name, dataset.size]);
        });
        this.generatePieChart(pieChartData, "Dataset Size Distribution", "sizePieChart");
        this.populateUploadLineChart(
          this.datasetList.map(dataset => dataset.creationTime),
          "Dataset Upload Overview",
          "datasetLineChart"
        );
        this.totalUploadedDatasetSize = totalDatasetSize;
      });

    this.UserService.getCreatedWorkflows(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(workflowList => {
        this.createdWorkflows = workflowList;
        this.populateUploadLineChart(
          this.createdWorkflows.map(workflow => workflow.creationTime),
          "Workflow Upload Overview",
          "workflowLineChart"
        );
      });

    this.UserService.getAccessWorkflows(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(accessWorkflows => {
        this.accessWorkflows = accessWorkflows;
      });

    this.UserService.getExecutionQuota(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(executionList => {
        this.totalQuotaSize = 0;
        this.executionCollections = executionList;
        this.workflows = [];

        this.executionCollections.forEach(execution => {
          this.totalQuotaSize += execution.resultBytes + execution.runTimeStatsBytes + execution.logBytes;
          let workflow = this.workflows.find(
            w => w.executions.length > 0 && w.executions[0].workflowId === execution.workflowId
          );

          if (!workflow) {
            workflow = {
              workflowId: execution.workflowId,
              workflowName: execution.workflowName,
              executions: [],
            };
            this.workflows.push(workflow);
          }
          workflow.executions.push(execution);
        });
      });
  }

  deleteCollection(eid: number) {
    this.UserService.deleteExecutionCollection(eid)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.workflows.forEach((workflow, index, array) => {
          const executionToDelete = workflow.executions.find(execution => execution.eid === eid);
          if (executionToDelete) {
            this.totalQuotaSize -= this.executionSize(executionToDelete);
            workflow.executions = workflow.executions.filter(execution => execution.eid !== eid);
          }
        });
        this.workflows = this.workflows.filter(workflow => workflow.executions.length > 0);
      });
  }

  // alias for formatSize
  formatSize = formatSize;

  public sortBySize: NzTableSortFn<ExecutionQuota> = (a: ExecutionQuota, b: ExecutionQuota) =>
    this.executionSize(a) - this.executionSize(b);
}
