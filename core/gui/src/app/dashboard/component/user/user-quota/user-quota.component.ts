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

import { Component, OnInit, inject } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { File, Workflow, ExecutionQuota, WorkflowQuota } from "../../../../common/type/user";
import { DatasetQuota } from "src/app/dashboard/type/quota-statistic.interface";
import { NzTableSortFn } from "ng-zorro-antd/table";
import { UserQuotaService } from "src/app/dashboard/service/user/quota/user-quota.service";
import { AdminUserService } from "src/app/dashboard/service/admin/user/admin-user.service";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import * as Plotly from "plotly.js-basic-dist-min";
import { formatSize } from "src/app/common/util/size-formatter.util";

type UserServiceType = AdminUserService | UserQuotaService;

@UntilDestroy()
@Component({
  templateUrl: "./user-quota.component.html",
  styleUrls: ["./user-quota.component.scss"],
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
      if (monthMap.has(month)) {
        monthMap.set(month, monthMap.get(month)! + value);
      } else {
        monthMap.set(month, value);
      }
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
        title: x_label,
      },
      yaxis: {
        title: y_label,
        rangemode: "tozero" as const,
        zeroline: true,
        zerolinewidth: 2,
        zerolinecolor: "#000",
        range: [0, "auto"],
        tickmode: yRange <= 5 ? ("linear" as const) : undefined,
        dtick: yRange <= 5 ? 1 : undefined,
      },
    };

    Plotly.newPlot(chart, data, layout);
  }

  refreshData() {
    this.UserService.getCreatedDatasets(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(datasetList => {
        this.datasetList = datasetList;
        let totalDatasetSize = 0;
        this.totalUploadedDatasetCount = datasetList.length;
        let pieChartData: Array<[string, ...number[]]> = [];
        let lineChartData: Map<string, number> = new Map();
        this.datasetList.forEach(dataset => {
          totalDatasetSize += dataset.size;
          pieChartData.push([dataset.name, dataset.size]);
          const date = new Date(dataset.creationTime).toLocaleDateString();
          if (lineChartData.has(date)) {
            lineChartData.set(date, lineChartData.get(date)! + 1);
          } else {
            lineChartData.set(date, 1);
          }
        });
        this.generatePieChart(pieChartData, "Dataset Size Distribution", "sizePieChart");
        let lineChartDataArray: Array<[string, number]> = [];
        lineChartData.forEach((count, date) => {
          lineChartDataArray.push([date, count]);
        });
        lineChartDataArray = this.aggregateData(lineChartDataArray, 5);
        this.generateLineChart(lineChartDataArray, "Date", "Count", "Dataset Upload Overview", "datasetLineChart");
        this.totalUploadedDatasetSize = totalDatasetSize;
      });

    this.UserService.getCreatedWorkflows(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(workflowList => {
        let lineChartData: Map<string, number> = new Map();
        this.createdWorkflows = workflowList;
        this.createdWorkflows.forEach(workflow => {
          const date = new Date(workflow.creationTime).toLocaleDateString();
          if (lineChartData.has(date)) {
            lineChartData.set(date, lineChartData.get(date)! + 1);
          } else {
            lineChartData.set(date, 1);
          }
        });
        let lineChartDataArray: Array<[string, number]> = [];
        lineChartData.forEach((count, date) => {
          lineChartDataArray.push([date, count]);
        });
        lineChartDataArray = this.aggregateData(lineChartDataArray, 5);
        this.generateLineChart(lineChartDataArray, "Date", "Count", "Workflow Upload Overview", "workflowLineChart");
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
            this.totalQuotaSize -=
              executionToDelete.resultBytes + executionToDelete.logBytes + executionToDelete.runTimeStatsBytes;
            workflow.executions = workflow.executions.filter(execution => execution.eid !== eid);
          }
        });
        this.workflows = this.workflows.filter(workflow => workflow.executions.length > 0);
      });
  }

  // alias for formatSize
  formatSize = formatSize;

  public sortBySize: NzTableSortFn<ExecutionQuota> = (a: ExecutionQuota, b: ExecutionQuota) =>
    b.resultBytes + b.logBytes + b.runTimeStatsBytes - a.resultBytes - a.logBytes - a.runTimeStatsBytes;
}
