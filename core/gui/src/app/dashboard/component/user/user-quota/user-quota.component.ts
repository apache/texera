import { Component, OnInit, inject } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { File, Workflow, MongoExecution, MongoWorkflow } from "../../../../common/type/user";
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
  totalMongoSize: number = 0;
  totalUploadedDatasetSize: number = 0;
  totalUploadedDatasetCount: number = 0;
  createdFiles: ReadonlyArray<File> = [];
  createdWorkflows: ReadonlyArray<Workflow> = [];
  accessFiles: ReadonlyArray<number> = [];
  accessWorkflows: ReadonlyArray<number> = [];
  mongodbExecutions: ReadonlyArray<MongoExecution> = [];
  datasetList: ReadonlyArray<DatasetQuota> = [];
  mongodbWorkflows: Array<MongoWorkflow> = [];
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

    this.UserService.getMongoDBs(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(mongoList => {
        this.totalMongoSize = 0;
        this.mongodbExecutions = mongoList;
        this.mongodbWorkflows = [];

        this.mongodbExecutions.forEach(execution => {
          let insert = false;
          this.totalMongoSize += execution.size;

          this.mongodbWorkflows.some((workflow, index, array) => {
            if (workflow.workflowName === execution.workflowName) {
              array[index].executions.push(execution);
              insert = true;
              return;
            }
          });

          if (!insert) {
            let workflow: MongoWorkflow = {
              workflowName: execution.workflowName,
              executions: [] as MongoExecution[],
            };
            workflow.executions.push(execution);
            this.mongodbWorkflows.push(workflow);
          }
        });
      });
  }

  deleteMongoCollection(collectionName: string, execution: MongoExecution, workflowName: string) {
    this.UserService.deleteMongoDBCollection(collectionName)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.mongodbWorkflows.some((workflow, index, array) => {
          if (workflow.workflowName === workflowName) {
            array[index].executions = array[index].executions.filter(e => e !== execution);
            this.totalMongoSize -= execution.size;
          }
        });
      });
  }

  // alias for formatSize
  formatSize = formatSize;

  public sortByMongoDBSize: NzTableSortFn<MongoExecution> = (a: MongoExecution, b: MongoExecution) => b.size - a.size;
}
