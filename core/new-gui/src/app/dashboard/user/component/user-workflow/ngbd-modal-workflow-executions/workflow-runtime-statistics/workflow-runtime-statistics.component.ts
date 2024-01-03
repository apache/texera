import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "src/app/dashboard/user/type/workflow-runtime-statistics";
import * as Plotly from "plotly.js-dist-min";
import { MatTabChangeEvent } from "@angular/material/tabs";

@UntilDestroy()
@Component({
  selector: "texera-workflow-runtime-statistics",
  templateUrl: "./workflow-runtime-statistics.component.html",
  styleUrls: ["./workflow-runtime-statistics.component.scss"],
})
export class WorkflowRuntimeStatisticsComponent implements OnInit {
  @Input()
  workflowRuntimeStatistics?: WorkflowRuntimeStatistics[];

  private tab_index = 0;
  private groupedStats?: Record<string, WorkflowRuntimeStatistics[]>;
  public metrics: string[] = ["Input Tuple Count", "Output Tuple Count"];

  constructor() {}

  ngOnInit(): void {
    if (!this.workflowRuntimeStatistics) {
      return;
    }

    this.groupedStats = this.groupStatsByOperatorId();
    //this.createChart();
  }

  onTabChanged(event: MatTabChangeEvent): void {
    this.tab_index = event.index;
    //this.createChart();
  }

  private groupStatsByOperatorId(): Record<string, WorkflowRuntimeStatistics[]> {
    return (
      this.workflowRuntimeStatistics?.reduce((acc: Record<string, WorkflowRuntimeStatistics[]>, stat) => {
        const statsArray = acc[stat.operatorId] || [];
        const lastStat = statsArray[statsArray.length - 1];

        if (lastStat) {
          stat.inputTupleCount += lastStat.inputTupleCount;
          stat.outputTupleCount += lastStat.outputTupleCount;
        }

        acc[stat.operatorId] = [...statsArray, stat];
        return acc;
      }, {}) || {}
    );
  }

  private createDatasets(): any[] {
    if (!this.groupedStats) {
      return [];
    }

    return Object.keys(this.groupedStats)
      .map(operatorId => {
        const operatorName = operatorId.split("-")[0];
        const uuidLast6Digits = operatorId.slice(-6);

        if (operatorName === "ProgressiveSinkOpDesc") {
          return null;
        }

        const yValues = this.tab_index === 0 ? "inputTupleCount" : "outputTupleCount";
        if (!this.groupedStats) {
          return null;
        }
        const stats = this.groupedStats[operatorId];

        return {
          x: stats.map((_, index) => index * 0.5),
          y: stats.map(stat => stat[yValues]),
          mode: "lines",
          name: `${operatorName}-${uuidLast6Digits}`,
        };
      })
      .filter(Boolean);
  }

  private createChart(): void {
    const datasets = this.createDatasets();

    if (!datasets || datasets.length === 0) {
      return;
    }

    const layout = {
      title: this.metrics[this.tab_index],
      xaxis: { title: "Time (s)" },
      yaxis: { title: this.metrics[this.tab_index] },
    };

    Plotly.newPlot("chart", datasets, layout);
  }
}
