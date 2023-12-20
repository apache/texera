import { Component, Input, OnInit } from "@angular/core";
import { UntilDestroy } from "@ngneat/until-destroy";
import { WorkflowRuntimeStatistics } from "src/app/dashboard/user/type/workflow-runtime-statistics";
import * as Plotly from 'plotly.js-dist-min';

@UntilDestroy()
@Component({
  selector: "texera-workflow-runtime-statistics",
  templateUrl: "./workflow-runtime-statistics.component.html",
  styleUrls: ["./workflow-runtime-statistics.component.scss"],
})
export class WorkflowRuntimeStatisticsComponent implements OnInit {
  @Input()
  workflowRuntimeStatistics?: WorkflowRuntimeStatistics[];

  constructor() {}

  ngOnInit(): void {
    if (this.workflowRuntimeStatistics === undefined) {
      return;
    }

    const groupedStats = this.groupStatsByOperatorId(this.workflowRuntimeStatistics);
    const datasets1 = this.createDatasets(groupedStats);

    this.createChart(datasets1);
  }

  groupStatsByOperatorId(stats: WorkflowRuntimeStatistics[]): Record<string, WorkflowRuntimeStatistics[]> {
    return stats.reduce((acc: Record<string, WorkflowRuntimeStatistics[]>, stat) => {
      acc[stat.operatorId] = acc[stat.operatorId] || [];
      if (acc[stat.operatorId].length > 0) {
        stat.inputTupleCount =
          stat.inputTupleCount + acc[stat.operatorId][acc[stat.operatorId].length - 1].inputTupleCount;
      }
      acc[stat.operatorId].push(stat);
      return acc;
    }, {});
  }

  createDatasets(groupedStats: Record<string, WorkflowRuntimeStatistics[]>): any[] {
    return Object.keys(groupedStats).map((operatorId, index) => {
      return {
        x: this.createLabels(groupedStats[operatorId]),
        y: groupedStats[operatorId].map(stat => stat.inputTupleCount),
        mode: 'lines',
        name: operatorId
      };
    });
  }

  createLabels(stats: WorkflowRuntimeStatistics[]): number[] {
    return stats.map((stat, index) => index * 0.5);
  }

  createChart(datasets: any[]): void {
    const layout = {
      title: 'Input Tuple Count',
      xaxis: {
        title: 'Time (s)'
      },
      yaxis: {
        title: 'Tuple Count'
      }
    };
  
    Plotly.newPlot("chart", datasets, layout);
  }
}
